package dsv

import (
	"bytes"
	"fmt"
	"reflect"
)

type dsvi struct {
	fieldDelimiter []byte
	lineSeparator  []byte
	fieldOperator  []byte
	escapeOperator []byte
	parseHeader    bool
	useCache       bool
	strictMap      bool
	stripField     []byte
	skipEmptyRow   bool
	serializers    map[string]func(interface{}) ([]byte, bool)
	deserializers  map[string]func(string, []byte) (interface{}, bool)

	escapedDelimiter []byte
	escapedOperator  []byte
	escapedSeparator []byte

	lslen int
	eolen int
	folen int
	fdlen int

	escdlen int
	escolen int
	escslen int
}

type dbyte struct {
	ok    bool
	value []byte
}
type dbool struct {
	ok    bool
	value bool
}
type dserial struct {
	ok    bool
	value map[string]func(interface{}) ([]byte, bool)
}
type ddeserial struct {
	ok    bool
	value map[string]func(string, []byte) (interface{}, bool)
}

type DSVOpt struct {
	FieldDelimiter dbyte
	LineSeparator  dbyte
	FieldOperator  dbyte
	EscapeCombined dbyte
	EscapeOperator dbyte
	ParseHeader    dbool
	UseCache       dbool
	StrictMap      dbool
	SkipEmptyRow   dbool
	StripField     dbyte
	Serializers    dserial
	Deserializers  ddeserial
}

func DByte(s []byte) dbyte {
	return dbyte{ok: true, value: s}
}

func DString(s string) dbyte {
	return dbyte{ok: true, value: []byte(s)}
}

func DBool(b bool) dbool {
	return dbool{ok: true, value: b}
}

func DDeserial(m map[string]func(string, []byte) (interface{}, bool)) ddeserial {
	return ddeserial{ok: true, value: m}
}

func DSerial(m map[string]func(interface{}) ([]byte, bool)) dserial {
	return dserial{ok: true, value: m}
}

func ref(o interface{}) (map[string]reflect.StructField, reflect.Type, error) {
	t := reflect.TypeOf(o)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return ref(t)
		}
	}
	m := map[string]reflect.StructField{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("csv")
		if tag == "" || tag == "-" {
			continue
		}
		if _, exists := m[tag]; exists {
			return nil, nil, dsvErr{err: fmt.Errorf("Tag '%s' appears multiple times in %s", tag, t.Name()), msg: DSV_DUPLICATE_TAG_IN_STRUCT.msg}
		}
		m[tag] = field
	}
	return m, t, nil
}

func NewDSVMust(opt DSVOpt) dsvi {
	d, e := NewDSV(opt)
	if e != nil {
		panic(e)
	}
	return d
}

func NewDSV(opt DSVOpt) (dsvi, error) {
	di := dsvi{
		fieldDelimiter: []byte(","),
		lineSeparator:  []byte("\n"),
		fieldOperator:  []byte("\""),
		escapeOperator: []byte("\\"),
		parseHeader:    true,
		useCache:       true,
		strictMap:      false,
		skipEmptyRow:   true,
		stripField:     []byte(" \r\n\t"),
		deserializers:  DefaultDeserializers,
		serializers:    DefaultSerializers,
	}
	if opt.FieldDelimiter.ok {
		di.fieldDelimiter = opt.FieldDelimiter.value
	}
	if opt.LineSeparator.ok {
		di.lineSeparator = opt.LineSeparator.value
	}
	if opt.FieldOperator.ok {
		di.fieldOperator = opt.FieldOperator.value
	}
	if opt.EscapeOperator.ok {
		di.escapeOperator = opt.EscapeOperator.value
	}
	if opt.ParseHeader.ok {
		di.parseHeader = opt.ParseHeader.value
	}
	if opt.UseCache.ok {
		di.useCache = opt.UseCache.value
	}
	if opt.SkipEmptyRow.ok {
		di.skipEmptyRow = opt.SkipEmptyRow.value
	}
	if opt.StrictMap.ok {
		di.strictMap = opt.StrictMap.value
	}
	if opt.StripField.ok {
		di.stripField = opt.StripField.value
	}
	if opt.Serializers.ok {
		for k, v := range opt.Serializers.value {
			di.serializers[k] = v //opt.Deserializers.value
		}
	}
	if opt.Deserializers.ok {
		for k, v := range opt.Deserializers.value {
			di.deserializers[k] = v //opt.Deserializers.value
		}
	}

	di.lslen = len(di.lineSeparator)
	di.eolen = len(di.escapeOperator)
	di.folen = len(di.fieldOperator)
	di.fdlen = len(di.fieldDelimiter)

	if di.fdlen == 0 {
		return di, DSV_FIELD_DELIMITER_NZ
	}
	if di.lslen == 0 {
		return di, DSV_LINE_SEPARATOR_NZ
	}

	di.escapedDelimiter = append(di.escapeOperator, di.fieldDelimiter...)
	di.escapedOperator = append(di.escapeOperator, di.fieldOperator...)
	di.escapedSeparator = append(di.escapeOperator, di.lineSeparator...)
	di.escdlen = di.eolen + di.fdlen
	di.escolen = di.eolen + di.folen
	di.escslen = di.eolen + di.lslen

	return di, nil
}

func (d dsvi) NormalizeString(s []byte) []byte {
	if len(d.stripField) != 0 {
		s = bytes.Trim(s, string(d.stripField))
	}
	sl := len(s)
	if sl > d.folen*2 && bytes.Compare(s[0:d.folen], d.fieldOperator) == 0 && bytes.Compare(s[sl-d.folen:], d.fieldOperator) == 0 {
		s = s[d.folen : sl-d.folen]
	}
	sl = len(s)
	for i := 0; i < sl; i++ {
		if sl > i+d.eolen && bytes.Compare(s[i:i+d.eolen], d.escapeOperator) == 0 {
			s = append(s[0:i], s[i+d.eolen:]...)
			i -= d.eolen
			sl -= d.eolen
		}
	}
	return s
}

func (d dsvi) DeserializeMapIndex(s string) (map[int][]string, error) {
	m := map[int][]string{}

	rmap := 0
	inqt := false
	l := 0
	slen := len(s)
	ed := string(d.escapedDelimiter)
	es := string(d.escapedSeparator)
	eo := string(d.escapedOperator)
	fo := string(d.fieldOperator)
	fd := string(d.fieldDelimiter)
	ls := string(d.lineSeparator)
	ol := l
	for i := 0; i < slen; i++ {
		if d.escdlen > 0 && slen > i+d.escdlen && s[i:i+d.escdlen] == ed {
			i += d.escdlen - 1
		} else if d.escslen > d.lslen && slen > i+d.escslen && s[i:i+d.escslen] == es {
			i += d.escslen - 1
		} else if d.escolen > d.folen && slen > i+d.escolen && s[i:i+d.escolen] == eo {
			i += d.escolen - 1
		} else if d.folen > 0 && slen > i+d.folen && s[i:i+d.folen] == fo {
			inqt = !inqt
		} else if !inqt && s[i:i+d.fdlen] == fd {
			ss := d.NormalizeString([]byte(s[l:i]))
			m[rmap] = append(m[rmap], string(ss))
			//fmt.Printf("l=%d,i=%d\n", l, i)
			i += d.fdlen - 1
			l = i + 1
		} else if !inqt && s[i:i+d.lslen] == ls {
			ss := d.NormalizeString([]byte(s[l:i]))
			m[rmap] = append(m[rmap], string(ss))
			//fmt.Printf("l=%d,i=%d\n", l, i)
			if ol <= i-d.lslen || !d.skipEmptyRow {
				//	fmt.Printf("m[rmap] = m[%d] = %+v :c=%d\n\n", rmap, m[rmap], len(m[rmap]))
				rmap++
				m[rmap] = []string{}
			} else {
				m[rmap] = []string{}
			}
			i += d.lslen - 1
			l = i + 1
			ol = l
		}
	}
	ss := d.NormalizeString([]byte(s[l:]))
	if ol <= slen-d.lslen || !d.skipEmptyRow {
		m[rmap] = append(m[rmap], string(ss))
	}
	if len(m[rmap]) == 0 {
		delete(m, rmap)
	}
	return m, nil
}

func (d dsvi) Deserialize(s []byte, tgt interface{}) error {
	rs := reflect.ValueOf(tgt)
	if rs.Kind() != reflect.Ptr {
		return DSV_INVALID_TARGET_NOT_PTR
	}
	rs = rs.Elem()
	if rs.Kind() != reflect.Slice {
		return DSV_INVALID_TARGET_NOT_SLICE.enhance(fmt.Errorf("got:%s", rs.Kind().String()))
	}
	fmap, typ, e := ref(tgt)
	if e != nil {
		return e
	}

	lineMap, err := d.DeserializeMapIndex(string(s))
	if err != nil {
		return err
	}
	lineCount := len(lineMap)

	offs := 0
	if d.parseHeader {
		offs = -1
	}
	if lineCount+offs <= 0 {
		return nil
	}
	iln := len(lineMap[0])
	rs.Set(reflect.MakeSlice(reflect.SliceOf(typ), lineCount+offs, lineCount+offs))
	rowIndex := 0
	for i := 0 + (-1 * offs); i < lineCount; i++ {
		ln := lineMap[i]
		if d.skipEmptyRow && len(ln) == 0 {
			continue
		}
		if len(ln) != iln && d.strictMap {
			return DSV_FIELD_NUM_MISMATCH.enhance(fmt.Errorf("StrictMap requires all rows have same number of fields, expected=%d,got=%d", iln, len(ln)))
		}
		fp := reflect.New(typ)
		fv := fp.Elem()
		for j, r := range ln {
			var fs reflect.Value
			if d.parseHeader {
				if j >= len(lineMap[0]) {
					break
				}
				fs = fv.FieldByName(fmap[lineMap[0][j]].Name)
			} else {
				if j >= fv.NumField() {
					break
				}
				fs = fv.Field(j)
			}
			if fs.IsValid() && fs.CanSet() {
				var perr error = nil
				func() {
					defer func() {
						if r := recover(); r != nil {
							perr = DSV_DESERIALIZE_ERROR.enhance(fmt.Errorf("%v", r))
						}
					}()
					ty := fs.Type().Name()
					if ty == "" {
						ty = fs.Type().String()
					}
					if f, okgo := d.deserializers[ty]; okgo {
						v, _ := f(r, []byte(r))
						fs.Set(reflect.ValueOf(v))
					} else {
						fs.Set(reflect.ValueOf(r))
					}
				}()
				if perr != nil {
					return perr
				}
			}
		}
		rs.Index(rowIndex).Set(fv)
		rowIndex++
	}

	return nil
}

func (d dsvi) serializeIfc(src reflect.Value, fields []string) ([]byte, error) {
	bs := []byte{}
	for _, fidx := range fields {
		fv := src.FieldByName(fidx)
		ty := fv.Type().Name()
		if ty == "" {
			ty = fv.Type().String()
		}
		if f, okgo := d.serializers[ty]; okgo {
			v, _ := f(fv.Interface())
			bs = append(append(bs, v...), d.fieldDelimiter...)
		} else {
			return bs, DSV_SERIALIZER_MISSING.enhance(fmt.Errorf("Unable to find handler for type: %s", ty))
		}
	}
	if len(bs) > 0 {
		bs = append(bs[:len(bs)-d.fdlen], d.lineSeparator...)
	}
	return bs, nil
}

func (d dsvi) Serialize(src interface{}) ([]byte, error) {
	bs := []byte{}
	fmap, _, e := ref(src)
	if e != nil {
		return bs, e
	}
	bks := []string{}
	for k, v := range fmap {
		if d.parseHeader {
			bs = append(append(bs, []byte(k)...), d.fieldDelimiter...)
		}
		bks = append(bks, v.Name)
	}
	if len(bs) > 0 {
		bs = append(bs[:len(bs)-d.fdlen], d.lineSeparator...)
	}

	rs := reflect.ValueOf(src)
	if rs.Kind() == reflect.Ptr {
		for rs.Kind() == reflect.Ptr {
			rs = rs.Elem()
		}
	}
	if rs.Kind() == reflect.Struct {
		ds, e := d.serializeIfc(reflect.ValueOf(src), bks)
		if e != nil {
			return bs, e
		}
		bs = append(bs, ds...)
	} else if rs.Kind() == reflect.Slice {
		for i := 0; i < rs.Len(); i++ {
			item := rs.Index(i)
			for item.Kind() == reflect.Ptr {
				item = item.Elem()
			}
			ds, e := d.serializeIfc(item, bks)
			if e != nil {
				return bs, e
			}
			bs = append(bs, ds...)
		}
	} else {
		fmt.Printf("unknown type: %v\n", rs.Kind())
	}

	if len(bs) > d.fdlen {
		bs = bs[:len(bs)-d.fdlen]
	}

	return bs, nil
}
