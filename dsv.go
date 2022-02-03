package dsv

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type dsvi struct {
	fieldDelimiter string
	lineSeparator  string
	fieldOperator  string
	escapeOperator string
	parseHeader    bool
	useCache       bool
	strictMap      bool
	stripField     string
	skipEmptyRow   bool
	serializers    map[string]func(interface{}) (string, bool)
	deserializers  map[string]func(string) (interface{}, bool)

	escapedDelimiter string
	escapedOperator  string
	escapedSeparator string

	lslen int
	eolen int
	folen int
	fdlen int

	escdlen int
	escolen int
	escslen int
}

type dstring struct {
	ok    bool
	value string
}
type dbool struct {
	ok    bool
	value bool
}
type dserial struct {
	ok    bool
	value map[string]func(interface{}) (string, bool)
}
type ddeserial struct {
	ok    bool
	value map[string]func(string) (interface{}, bool)
}

type DSVOpt struct {
	FieldDelimiter dstring
	LineSeparator  dstring
	FieldOperator  dstring
	EscapeCombined dstring
	EscapeOperator dstring
	ParseHeader    dbool
	UseCache       dbool
	StrictMap      dbool
	SkipEmptyRow   dbool
	StripField     dstring
	Serializers    dserial
	Deserializers  ddeserial
}

func DString(s string) dstring {
	return dstring{ok: true, value: s}
}

func DBool(b bool) dbool {
	return dbool{ok: true, value: b}
}

func DDeserial(m map[string]func(string) (interface{}, bool)) ddeserial {
	return ddeserial{ok: true, value: m}
}

func DSerial(m map[string]func(interface{}) (string, bool)) dserial {
	return dserial{ok: true, value: m}
}

func ref(o interface{}) (map[string]reflect.StructField, reflect.Type, error) {
	t := reflect.TypeOf(o)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
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

func NewDSV(opt DSVOpt) (dsvi, error) {
	di := dsvi{
		fieldDelimiter: ",",
		lineSeparator:  "\n",
		fieldOperator:  "\"",
		escapeOperator: "\\",
		parseHeader:    true,
		useCache:       true,
		strictMap:      false,
		skipEmptyRow:   true,
		stripField:     " \r\n\t",
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
		di.serializers = opt.Serializers.value
	}
	if opt.Deserializers.ok {
		di.deserializers = opt.Deserializers.value
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

	di.escapedDelimiter = di.escapeOperator + di.fieldDelimiter
	di.escapedOperator = di.escapeOperator + di.fieldOperator
	di.escapedSeparator = di.escapeOperator + di.lineSeparator
	di.escdlen = di.eolen + di.fdlen
	di.escolen = di.eolen + di.folen
	di.escslen = di.eolen + di.lslen

	return di, nil
}

var unescapeNL = regexp.MustCompile("\\\n")

func (d dsvi) NormalizeString(s string) string {
	if d.stripField != "" {
		s = strings.Trim(s, d.stripField)
	}
	sl := len(s)
	if sl > d.folen*2 && s[0:d.folen] == d.fieldOperator && s[sl-d.folen:] == d.fieldOperator {
		s = s[d.folen : sl-d.folen]
	}
	sl = len(s)
	for i := 0; i < sl; i++ {
		if d.escslen > d.lslen && sl > i+d.escslen && s[i:i+d.escslen] == d.escapedSeparator {
			s = s[0:i] + d.lineSeparator + s[i+d.escslen:]
			i -= d.escslen
			sl = len(s)
			continue
		}
		if d.escolen > d.folen && sl > i+d.escolen && s[i:i+d.escolen] == d.escapedOperator {
			s = s[0:i] + d.fieldOperator + s[i+d.escolen:]
			i -= d.escolen
			sl = len(s)
			continue
		}
		if d.escdlen > d.fdlen && sl > i+d.escdlen && s[i:i+d.escdlen] == d.escapedDelimiter {
			s = s[0:i] + d.fieldDelimiter + s[i+d.escdlen:]
			i -= d.escdlen
			sl = len(s)
			continue
		}
	}
	return s
}

func (d dsvi) DeserializeString(s string, tgt interface{}, opt ...int) error {
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

	lines := [][]string{[]string{}}
	l := 0
	lnlen := 0
	slen := len(s)
	inqt := false
	idxmap := map[int]string{}
	for i := 0; i < slen; i++ {
		if d.escdlen > 0 && slen > i+d.escdlen && inqt && s[i:i+d.escdlen] == d.escapedDelimiter {
			i += d.escdlen
		} else if d.escslen > d.lslen && slen > i+d.escslen && s[i:i+d.escslen] == d.escapedSeparator {
			i += d.escslen
		} else if d.escolen > d.folen && slen > i+d.escolen && s[i:i+d.escolen] == d.escapedOperator {
			i += d.escolen
		} else if d.folen > 0 && slen > i+d.folen && s[i:i+d.folen] == d.fieldOperator {
			inqt = !inqt
		} else if !inqt && s[i:i+d.fdlen] == d.fieldDelimiter {
			ss := d.NormalizeString(s[l:i])
			if len(ss) > 0 || !d.skipEmptyRow {
				lines[lnlen] = append(lines[lnlen], ss)
			}
			i += d.fdlen - 1
			l = i + 1
			if d.parseHeader && lnlen == 0 {
				idxmap[len(idxmap)] = ss
			}
		} else if !inqt && s[i:i+d.lslen] == d.lineSeparator {
			ss := d.NormalizeString(s[l:i])
			if d.parseHeader && lnlen == 0 {
				idxmap[len(idxmap)] = ss
			}
			if len(ss) > 0 || !d.skipEmptyRow {
				lines[lnlen] = append(lines[lnlen], ss)
				lnlen++
				lines = append(lines, []string{})
			}
			i += d.lslen - 1
			l = i + 1
		}
	}
	ss := d.NormalizeString(s[l:])
	if len(ss) > 0 || !d.skipEmptyRow {
		lines[lnlen] = append(lines[lnlen], ss)
	}
	if len(lines[lnlen]) == 0 {
		lines = lines[:lnlen]
	}

	offs := -1
	if !d.parseHeader {
		offs = 0
	}
	if len(lines)+offs <= 0 {
		return nil
	}
	rs.Set(reflect.MakeSlice(reflect.SliceOf(typ), len(lines)+offs, len(lines)+offs))
	iln := len(idxmap)
	for i, ln := range lines {
		if (i == 0 && d.parseHeader) || (d.skipEmptyRow && len(ln) == 0) {
			continue
		}
		if len(ln) != iln && d.strictMap {
			return DSV_FIELD_NUM_MISMATCH.enhance(fmt.Errorf("StrictMap requires all rows have same number of fields, expected=%d,got=%d", iln, len(ln)))
		}
		fp := reflect.New(typ)
		fv := fp.Elem()
		for j, r := range ln {
			if d.parseHeader && i > 0 {
				fs := fv.FieldByName(fmap[idxmap[j]].Name)
				if fs.IsValid() && fs.CanSet() {
					var perr error = nil
					func() {
						defer func() {
							if r := recover(); r != nil {
								perr = DSV_DESERIALIZE_ERROR.enhance(fmt.Errorf("%v", r))
							}
						}()
						fs.Set(reflect.ValueOf(r))
						// TODO: this should be way more robust
					}()
					if perr != nil {
						return perr
					}
				}
			}
		}
		rs.Index(i + offs).Set(fv)
	}

	return nil
}
