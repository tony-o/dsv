package dsv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

func Lines1(bs, q, e, d []byte) [][]byte {
	rs := [][]byte{}
	elen := len(e)
	dlen := len(d)
	qlen := len(q)
	blen := len(bs)
	rd := regexp.QuoteMeta(string(d))
	rg, err := regexp.Compile(rd)
	if err != nil {
		return rs
	}
	lidx := 0
	matches := rg.FindAllIndex(bs, -1)
	for _, m := range matches {
		x := lidx
		y := m[1]
		inqt := false
		if y >= blen {
			rs = append(rs, bs[lidx:])
			break
		}
		if m[0]-elen > 0 && bytes.Compare(bs[m[0]-elen:m[0]], e) == 0 {
			continue
		}
		for x < y {
			if x > elen && bytes.Compare(bs[x:x+elen], e) == 0 {
				x += dlen
			} else if bytes.Compare(bs[x:x+qlen], q) == 0 {
				inqt = !inqt
			}
			x++
		}
		if !inqt {
			rs = append(rs, bs[lidx:y-dlen])
			lidx = y
		}
	}
	return rs
}

func Lines2(bs, q, e, d []byte) [][]byte {
	rs := [][]byte{}
	elen := len(e)
	blen := len(bs)
	qlen := len(q)
	dlen := len(d)
	idxd := 0
	inqt := false
	for i := 0; i < blen; i++ {
		if i > elen && bytes.Compare(bs[i:i+elen], e) == 0 {
			i += dlen
			continue
		}
		if bytes.Compare(bs[i:i+qlen], q) == 0 {
			// TODO: see if we need to skip this delim
			inqt = !inqt
			continue
		}
		if !inqt && bytes.Compare(bs[i:i+dlen], d) == 0 {
			rs = append(rs, bs[idxd:i])
			i += dlen - 1
			idxd = 1 + i
			continue
		}
	}
	if idxd < blen {
		rs = append(rs, bs[idxd:])
	}
	return rs
}

func Lines3(bs, q, e, d []byte) [][]byte {
	rs := [][]byte{}
	stream := bytes.NewBuffer(bs)
	buffer := bytes.NewBuffer([]byte{})
	elen := len(e)
	//qlen := len(q)
	dlen := len(d)
	//blen := len(bs)
	idxd := 0
	var line []byte
	var err error
	for line, err = stream.ReadBytes(d[0]); err == nil; line, err = stream.ReadBytes(d[0]) {
		line = append(line, stream.Next(dlen-1)...)
		idxd += len(line)
		if (idxd-dlen >= 0 && bytes.Compare(bs[idxd-dlen:idxd], d) != 0) || (idxd-dlen-elen >= 0 && bytes.Compare(bs[idxd-dlen-elen:idxd-dlen], e) == 0) {
			continue
		}
		buffer.Write(line)
		subs := bytes.NewBuffer(buffer.Bytes())
		inqt := false
		for lnx, err := subs.ReadBytes(q[0]); err == nil; lnx, err = subs.ReadBytes(q[0]) {
			llen := len(lnx)
			if llen > elen+1 && bytes.Compare(lnx[llen-elen-1:llen-1], e) == 0 {
				continue
			} else {
				subbuf := bytes.NewBuffer(subs.Bytes())
				rq := subbuf.Next(dlen - 1)
				if len(rq) != dlen-1 || bytes.Compare(rq, d[1:]) != 0 {
					break
				}
				inqt = !inqt
			}
		}
		if !inqt {
			rs = append(rs, buffer.Bytes())
			buffer = bytes.NewBuffer([]byte{})
		}
	}
	if len(line)+len(buffer.Bytes())+len(stream.Bytes()) > 0 { //lidx < blen {
		rs = append(rs, append(append(buffer.Bytes(), stream.Bytes()...), line...))
	}
	return rs
}

func Lines4(bs, q, e, d []byte) [][]byte {
	rs := [][]byte{}
	blen := len(bs)
	dlen := len(d)
	elen := len(e)
	qlen := len(q)
	qidx := 0
	qlidx := 0
	idx := 0
	lidx := 0
	ulidx := 0
	for idx = bytes.Index(bs, d); idx < blen && idx != lidx-1; idx = lidx + bytes.Index(bs[lidx:], d) {
		if idx == -1 {
			break
		}
		if idx-elen > 0 && bytes.Compare(bs[idx-elen:idx], e) != 0 {
			inqt := false
			qlidx = ulidx
			for qidx = qlidx + bytes.Index(bs[qlidx:idx], q); qidx < idx && qidx != qlidx-1; qidx = qlidx + bytes.Index(bs[qlidx:idx], q) {
				if qidx-elen < 0 || bytes.Compare(bs[qidx-elen:qidx], e) != 0 {
					inqt = !inqt
				}
				qlidx = qidx + qlen
			}
			if !inqt {
				rs = append(rs, bs[ulidx:idx])
				ulidx = idx + dlen
			}
		}
		lidx = idx + dlen
	}
	if ulidx < blen {
		rs = append(rs, bs[ulidx:])
	}
	return rs
}

// Deserializers

type refType struct {
	F map[string]reflect.StructField
	T reflect.Type
	E error
}

var refMemoize map[string]refType = map[string]refType{}

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
	memkey := t.PkgPath() + ":" + t.Name()
	if rt, ok := refMemoize[memkey]; ok {
		return rt.F, rt.T, rt.E
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
	refMemoize[memkey] = refType{
		F: m,
		T: t,
		E: nil,
	}
	return m, t, nil
}

func Deserialize4(i interface{}, bs, q, e, l, d []byte) (perr error) {
	rs := reflect.ValueOf(i)
	if rs.Kind() != reflect.Ptr {
		return DSV_INVALID_TARGET_NOT_PTR
	}
	rs = rs.Elem()
	if rs.Kind() != reflect.Slice {
		return DSV_INVALID_TARGET_NOT_SLICE.enhance(fmt.Errorf("got:%s", rs.Kind().String()))
	}

	fmap, typ, err := ref(i)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			perr = DSV_DESERIALIZE_ERROR.enhance(fmt.Errorf("%v", r))
		}
	}()

	ls := Lines4(bs, q, e, l)
	lineCount := len(ls) - 1
	headers := []string{}
	rs.Set(reflect.MakeSlice(reflect.SliceOf(typ), lineCount, lineCount))
	for i, l := range ls {
		fs := Lines4(l, q, e, d)
		if i == 0 {
			for _, f := range fs {
				headers = append(headers, string(f))
			}
			continue
		}
		fp := reflect.New(typ)
		fv := fp.Elem()
		for j, f := range fs {
			field := fv.FieldByName(fmap[headers[j]].Name)
			if field.IsValid() && field.CanSet() {
				ty := field.Type().String()
				if fn, okgo := DefaultDeserializers[ty]; okgo {
					v, _ := fn(f)
					field.Set(reflect.ValueOf(v))
				} else {
					field.Set(reflect.ValueOf(f))
				}
			} else {
				panic(fmt.Sprintf("target.%s error: unable to set or is invalid", fmap[headers[j]].Name))
			}
		}
		rs.Index(i - 1).Set(fv)
	}
	return nil
}

// Errors
type dsvErr struct {
	err error
	msg string
}

var (
	DSV_DUPLICATE_TAG_IN_STRUCT  = dsvErr{msg: "Struct contains a duplicate tag"}
	DSV_INVALID_TARGET_NOT_PTR   = dsvErr{msg: "Invalid target, not a pointer", err: errors.New("Invalid target, not a pointer")}
	DSV_INVALID_TARGET_NOT_SLICE = dsvErr{msg: "Invalid target, not a *slice", err: errors.New("Invalid target, not a *slice")}
	DSV_DESERIALIZE_ERROR        = dsvErr{msg: "Error occurred during deserialize"}
	DSV_FIELD_NUM_MISMATCH       = dsvErr{msg: "Strict Map option requires all rows have same number of fields"}
	DSV_FIELD_DELIMITER_NZ       = dsvErr{msg: "FieldDelimiter must not be zero length", err: errors.New("FieldDelimiter must not be zero length")}
	DSV_LINE_SEPARATOR_NZ        = dsvErr{msg: "LineSeparator must not be zero length", err: errors.New("LineSeparator must not be zero length")}

	DSV_SERIALIZER_MISSING = dsvErr{msg: "Serializer requested was not found"}
)

func (e dsvErr) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.msg, e.err)
	}
	return e.msg
}

func (e dsvErr) Is(t error) bool {
	return t.Error() == e.msg || strings.HasPrefix(t.Error(), e.msg+": ")
}

func (e dsvErr) enhance(in error) dsvErr {
	return dsvErr{msg: e.msg, err: in}
}
