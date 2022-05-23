package dsv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type dsv struct {
	record, escape, delimiter, quote []byte
	hasHeaders                       bool
}

func NewDSV(parseHeaders bool, recordSeparator, fieldSeparator, escapeSequence, quoteSequence []byte) dsv {
	return dsv{
		hasHeaders: parseHeaders,
		record:     recordSeparator,
		escape:     escapeSequence,
		quote:      quoteSequence,
		delimiter:  fieldSeparator,
	}
}

func (d dsv) Lines(bs []byte) [][]byte {
	return Lines(bs, d.quote, d.escape, d.record)
}

func (d dsv) FieldsFromLine(bs []byte) [][]byte {
	return Lines(bs, d.quote, d.escape, d.delimiter)
}

func (d dsv) Deserialize(bs []byte, into interface{}) error {
	return Deserialize(into, true, d.hasHeaders, bs, d.quote, d.escape, d.record, d.delimiter)
}

func (d dsv) Serialize(from interface{}) ([]byte, error) {
	return nil, errors.New("NYI")
}

func Lines(bs, quote, escape, delimiter []byte) [][]byte {
	rs := [][]byte{}
	blen := len(bs)
	dlen := len(delimiter)
	elen := len(escape)
	qlen := len(quote)
	qidx := 0
	qlidx := 0
	idx := 0
	lidx := 0
	ulidx := 0
	for idx = bytes.Index(bs, delimiter); idx < blen && idx != lidx-1; idx = lidx + bytes.Index(bs[lidx:], delimiter) {
		if idx == -1 {
			break
		}
		if idx-elen > 0 && bytes.Compare(bs[idx-elen:idx], escape) != 0 {
			inqt := false
			qlidx = ulidx
			for qidx = qlidx + bytes.Index(bs[qlidx:idx], quote); qidx < idx && qidx != qlidx-1; qidx = qlidx + bytes.Index(bs[qlidx:idx], quote) {
				if qidx-elen < 0 || bytes.Compare(bs[qidx-elen:qidx], escape) != 0 {
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

func Deserialize(i interface{}, ignoreUnmapped, parseHeaders bool, bs, quote, escape, record, delimiter []byte) (perr error) {
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

	ls := Lines(bs, quote, escape, record)
	lineCount := len(ls) - 1
	headers := []string{}
	rs.Set(reflect.MakeSlice(reflect.SliceOf(typ), lineCount, lineCount))
	for i, l := range ls {
		fs := Lines(l, quote, escape, delimiter)
		if i == 0 && parseHeaders {
			for _, f := range fs {
				headers = append(headers, string(f))
			}
			continue
		}
		fp := reflect.New(typ)
		fv := fp.Elem()
		for j, f := range fs {
			if _, ok := fmap[headers[j]]; ignoreUnmapped && !ok {
				continue
			}
			var field reflect.Value
			if parseHeaders {
				field = fv.FieldByName(fmap[headers[j]].Name)
			} else {
				field = fv.Field(j)
			}
			if field.IsValid() && field.CanSet() {
				ty := field.Type().String()
				if fn, okgo := DefaultDeserializers[ty]; okgo {
					v, _ := fn(f)
					field.Set(reflect.ValueOf(v))
				} else {
					field.Set(reflect.ValueOf(f))
				}
			} else {
				panic(fmt.Sprintf("target.%s(->%s) error: unable to set or is invalid", headers[j], fmap[headers[j]].Name))
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
