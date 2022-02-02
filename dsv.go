package dsv

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type dsvErr struct {
	err error
	msg string
}

var (
	DSV_DUPLICATE_TAG_IN_STRUCT  = dsvErr{msg: "Struct contains a duplicate tag"}
	DSV_INVALID_OPTION           = dsvErr{msg: "Invalid option given"}
	DSV_INVALID_TYPE_FOR_OPTION  = dsvErr{msg: "Invalid option for type"}
	DSV_INVALID_TARGET_NOT_PTR   = dsvErr{msg: "Invalid target, not a pointer", err: errors.New("Invalid target, not a pointer")}
	DSV_INVALID_TARGET_NOT_SLICE = dsvErr{msg: "Invalid target, not a *slice", err: errors.New("Invalid target, not a *slice")}
)

type dsvi struct {
	fieldDelimiter string
	lineSeparator  string
	fieldOperator  string
	escapeCombined string
	escapeOperator string
	parseHeader    bool
	useCache       bool
	strictMap      bool
	stripFieldWS   string

	lslen int
	eolen int
	folen int
	fdlen int
	eclen int
}

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

func NewDSV(options map[string]interface{}) (dsvi, error) {
	di := dsvi{
		fieldDelimiter: ",",
		lineSeparator:  "\n",
		fieldOperator:  "\"",
		escapeOperator: "\\",
		parseHeader:    true,
		useCache:       true,
		strictMap:      false,
		stripFieldWS:   " \r\n\t",
	}
	for k, v := range options {
		if k == "FieldDelimiter" {
			switch vt := v.(type) {
			case string:
				di.fieldDelimiter = v.(string)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "LineSeparator" {
			switch vt := v.(type) {
			case string:
				di.lineSeparator = v.(string)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "FieldOperator" {
			switch vt := v.(type) {
			case string:
				di.fieldOperator = "\""
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "EscapeOperator" {
			switch vt := v.(type) {
			case string:
				di.escapeOperator = v.(string)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "ParseHeader" {
			switch vt := v.(type) {
			case bool:
				di.parseHeader = v.(bool)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "UseCache" {
			switch vt := v.(type) {
			case bool:
				di.useCache = v.(bool)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}

		} else if k == "StrictMap" {
			switch vt := v.(type) {
			case bool:
				di.strictMap = v.(bool)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		} else if k == "StripFieldWS" {
			switch vt := v.(type) {
			case string:
				di.stripFieldWS = v.(string)
			default:
				return di, DSV_INVALID_TYPE_FOR_OPTION.enhance(fmt.Errorf("FieldDelimiter got:%T,expected:string", vt))
			}
		}
	}

	di.escapeCombined = di.escapeOperator + di.fieldOperator
	di.lslen = len(di.lineSeparator)
	di.eolen = len(di.escapeOperator)
	di.folen = len(di.fieldOperator)
	di.fdlen = len(di.fieldDelimiter)
	di.eclen = di.eolen + di.folen

	return di, nil
}

func (d dsvi) NormalizeString(s string) string {
	if d.stripFieldWS != "" {
		s = strings.Trim(s, d.stripFieldWS)
	}
	sl := len(s)
	if sl > d.folen && s[0:d.folen] == d.fieldOperator && s[sl-d.folen-1:] == d.fieldOperator {
		s = s[d.folen : sl-d.folen-1]
	}
	return s
}

func (d dsvi) DeserializeString(s string, tgt interface{}, opt ...int) error {
	// TODO: make sure i is a slice
	rs := reflect.ValueOf(tgt)
	if rs.Kind() != reflect.Ptr {
		return DSV_INVALID_TARGET_NOT_PTR
	}
	rs = rs.Elem()
	if rs.Kind() != reflect.Slice {
		return DSV_INVALID_TARGET_NOT_SLICE
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
	for i := 0; i < len(s); i++ {
		if slen < i+d.eclen && inqt && s[i:i+d.eclen] == d.escapeCombined {
			i += d.eclen - 1
			//TODO: TEST!
		} else if slen < i+d.folen && s[i:i+d.folen] == d.fieldOperator {
			inqt = true
		} else if !inqt && s[i:i+d.fdlen] == d.fieldDelimiter {
			ss := d.NormalizeString(s[l:i])
			lines[lnlen] = append(lines[lnlen], ss)
			i += d.fdlen
			l = i
			if d.parseHeader && lnlen == 0 {
				idxmap[len(idxmap)] = ss
			}
		} else if !inqt && s[i:i+d.lslen] == d.lineSeparator {
			ss := d.NormalizeString(s[l:i])
			lines[lnlen] = append(lines[lnlen], ss)
			if d.parseHeader && lnlen == 0 {
				idxmap[len(idxmap)] = ss
			}
			lnlen++
			lines = append(lines, []string{})
			i += d.lslen
			l = i
		}
	}
	lines[lnlen] = append(lines[lnlen], d.NormalizeString(s[l:]))

	offs := -1
	if !d.parseHeader {
		offs = 0
	}
	rs.Set(reflect.MakeSlice(reflect.SliceOf(typ), len(lines)+offs, len(lines)+offs))
	for i, ln := range lines {
		if i == 0 && d.parseHeader {
			continue
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
								//TODO: better reporting
								perr = errors.New(fmt.Sprintf("%v", r))
							}
						}()
						fs.Set(reflect.ValueOf(r))
						// TODO: this should be way more robust
					}()
					if perr != nil {
						return perr
						// TODO: CONST this
					}
				}
			}
		}
		rs.Index(i + offs).Set(fv)
	}

	return nil
}
