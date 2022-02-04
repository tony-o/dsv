package dsv

import (
	"fmt"
	"strconv"
)

func ifunc(s string, _ []byte) (interface{}, bool) {
	i, e := strconv.Atoi(s)
	if e != nil {
		return 0, false
	}
	return i, true
}
func uifunc(s string, _ []byte) (interface{}, bool) {
	u, e := strconv.ParseUint(s, 10, 64)
	if e != nil {
		return 0, false
	}
	return u, true
}
func ffunc(s string, _ []byte) (interface{}, bool) {
	f, e := strconv.ParseFloat(s, 64)
	if e != nil {
		return 0, false
	}
	return f, true
}

func cfunc(s string, _ []byte) (interface{}, bool) {
	c, e := strconv.ParseComplex(s, 128)
	if e != nil {
		return 0, false
	}
	return c, true
}

var (
	DefaultDeserializers = map[string]func(string, []byte) (interface{}, bool){
		"bool": func(s string, _ []byte) (interface{}, bool) {
			if s == "1" || s == "t" || s == "true" {
				return true, true
			}
			return false, true
		},
		"string": func(s string, _ []byte) (interface{}, bool) {
			return s, true
		},
		"int":     ifunc,
		"int8":    ifunc,
		"int16":   ifunc,
		"int32":   ifunc,
		"int64":   ifunc,
		"uint":    uifunc,
		"uint8":   uifunc,
		"uint16":  uifunc,
		"uint32":  uifunc,
		"uint64":  uifunc,
		"uintptr": uifunc,
		"byte": func(_ string, bs []byte) (interface{}, bool) {
			if len(bs) != 1 {
				return byte(0), false
			}
			return bs[0], true
		},
		"rune":       ifunc,
		"float32":    ffunc,
		"float64":    ffunc,
		"complex64":  cfunc,
		"complex128": cfunc,
	}

	// TODO: these need to escape output
	DefaultSerializers = map[string]func(interface{}) ([]byte, bool){
		"int": func(i interface{}) ([]byte, bool) {
			switch i.(type) {
			case int:
				return []byte(fmt.Sprintf("%d", i.(int))), true
			}
			return []byte{}, false
		},
		"string": func(i interface{}) ([]byte, bool) {
			switch i.(type) {
			case string:
				return []byte(i.(string)), true
			}
			return []byte{}, false
		},
	}
)
