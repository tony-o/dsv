package dsv

import (
	"fmt"
	"strconv"
	"strings"
)

func ifunc(s []byte) (interface{}, bool) {
	i, e := strconv.Atoi(string(s))
	if e != nil {
		return 0, false
	}
	return i, true
}
func i8func(s []byte) (interface{}, bool) {
	i, e := ifunc(s)
	return int8(i.(int64)), e
}
func i16func(s []byte) (interface{}, bool) {
	i, e := ifunc(s)
	return int8(i.(int64)), e
}
func i32func(s []byte) (interface{}, bool) {
	i, e := ifunc(s)
	return int8(i.(int64)), e
}
func uifunc(s []byte) (interface{}, bool) {
	u, e := strconv.ParseUint(string(s), 10, 64)
	if e != nil {
		return 0, false
	}
	return u, true
}
func ui8func(s []byte) (interface{}, bool) {
	i, e := uifunc(s)
	return uint8(i.(uint64)), e
}
func ui16func(s []byte) (interface{}, bool) {
	i, e := uifunc(s)
	return uint8(i.(uint64)), e
}
func ui32func(s []byte) (interface{}, bool) {
	i, e := uifunc(s)
	return uint8(i.(uint64)), e
}
func ffunc(s []byte) (interface{}, bool) {
	f, e := strconv.ParseFloat(string(s), 64)
	if e != nil {
		return 0, false
	}
	return f, true
}
func f32func(s []byte) (interface{}, bool) {
	f, t := ffunc(s)
	return float32(f.(float64)), t
}

func cfunc(s []byte) (interface{}, bool) {
	c, e := strconv.ParseComplex(string(s), 128)
	if e != nil {
		return 0, false
	}
	return c, true
}

func intser(i interface{}) ([]byte, bool) {
	switch i.(type) {
	case uint8, uint16, uint32, uint64, uint, int, int8, int16, int32, int64:
		return []byte(fmt.Sprintf("%d", i)), true
	case []uint8, []uint16, []uint32, []uint64, []uint, []int, []int8, []int16, []int32, []int64:
		return []byte(fmt.Sprintf("%v", i)), true
	}
	return []byte{}, false
}
func floatser(i interface{}) ([]byte, bool) {
	switch i.(type) {
	case float32, float64:
		return []byte(fmt.Sprintf("%f", i)), true
	}
	return []byte{}, false
}

func intdeser(s []byte) (interface{}, bool) {
	r := []int{}
	if len(s) == 0 {
		return r, true
	}
	if s[0] == '[' {
		s = s[1:]
	}
	if s[len(s)-1] == ']' {
		s = s[0 : len(s)-2]
	}
	values := strings.Split(string(s), " ")
	for _, v := range values {
		i, e := ifunc([]byte(v))
		if !e {
			return r, e
		}
		r = append(r, i.(int))
	}
	return r, true
}

func int8deser(s []byte) (interface{}, bool) {
	r := []int8{}
	if len(s) == 0 {
		return r, true
	}
	if s[0] == '[' {
		s = s[1:]
	}
	if s[len(s)-1] == ']' {
		s = s[0 : len(s)-2]
	}
	values := strings.Split(string(s), " ")
	for _, v := range values {
		i, e := i8func([]byte(v))
		if !e {
			return r, e
		}
		r = append(r, i.(int8))
	}
	return r, true
}
func uint8deser(s []byte) (interface{}, bool) {
	r := []uint8{}
	if len(s) == 0 {
		return r, true
	}
	if s[0] == '[' {
		s = s[1:]
	}
	if s[len(s)-1] == ']' {
		s = s[0 : len(s)-1]
	}
	values := strings.Split(string(s), " ")
	for _, v := range values {
		i, e := ui8func([]byte(v))
		if !e {
			return r, e
		}
		r = append(r, i.(uint8))
	}
	return r, true
}

var (
	DefaultDeserializers = map[string]func([]byte) (interface{}, bool){
		"bool": func(s []byte) (interface{}, bool) {
			if string(s) == "1" || string(s) == "t" || string(s) == "true" {
				return true, true
			}
			return false, true
		},
		"string": func(s []byte) (interface{}, bool) {
			return string(s), true
		},
		"[]int8":  int8deser,
		"int":     ifunc,
		"int8":    i8func,
		"int16":   i16func,
		"int32":   i32func,
		"int64":   ifunc,
		"[]uint8": uint8deser,
		"uint":    uifunc,
		"uint8":   ui8func,
		"uint16":  ui16func,
		"uint32":  ui32func,
		"uint64":  uifunc,
		"uintptr": uifunc,
		"byte": func(bs []byte) (interface{}, bool) {
			if len(bs) != 1 {
				return byte(0), false
			}
			return bs[0], true
		},
		"rune":       ifunc,
		"float32":    f32func,
		"float64":    ffunc,
		"complex64":  cfunc,
		"complex128": cfunc,
	}

	// TODO: these need to escape output
	DefaultSerializers = map[string]func(interface{}) ([]byte, bool){
		"string": func(i interface{}) ([]byte, bool) {
			switch i.(type) {
			case string:
				return []byte(i.(string)), true
			}
			return []byte{}, false
		},
		"float32": floatser,
		"float64": floatser,
		"bool": func(i interface{}) ([]byte, bool) {
			switch i.(type) {
			case bool:
				return []byte(fmt.Sprintf("%t", i.(bool))), true
			}
			return []byte{}, false
		},
		"[]int":    intser,
		"[]int8":   intser,
		"[]int16":  intser,
		"[]int32":  intser,
		"[]int64":  intser,
		"[]uint":   intser,
		"[]uint8":  intser,
		"[]uint16": intser,
		"[]uint32": intser,
		"[]uint64": intser,
		"int":      intser,
		"int8":     intser,
		"int16":    intser,
		"int32":    intser,
		"int64":    intser,
		"uint":     intser,
		"uint8":    intser,
		"uint16":   intser,
		"uint32":   intser,
		"uint64":   intser,
		"[]byte": func(i interface{}) ([]byte, bool) {
			// TODO escape output
			switch i.(type) {
			case []byte:
				return i.([]byte), true
			}
			return []byte{}, false
		},
	}
)
