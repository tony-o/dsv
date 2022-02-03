package dsv

import (
	"errors"
	"fmt"
	"strings"
)

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
