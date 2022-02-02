package dsv_test

import (
	"errors"
	"fmt"
	"testing"

	dsv "github.com/tony-o/dsv"
)

type TagTest struct {
	Id    int    `csv:"-"`
	Name  string `csv:"name"`
	Email string `csv:"email address"`
}

type TagTestArray []TagTest

func TestDSV_TagTestGood(t *testing.T) {
	a := `name, email address, whatever
name1, email1@xyz.com, 1
name2, email2@xyz.com, 2
name3, email3@xyz.com, 3`
	d, e := dsv.NewDSV(nil)
	if e != nil {
		t.FailNow()
	}
	tts := TagTestArray{}
	e = d.DeserializeString(a, &tts)
	if e != nil {
		t.FailNow()
	}

	expect := TagTestArray{
		TagTest{Name: "name1", Email: "email1@xyz.com"},
		TagTest{Name: "name2", Email: "email2@xyz.com"},
		TagTest{Name: "name3", Email: "email3@xyz.com"},
	}
	if len(tts) != len(expect) {
		t.FailNow()
	}

	for i, e := range expect {
		if e.Id != tts[i].Id {
			t.Logf("Id: index=%d,expected=%v,got=%v", i, e.Id, tts[i].Id)
			t.FailNow()
		}
		if e.Email != tts[i].Email {
			t.Logf("Email: index=%d,expected=%v,got=%v", i, e.Email, tts[i].Email)
			t.FailNow()
		}
		if e.Name != tts[i].Name {
			t.Logf("Name: index=%d,expected=%v,got=%v", i, e.Name, tts[i].Name)
			t.FailNow()
		}
	}
}

func TestDSV_TagTestTypesGood(t *testing.T) {
	types := []interface{}{TagTest{}, &TagTest{}, TagTestArray{}, &TagTestArray{}}
	exp := []interface{}{dsv.DSV_INVALID_TARGET_NOT_PTR, dsv.DSV_INVALID_TARGET_NOT_SLICE, dsv.DSV_INVALID_TARGET_NOT_PTR, true}
	for i, ty := range types {
		t.Run(fmt.Sprintf("%T", ty), func(t2 *testing.T) {
			d, e := dsv.NewDSV(nil)
			if e != nil {
				t2.FailNow()
			}
			e = d.DeserializeString("test", ty)
			switch exp[i].(type) {
			case error:
				if !errors.Is(e, exp[i].(error)) {
					t2.Logf("%T caused not the expected error: expected=%v,got=%v", ty, exp[i], e)
					t2.Fail()
				}
			case bool:
				if e != nil && exp[i].(bool) {
					t2.Logf("%T caused unexpected error: %v", ty, e)
					t2.Fail()
				}
			}
		})
	}
}

type BadTagTest struct {
	Id    int    `csv:"-"`
	Name  string `csv:"name"`
	Email string `csv:"name"`
}

func TestDSV_TestBad(t *testing.T) {
	a := "ehlo"
	d, e := dsv.NewDSV(nil)
	if e != nil {
		t.FailNow()
	}
	e = d.DeserializeString(a, &[]BadTagTest{})
	if !errors.Is(e, dsv.DSV_DUPLICATE_TAG_IN_STRUCT) {
		t.Errorf("Duplicate tags should return an error, got: %v", e)
		t.FailNow()
	}
}
