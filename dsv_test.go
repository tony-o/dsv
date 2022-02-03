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
	d, e := dsv.NewDSV(dsv.DSVOpt{})
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

type testO struct {
	Name   string
	Dsvo   dsv.DSVOpt
	Into   interface{}
	Len    func(interface{}) int
	Cmp    func(_, _ interface{}) (bool, string)
	Expect struct {
		RowCount int
		Value    interface{}
	}
	Data string
}

func TagTestCmp(i1, i2 interface{}) (bool, string) {
	t1 := i1.(*TagTestArray)
	t2 := i2.(*TagTestArray)
	if len(*t1) != len(*t2) {
		return false, fmt.Sprintf("Row length mismatch a=%d,b=%d", len(*t1), len(*t2))
	}
	for i, a := range *t1 {
		b := (*t2)[i]
		if a.Id != b.Id {
			return false, fmt.Sprintf("Id mismatch expect=%d,got=%d", a.Id, b.Id)
		}
		if a.Name != b.Name {
			return false, fmt.Sprintf("Name mismatch expect=%s,got=%s", a.Name, b.Name)
		}
		if a.Email != b.Email {
			return false, fmt.Sprintf("Email mismatch expect=%s,got=%s", a.Email, b.Email)
		}
	}
	return true, ""
}

type genericCSV struct {
	Field1 string `csv:"i"`
	Field2 string `csv:"has"`
	Field3 string `csv:"headers"`
	Field4 string `csv:"with"`
	Field5 string `csv:"a line\nbreak"`
}

func GenericCSVCmp(i1, i2 interface{}) (bool, string) {
	t1 := i1.(*[]genericCSV)
	t2 := i2.(*[]genericCSV)
	if len(*t1) != len(*t2) {
		return false, fmt.Sprintf("Row length mismatch a=%d,b=%d", len(*t1), len(*t2))
	}
	for i, a := range *t1 {
		b := (*t2)[i]
		if a.Field1 != b.Field1 {
			return false, fmt.Sprintf("Field1 mismatch expect=%s,got=%s", a.Field1, b.Field1)
		}
		if a.Field2 != b.Field2 {
			return false, fmt.Sprintf("Field2 mismatch expect=%s,got=%s", a.Field2, b.Field2)
		}
		if a.Field3 != b.Field3 {
			return false, fmt.Sprintf("Field3 mismatch expect=%s,got=%s", a.Field3, b.Field3)
		}
		if a.Field4 != b.Field4 {
			return false, fmt.Sprintf("Field4 mismatch expect=%s,got=%s", a.Field4, b.Field4)
		}
		if a.Field5 != b.Field5 {
			return false, fmt.Sprintf("Field5 mismatch expect=%s,got=%s", a.Field5, b.Field5)
		}
	}
	return true, ""

}

var tests = []testO{
	{
		Name: "tabs",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString("\t"),
		},
		Into: &(TagTestArray{}),
		Len:  func(i interface{}) int { return len(*(i.(*TagTestArray))) },
		Cmp:  TagTestCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 3,
			Value: &TagTestArray{
				TagTest{Name: "name1", Email: "email1@xyz.com"},
				TagTest{Name: "name2", Email: "email2@xyz.com"},
				TagTest{Name: "name3", Email: "email3@xyz.com"},
			},
		},
		Data: "name\temail address\twhatever\nname1\temail1@xyz.com\t1\nname2\temail2@xyz.com\t2\nname3\temail3@xyz.com\t3",
	},
	{
		Name: "multiline",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString(","),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 1,
			Value: &([]genericCSV{
				{Field1: "i", Field2: "am", Field3: "data", Field4: "with", Field5: "a line\nbreak"},
			}),
		},
		Data: `i,has,headers,with,"a line
break"
i,am,data,with,"a line
break"`,
	},
	{
		Name: "escaped multiline",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString(","),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 1,
			Value: &([]genericCSV{
				{Field1: "i", Field2: "am", Field3: "data", Field4: "with", Field5: "a line\nbreak"},
			}),
		},
		Data: `i,has,headers,with,a line\` + "\n" + `break
i,am,data,with,a line\` + "\n" + `break`,
	},
	{
		Name: "skip blank lines",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString(","),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 2,
			Value: &([]genericCSV{
				{Field1: "i1", Field2: "has1", Field3: "headers1", Field4: "with1", Field5: "a line1"},
				{Field1: "i2", Field2: "has2", Field3: "headers2", Field4: "with2", Field5: "a line2"},
			}),
		},
		Data: `i,has,headers,with,a line\
break
i1,has1,headers1,with1,a line1

i2,has2,headers2,with2,a line2
`,
	},
	{
		Name: "one column csv",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString(","),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 2,
			Value: &([]genericCSV{
				{Field1: "i1"},
				{Field1: "i2"},
			}),
		},
		Data: `i
i1

i2




`,
	},
	{
		Name: "empty csv",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString(","),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 0,
			Value:    &([]genericCSV{}),
		},
		Data: ``,
	},
	{
		Name: "multichar field delimiter, line separator, and escape character",
		Dsvo: dsv.DSVOpt{
			FieldDelimiter: dsv.DString("ABC"),
			FieldOperator:  dsv.DString("___"),
			EscapeOperator: dsv.DString("|||"),
			LineSeparator:  dsv.DString("&&&&&&&&&&&"),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 1,
			Value: &([]genericCSV{
				{Field1: "i", Field2: "am", Field3: "data", Field4: "with ABC and ___ in the middle"},
			}),
		},
		Data: `iABChasABCheadersABCwith&&&&&&&&&&&iABCamABCdataABC___with ABC and |||___ in the middle___`,
	},
	{
		Name: "zero length escape",
		Dsvo: dsv.DSVOpt{
			EscapeOperator: dsv.DString(""),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 1,
			Value: &([]genericCSV{
				{Field1: "\\hello"},
			}),
		},
		Data: `i
\hello`,
	},
	{
		Name: "zero length escape, no data",
		Dsvo: dsv.DSVOpt{
			EscapeOperator: dsv.DString(""),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 0,
			Value:    &([]genericCSV{}),
		},
		Data: `i`,
	},
	{
		Name: "zero length escape, data but nothing mapped",
		Dsvo: dsv.DSVOpt{
			EscapeOperator: dsv.DString(""),
		},
		Into: &([]genericCSV{}),
		Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
		Cmp:  GenericCSVCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 1,
			Value: &([]genericCSV{
				{Field1: ""},
			}),
		},
		Data: `i\
a`,
	},
}

func TestDSV_TagTestGoodOpts(t *testing.T) {
	for _, tst := range tests {
		t.Run(tst.Name, func(t2 *testing.T) {
			d, e := dsv.NewDSV(tst.Dsvo)
			if e != nil {
				t2.Logf("%s failed: create error %v", tst.Name, e)
				t2.FailNow()
			}
			switch tst.Into.(type) {
			case *[]TagTest:
				e = d.DeserializeString(tst.Data, (tst.Into.(*[]TagTest)))
			case *TagTestArray:
				e = d.DeserializeString(tst.Data, (tst.Into.(*TagTestArray)))
			case *[]genericCSV:
				e = d.DeserializeString(tst.Data, (tst.Into.(*[]genericCSV)))
			default:
				t2.Logf("%s failed: invalid type %T", tst.Name, tst.Into)
				t2.FailNow()
			}
			if e != nil {
				t2.Logf("%s failed: parse error %v", tst.Name, e)
				t2.FailNow()
			}
			if tst.Len(tst.Into) != tst.Expect.RowCount {
				fmt.Printf("tst.Into=%q\n", tst.Into)
				t2.Logf("%s failed: row count expected=%d,got=%d", tst.Name, tst.Expect.RowCount, tst.Len(tst.Into))
				t2.FailNow()
			}
			if pass, errstr := tst.Cmp(tst.Expect.Value, tst.Into); !pass {
				t2.Logf("%s failed: cmp fails with message: %s", tst.Name, errstr)
				t2.FailNow()
			}
		})
	}
}

func TestDSV_TagTestTypeReflection(t *testing.T) {
	types := []interface{}{TagTest{}, &TagTest{}, TagTestArray{}, &TagTestArray{}}
	exp := []interface{}{dsv.DSV_INVALID_TARGET_NOT_PTR, dsv.DSV_INVALID_TARGET_NOT_SLICE, dsv.DSV_INVALID_TARGET_NOT_PTR, true}
	for i, ty := range types {
		t.Run(fmt.Sprintf("%T", ty), func(t2 *testing.T) {
			d, e := dsv.NewDSV(dsv.DSVOpt{})
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
	d, e := dsv.NewDSV(dsv.DSVOpt{})
	if e != nil {
		t.FailNow()
	}
	e = d.DeserializeString(a, &[]BadTagTest{})
	if !errors.Is(e, dsv.DSV_DUPLICATE_TAG_IN_STRUCT) {
		t.Errorf("Duplicate tags should return an error, got: %v", e)
		t.FailNow()
	}
}
