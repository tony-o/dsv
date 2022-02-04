package dsv_test

import (
	"fmt"
	"testing"

	dsv "github.com/tony-o/dsv"
)

// TestDSV_Serialize_Basic basic test for serialization
func TestDSV_Serialize_Basic(t *testing.T) {
	d, e := dsv.NewDSV(dsv.DSVOpt{})
	if e != nil {
		t.Logf("failed to create dsv: %v", e)
		t.FailNow()
	}
	ts := &[]*TagTest{
		&TagTest{
			Id:    42,
			Name:  "nAME",
			Email: "eMAIL",
		},
		&TagTest{
			Id:    64,
			Name:  "NaMe",
			Email: "EmAiL",
		},
	}
	bs, e := d.Serialize(ts)
	if e != nil {
		t.Logf("serialization error: %v", e)
		t.FailNow()
	}

	xs := TagTestArray{}
	e = d.Deserialize(bs, &xs)
	if e != nil {
		t.Logf("deserialization error: %v", e)
		t.FailNow()
	}
	var ls TagTestArray = TagTestArray{*(*ts)[0], *(*ts)[1]}

	if ok, _ := TagTestCmp(&xs, &ls); ok {
		t.Logf("results failure: expected:\"id,name,email address\\n42,nAME,eMAIL\\n64,NaMe,EmAiL\", got:%q", string(bs))
		t.FailNow()
	}
}

// TestDSV_Serialize_Tests tests that the serializer data can be deserialized again and returns what's expected
func TestDSV_Serialize_Tests(t *testing.T) {
	for _, tst := range tests {
		t.Run(tst.Name, func(t2 *testing.T) {
			d := dsv.NewDSVMust(tst.Dsvo)
			bs, e := d.Serialize(tst.Expect)
			if e != nil {
				t.Logf("%s failed: serialization error %v", tst.Name, e)
				t.FailNow()
			}
			switch tst.Into.(type) {
			case *[]TagTest:
				e = d.Deserialize(bs, (tst.Into.(*[]TagTest)))
			case *TagTestArray:
				e = d.Deserialize(bs, (tst.Into.(*TagTestArray)))
			case *[]genericCSV:
				e = d.Deserialize(bs, (tst.Into.(*[]genericCSV)))
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
