package dsv_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	dsv "github.com/tony-o/dsv"
)

type LottoFields struct {
	A string  `csv:"A"`
	B int     `csv:"B"`
	C float64 `csv:"C"`
	D string  `csv:"D"`
	E string  `csv:"E"`
	F string  `csv:"F"`
	G string  `csv:"G"`
	H string  `csv:"H"`
	I float32 `csv:"I"`
	J bool    `csv:"J"`
	K byte    `csv:"K"`
	L []byte  `csv:"L"`
}

func (a LottoFields) Cmp(b LottoFields) (bool, string) {
	if a.A != b.A {
		return false, "A"
	}
	if a.B != b.B {
		return false, "B"
	}
	if fmt.Sprintf("%0.2f", a.C) != fmt.Sprintf("%0.2f", b.C) {
		return false, "C"
	}
	if a.D != b.D {
		return false, "D"
	}
	if a.E != b.E {
		return false, "E"
	}
	if a.F != b.F {
		return false, "F"
	}
	if a.G != b.G {
		return false, "G"
	}
	if a.H != b.H {
		return false, "H"
	}
	if fmt.Sprintf("%0.2f", a.I) != fmt.Sprintf("%0.2f", b.I) {
		return false, "I"
	}
	if a.J != b.J {
		return false, "J"
	}
	if a.K != b.K {
		return false, "K"
	}

	if len(a.L) != len(b.L) {
		return false, "L"
	}
	for i := range a.L {
		if a.L[i] != b.L[i] {
			return false, "L"
		}
	}
	return true, ""
}

func TestDSV_Serialize_EnsureOrdering(t *testing.T) {
	return
	testCase := []LottoFields{}
	for i := 0; i < 2000; i++ {
		var b bool
		if rand.Intn(1000) < 500 {
			b = true
		}
		testCase = append(testCase, LottoFields{
			A: randStr(15),
			B: rand.Intn(2400),
			C: rand.Float64() * 2400,
			D: randStr(24),
			E: randStr(3),
			F: randStr(16),
			G: randStr(512),
			H: randStr(1),
			I: rand.Float32() * 10,
			J: b,
			K: ls[rand.Intn(len(ls))],
			L: []byte(randStr(5000)),
		})
	}

	d, e := dsv.NewDSV(dsv.DSVOpt{})
	if e != nil {
		t.Logf("failed to create dsv: %v", e)
		t.FailNow()
	}
	bs, e := d.Serialize(testCase)
	if e != nil {
		t.Logf("serialization error: %v", e)
		t.FailNow()
	}

	resultCase := []LottoFields{}
	e = d.Deserialize(bs, &resultCase)
	if e != nil {
		t.Logf("deserialization error: %v", e)
		t.FailNow()
	}
	if len(resultCase) != len(testCase) {
		t.Logf("deserialization length mismatch, expected=%d,got=%d", len(testCase), len(resultCase))
		t.FailNow()
	}
	for i := range testCase {
		if ok, fld := testCase[i].Cmp(resultCase[i]); !ok {
			av := reflect.ValueOf(testCase[i])
			af := av.FieldByName(fld)
			bv := reflect.ValueOf(resultCase[i])
			bf := bv.FieldByName(fld)
			t.Logf("deserialization error with field [%d]%s: expected=%+v,got=%+v", i, fld, af.Interface(), bf.Interface())
			t.FailNow()
		}
	}
}

var ls = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ls[rand.Intn(len(ls))]
	}
	return string(b)
}

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

type X struct {
	F float64
}

type Y struct {
	X *X `csv:"x"`
}

func TestDSV_Serialize_FullPkg(t *testing.T) {
	d := dsv.NewDSVMust(dsv.DSVOpt{
		ParseHeader: dsv.DBool(false),
		Serializers: dsv.DSerial(map[string]func(interface{}) ([]byte, bool){
			"*dsv_test.X": func(i interface{}) ([]byte, bool) {
				switch i.(type) {
				case *X:
					if i.(*X) == nil {
						return []byte("nil"), true
					}
					return []byte(fmt.Sprintf("+%0.0f", i.(*X).F)), true
				}
				return []byte{}, false
			},
		}),
	})
	bs, e := d.Serialize(&[]Y{
		{X: &X{F: 5.00}},
		{},
	})
	if e != nil {
		t.Logf("serialization error: %s", e)
		t.FailNow()
	}
	if string(bs) != "+5\nnil" {
		t.Logf("serialization wrong: expected=\"+5\\nnil\",got=%q", string(bs))
		t.FailNow()
	}
}
