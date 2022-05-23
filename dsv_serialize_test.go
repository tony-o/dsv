package dsv_test

import (
	"fmt"
	"testing"

	"github.com/tony-o/dsv"
)

type Embedded struct {
	X int
}

type Basic struct {
	A      int      `dsv:"a"`
	B      string   `dsv:"b"`
	C      float64  `dsv:"c"`
	X      Embedded `dsv:"x"`
	Ignore string
}

func TestSerialize_Basic(t *testing.T) {
	bs := []Basic{
		{
			A:      1,
			B:      "one",
			C:      1.1,
			X:      Embedded{1},
			Ignore: "one again",
		},
		{
			A:      2,
			B:      "two",
			C:      2.2,
			X:      Embedded{2},
			Ignore: "two again",
		},
		{
			A:      3,
			B:      "\"\nthree\"",
			C:      3.3,
			X:      Embedded{3},
			Ignore: "whatever",
		},
	}

	/*dsv.Serializers["dsv_test.Embedded"] = func(i interface{}) (string, bool) {
		switch i.(type) {
		case Embedded:
			return fmt.Sprintf("xXx%dxXx", i.(Embedded).X), true
		}
		return "", false
	}*/
	d := dsv.NewDSV(true, []byte("\n"), []byte(","), []byte("\\"), []byte("\""))

	dsv, err := d.Serialize(bs)
	if err != nil {
		t.Logf("error: %v", err)
		t.FailNow()
	}
	exp := fmt.Sprintf("\"a\",\"b\",\"c\",\"x\"\n%d,%v,%f,xXx%dxXx\n%d,%v,%f,xXx%dxXx\n%d,\"\\\"\nthree\\\"\",%f,xXx%dxXx", bs[0].A, bs[0].B, bs[0].C, bs[0].X.X, bs[1].A, bs[1].B, bs[1].C, bs[1].X.X, bs[2].A, bs[2].C, bs[2].X.X)
	if string(dsv) != exp {
		t.Logf("\nexpected=%q\n     got=%q", exp, dsv)
		t.FailNow()
	}

	basics := []Basic{}
	d.Deserialize(dsv, &basics)

	dsv, err = d.Serialize(&(bs[0]))
	if err != nil {
		t.Logf("error: %v", err)
		t.FailNow()
	}
	exp = fmt.Sprintf("\"a\",\"b\",\"c\",\"x\"\n%d,%v,%f,xXx%dxXx", bs[0].A, bs[0].B, bs[0].C, bs[0].X.X)
	if string(dsv) != exp {
		t.Logf("\nexpected=%q\n     got=%q", exp, dsv)
		t.FailNow()
	}
}
