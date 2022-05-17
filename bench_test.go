package dsv_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gocarina/gocsv"

	"github.com/tony-o/dsv"
)

func logfield(b *testing.B, ls [][]byte, fn func(bs, q, e, d []byte) [][]byte) {
	for _, l := range ls {
		logese(b, true, fn(l, quote, escape, delim), 3)
	}
}

func logese(b *testing.B, show bool, xs [][]byte, l int) {
	if len(xs) != l {
		b.Logf("expected=%d,got=%d\n", l, len(xs))
		if show {
			for _, x := range xs {
				b.Logf("%q\n", x)
			}
		}
		b.FailNow()
	}
}

func Benchmark_NDSV_Lines1(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logese(b, false, dsv.Lines1(bs, quote, escape, ender), 49)
	}
}

func Benchmark_NDSV_Lines2(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logese(b, false, dsv.Lines2(bs, quote, escape, ender), 49)
	}
}

func Benchmark_NDSV_Lines3(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logese(b, false, dsv.Lines3(bs, quote, escape, ender), 49)
	}
}

func Benchmark_NDSV_Lines4(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logese(b, false, dsv.Lines4(bs, quote, escape, ender), 49)
	}
}

/*func Benchmark_NDSV_Doc1(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ls := dsv.Lines1(bs, quote, escape, ender)
		logese(b, false, ls, 48)
		logfield(b, ls, dsv.Lines1)
	}
}*/

func Benchmark_NDSV_Doc2(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ls := dsv.Lines2(bs, quote, escape, ender)
		logese(b, false, ls, 49)
		logfield(b, ls, dsv.Lines2)
	}
}

func Benchmark_NDSV_Doc3(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ls := dsv.Lines3(bs, quote, escape, ender)
		logese(b, false, ls, 49)
		logfield(b, ls, dsv.Lines3)
	}
}

func Benchmark_NDSV_Doc4(b *testing.B) {
	bs := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ls := dsv.Lines4(bs, quote, escape, ender)
		logese(b, false, ls, 49)
		logfield(b, ls, dsv.Lines4)
	}
}

type Client struct {
	Id      string `csv:"client_id"`
	Name    string `csv:"client_name"`
	Age     int    `csv:"client_age"`
	NotUsed string `csv:"-"`
}

func Benchmark_NDSV_Deserialize(b *testing.B) {
	cs := []Client{}
	lines := strings.Split(data, "\n")
	llen := len(lines) - 1
	data2 := []byte(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e := dsv.Deserialize4(&cs, data2, quote, escape, ender, delim); len(cs) != llen || e != nil {
			b.Logf("error=%v (b.N=%d,rows=%d,got=%d)", e, b.N, llen, len(cs))
			for i, c := range cs {
				b.Logf(" %d: %+v\n", i, c)
			}
			b.FailNow()
		}
	}
	b.StopTimer()
}

func Benchmark_GOCSV_Deserialize(b *testing.B) {
	cs := []Client{}
	lines := strings.Split(data, "\n")
	llen := len(lines) - 1
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e := gocsv.UnmarshalString(data, &cs); len(cs) != llen || e != nil {
			b.Logf("error=%v (b.N=%d,rows=%d,got=%d)", e, b.N, llen, len(cs))
			for i, c := range cs {
				fmt.Printf(" %d: %+v\n", i, c)
			}
			b.FailNow()
		}
	}
	b.StopTimer()
}

var quote []byte = []byte(`"`)
var escape []byte = []byte(`\`)
var ender []byte = []byte("\n")
var delim []byte = []byte(`,`)
var data string = `client_id,client_name,client_age
AZGEPSWUXRBICNF,CAWFOMGEKYIQPDXZTRV,28
KMDGBZER,BCFXOJLKSTDZWQ,14
BVOKYLSI,OEMSIJWCHTRY,73
WNDBTY,SGMQOJRTAXECBIFLZWHY,46
TFWEXUQMKNGYZPRJAVIDOLHB,YKPFMGVIBXCULRHWAE,70
MJQXICZDGVHOWEFBNLPU,QCIBAFWJNUPXMYKV,73
OSIDVLZER,HNCQDUIGFRYVLZABMKXJPO,7
TGOAPKVYC,DWMTLFZNUAVSJR,81
LMXPNSWHUZIG,QPVJLDSUXTMYBGFEIAOKH,72
XHDLWRC,XDZEQ,71
JLIOADFTNKQHXVZGMYPCWRSU,BDSRYPUNXV,79
UFPTYXDJCLNWRIKVM,BWSKLPXA,36
VHLSOWRYXFQTUIAKEPGJDC,ZTSAPMEGWJFNBRIDVLO,18
JTASGBXULCNZIHWM,XUALROZESGNPBCHVFIMYDTJQWK,68
QKTXLHANCDWJROUZV,UTDSQYHZJCMBOKWPLNR,67
ZHERUWXYJQVD,CBSTHUPNJVLAGQEMXIDOKFYWR,13
AOUEFDJQIKRYSMWNPXBLCVT,LDYGOPAEIRZXTWHKB,30
PUCRLT,MDIWLXEBSRCPGAZFTHNQVOJYUK,68
JQBZIVELYRSFKH,CHWJRXEKGBNVUZ,65
JQXPAKTLYNUOCIWGM,ILEUJHQGRCVBSFXKWMYNTODA,26
XWINJRLQUOSGHYCA,ROYUWQEMZSNCJVLPDKTBHF,43
IJNFYCDPAZWXGLBTKVHU,LIRJQHNFSCUPVAMWTKEGODBX,71
IAOLZSWNDYPFGMKBEJH,XGRFUYBTOAEJK,86
GPINESOQALXVR,SUTERXJNBPQLDCZWHFIMKAVO,80
WMLBGJD,YLXEKPJ,11
YGODXIHABN,ZYRGKQFTCLABWNPIMJUOD,16
PBGQSMKEJOXF,NHCLI,49
XLEQJRP,YWQHISLBJPZTFAXVKENCUDMGRO,81
DSNWFJ,NCDXBYGTEIKLWHJAFUQSMROZPV,60
UEJFCMHP,VLWEAJ,35
DJIUVCKLRWEBG,WSAOQHYVTNGLRBPCIKMJE,35
LDTYSVNREFOQHXKIGJWZAMC,EQCYNUTLISKOMHBDZX,60
QJSUKDBYGTRFMEHLPXOZWANI,BGVWLJNCXYSU,81
CZAYIXKED,ONWQF,55
YLKEDCMIFWZJA,FSUAYHKMEXRWNPDJI,9
MVZKLNCQEBOISH,BZPAWDNL,35
NCSEYVZFQHRMXTP,GLWSHJYXCUEPAIRBZVOFMDTKQN,42
DENMJYRUHWTZGKQAB,LYABUVWPTOXSDCGJ,61
VJHDPCOBZXW,FWQJOAR,69
XKEJBDOLHZFPRCMNYUG,LMCYOHFBEPXRUISZAWGJTVDNQK,18
OFPXIHZSTB,OZGIUKTMYEPHF,7
UOTEAPY,UKPNMGQTFIW,71
IASBHWPOQDUZTNMGR,KXPJGSFWRMICNVA,44
SRJEXQDFPHZTGAMYLCB,UXDZGSVMTNFRQYPECHIKALJBOW,25
UENCPZ,UQCTNBVZMGEPRJ,27
EHNYMRPIUOQZXSVBWJL,HMFWEIRVDCUTGPZAB,38
PSELMDUZI,VQMBEP,47
WYEZRLPVINAUTKSHFX,ZOAXGFURIMDSPWT,78` /*
"hello","world
", 56
one,\
two,57
`*/
