package dsv_test

import (
	"errors"
	"fmt"
	"testing"

	dsv "github.com/tony-o/dsv"
)

type TagTest struct {
	Id    int    `csv:"id"`
	Name  string `csv:"name"`
	Email string `csv:"email address"`
}

type TagTestArray []TagTest

func TestDSV_Deserialize_TagTestGood(t *testing.T) {
	a := `name,email address,whatever
name1,email1@xyz.com,1
name2,email2@xyz.com,2
name3,email3@xyz.com,3`
	d := dsv.NewDSV(true, []byte("\n"), []byte(","), []byte("\\"), []byte("\""))
	tts := TagTestArray{}
	e := d.Deserialize([]byte(a), &tts)
	if e != nil {
		t.FailNow()
	}

	expect := TagTestArray{
		TagTest{Name: "name1", Email: "email1@xyz.com"},
		TagTest{Name: "name2", Email: "email2@xyz.com"},
		TagTest{Name: "name3", Email: "email3@xyz.com"},
	}
	if len(tts) != len(expect) {
		t.Logf("Did not receive the correct number of results: expected:%d,got:%d", len(expect), len(tts))
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
	Dsvo   [][]byte
	Into   interface{}
	Len    func(interface{}) int
	Cmp    func(_, _ interface{}) (bool, string)
	Expect struct {
		RowCount int
		Value    interface{}
	}
	Data string
	Map  map[int][]string
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
		Dsvo: [][]byte{[]byte("\n"), []byte("\t"), []byte("\\"), []byte("\"")},
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
		Map: map[int][]string{
			0: []string{"name", "email address", "whatever"},
			1: []string{"name1", "email1@xyz.com", "1"},
			2: []string{"name2", "email2@xyz.com", "2"},
			3: []string{"name3", "email3@xyz.com", "3"},
		},
	},
	{
		Name: "multiline",
		Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
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
		Map: map[int][]string{
			0: []string{"i", "has", "headers", "with", "a line\nbreak"},
			1: []string{"i", "am", "data", "with", "a line\nbreak"},
		},
	}, /*
			{
				Name: "escaped multiline",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
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
				Map: map[int][]string{
					0: []string{"i", "has", "headers", "with", "a line\nbreak"},
					1: []string{"i", "am", "data", "with", "a line\nbreak"},
				},
			},
			{
				Name: "skip blank lines",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
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
				Map: map[int][]string{
					0: []string{"i", "has", "headers", "with", "a line\nbreak"},
					1: []string{"i1", "has1", "headers1", "with1", "a line1"},
					2: []string{"i2", "has2", "headers2", "with2", "a line2"},
				},
			},
			{
				Name: "one column csv",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
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
				Map: map[int][]string{
					0: []string{"i"},
					1: []string{"i1"},
					2: []string{"i2"},
				},
			},
			{
				Name: "empty csv",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
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
				Map:  map[int][]string{},
			},
			{
				Name: "multichar field delimiter, line separator, and escape character",
				Dsvo: [][]byte{[]byte("&&&&&&&&&&&"), []byte("ABC"), []byte("|||"), []byte("___")},
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
				Map: map[int][]string{
					0: []string{"i", "has", "headers", "with"},
					1: []string{"i", "am", "data", "with ABC and ___ in the middle"},
				},
			},
			{
				Name: "zero length escape",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte(""), []byte("\"")},
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
				Map: map[int][]string{
					0: []string{"i"},
					1: []string{"\\hello"},
				},
			},
			{
				Name: "zero length escape, no data",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte(""), []byte("\"")},
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
				Map:  map[int][]string{0: []string{"i"}},
			},
			{
				Name: "zero length escape, data but nothing mapped",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte(""), []byte("\"")},
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
		a`, // note to self: Field1 should be "" because header `i\` doesn't map in Expect but should in the returned `Map`
				Map: map[int][]string{0: []string{"i\\"}, 1: []string{"a"}},
			},
			{
				Name: "strange delimiter eats correctly",
				Dsvo: [][]byte{[]byte("&"), []byte(","), []byte("0"), []byte("\"")},
				Into: &([]genericCSV{}),
				Len:  func(i interface{}) int { return len(*(i.(*[]genericCSV))) },
				Cmp:  GenericCSVCmp,
				Expect: struct {
					RowCount int
					Value    interface{}
				}{
					RowCount: 2,
					Value: &([]genericCSV{
						{Field1: "0", Field2: "000000"},
						{Field1: "o1", Field2: "o2"},
					}),
				},
				Data: `i0has&\00\0\0\0\0\0\0&o10o2`,
				Map: map[int][]string{
					0: []string{"i", "has"},
					1: []string{"0", "000000"},
					2: []string{"o1", "o2"},
				},
			},
			{
				Name: "custom deserializers",
				Dsvo: [][]byte{[]byte("\n"), []byte(","), []byte("\\"), []byte("\"")},
				/*Dsvo: dsv.DSVOpt{
					Deserializers: dsv.DDeserial(map[string]func(string, []byte) (interface{}, bool){
						"int": func(s string, _ []byte) (interface{}, bool) {
							i, e := strconv.Atoi(s)
							if e != nil {
								return -1, true
							}
							return i * 2, true
						},
					}),
				},*/ /*
		Into: &(TagTestArray{}),
		Len:  func(i interface{}) int { return len(*(i.(*TagTestArray))) },
		Cmp:  TagTestCmp,
		Expect: struct {
			RowCount int
			Value    interface{}
		}{
			RowCount: 3,
			Value: &TagTestArray{
				TagTest{Id: -1, Name: "name1"},
				TagTest{Id: 10, Name: "name2"},
				TagTest{Id: -20, Name: "name3"},
			},
		},
		Data: "id,name\na,name1\n5,name2\n-10,name3\n",
		Map: map[int][]string{
			0: []string{"id", "name"},
			1: []string{"a", "name1"},
			2: []string{"5", "name2"},
			3: []string{"-10", "name3"},
		},
	},*/
}

func TestDSV_Deserialize_TagTestGoodOpts(t *testing.T) {
	for _, tst := range tests {
		t.Run(tst.Name, func(t2 *testing.T) {
			d := dsv.NewDSV(true, tst.Dsvo[0], tst.Dsvo[1], tst.Dsvo[2], tst.Dsvo[3])
			var e error
			switch tst.Into.(type) {
			case *[]TagTest:
				e = d.Deserialize([]byte(tst.Data), (tst.Into.(*[]TagTest)))
			case *TagTestArray:
				e = d.Deserialize([]byte(tst.Data), (tst.Into.(*TagTestArray)))
			case *[]genericCSV:
				e = d.Deserialize([]byte(tst.Data), (tst.Into.(*[]genericCSV)))
			default:
				t2.Logf("%s failed: invalid type %T", tst.Name, tst.Into)
				t2.FailNow()
			}
			if e != nil {
				t2.Logf("%s failed: parse error %v", tst.Name, e)
				t2.FailNow()
			}
			if tst.Len(tst.Into) != tst.Expect.RowCount {
				fmt.Printf("tst.Into=%v\n", tst.Into)
				t2.Logf("%s failed: row count expected=%d,got=%d", tst.Name, tst.Expect.RowCount, tst.Len(tst.Into))
				t2.FailNow()
			}
			if pass, errstr := tst.Cmp(tst.Expect.Value, tst.Into); !pass {
				t2.Logf("%s failed: cmp fails with message: %s", tst.Name, errstr)
				t2.FailNow()
			}
			if len(tst.Map) > 0 {
				/*ms, e := d.DeserializeMapIndex(tst.Data)
				if e != nil {
					t2.Logf("%s failed: deserializing to map failed %v", tst.Name, e)
					t2.FailNow()
				}
				if len(ms) != len(tst.Map) {
					t2.Logf("%s failed: deserializing to map count mismatch expected=%d,got=%d", tst.Name, len(tst.Map), len(ms))
					t2.FailNow()
				}
				for i, m := range ms {
					for j, tm := range tst.Map[i] {
						if m[j] != tm {
							t2.Logf("%s failed: %d,%d data mismatch expected=%q,got=%q", tst.Name, i, j, tm, m[j])
							t2.FailNow()
						}
					}
				}
				*/
			}
		})
	}
}

func TestDSV_Deserialize_TagTestTypeReflection(t *testing.T) {
	types := []interface{}{TagTest{}, &TagTest{}, TagTestArray{}, &TagTestArray{}}
	exp := []interface{}{dsv.DSV_INVALID_TARGET_NOT_PTR, dsv.DSV_INVALID_TARGET_NOT_SLICE, dsv.DSV_INVALID_TARGET_NOT_PTR, true}
	for i, ty := range types {
		t.Run(fmt.Sprintf("%T", ty), func(t2 *testing.T) {
			d := dsv.NewDSV(true, []byte("\n"), []byte(","), []byte("\\"), []byte("\""))
			e := d.Deserialize([]byte("test"), ty)
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

func TestDSV_Deserialize_TestBad(t *testing.T) {
	a := "ehlo"
	d := dsv.NewDSV(true, []byte("\n"), []byte(","), []byte("\\"), []byte("\""))
	e := d.Deserialize([]byte(a), &[]BadTagTest{})
	if !errors.Is(e, dsv.DSV_DUPLICATE_TAG_IN_STRUCT) {
		t.Errorf("Duplicate tags should return an error, got: %v", e)
		t.FailNow()
	}
}

type InterestPaymentMonitoringRecord struct {
	Address                                 string `csv:"Address"`
	Unit                                    string `csv:"Unit"`
	City                                    string `csv:"City"`
	State                                   string `csv:"State"`
	Postal                                  string `csv:"Postal"`
	County                                  string `csv:"County Name"`
	LoanStatus                              string `csv:"Loan Status"`
	BorrowerName                            string `csv:"Borrower Name"`
	WarmBodyName                            string `csv:"Warm Body"`
	DefaultStatus                           string `csv:"Default Status"`
	PayoffStatus                            string `csv:"Payoff Status"`
	Status                                  string `csv:"Status"`
	Owner                                   string `csv:"Owner"`
	PoolSale                                string `csv:"Pool Sale"`
	AlphaFlowLoanID                         string `csv:"AlphaFlow Loan ID"`
	Originator                              string `csv:"Originator"`
	OriginatorLoanID                        string `csv:"Originator Loan ID"`
	SubservicerLoanID                       string `csv:"Subservicer Loan ID"`
	Subservicer                             string `csv:"SubServicer"`
	ServiceTransfer                         string `csv:"Service Transfer"`
	BoardingStatus                          string `csv:"BoardingStatus"`
	DateBoardedWithServicer                 string `csv:"Date Boarded w/ servicer"`
	BoardingAgingDaysFromPurchaseDate       string `csv:"Boarding Aging Days from Purchase Date"`
	ReasonForBoardingDelay                  string `csv:"Reason for Boarding Delay if > 7 Days (From Date Sent to Servicer to Live)"`
	InterestOnTotalLoanAmount               string `csv:"Interest on Total Loan Amount"`
	TotalInterestEscrow                     string `csv:"Total Interest Escrow"`
	NetInterestReserve                      string `csv:"Net Interest Reserve (Note: Neg. Offset to Net to Lender)"`
	CurrentInterestReserve                  string `csv:"Current Interest Reserve"`
	OutstandingPrincipalBalanceAtAFPurchase string `csv:"Outstanding principal balance at AF purchase"`
	CurrentPrincipalBalanceFundsFromAf      string `csv:"Current principal balance (funds from AF/investor)"`
	CurrentPrincipalBalanceFundsToBorrower  string `csv:"Current principal balance (funds to borrower)"`
	TotalLoanAmount                         string `csv:"Total Loan Amount"`
	EndingPrincipalBalance                  string `csv:"Ending Principal Balance"`
	ServicerUPB                             string `csv:"Servicer UPB"`
	OriginationDate                         string `csv:"Origination Date"`
	PurchaseDate                            string `csv:"Purchase Date"`
	FirstPaymentDueDateToServicer           string `csv:"First Payment Due Date to Servicer"`
	FirstPaymentDate                        string `csv:"First payment date"`
	PreviousPaymentDate                     string `csv:"Previous Payment Date"`
	NextPaymentDueDate                      string `csv:"Next Payment Due Date"`
	PaymentAmount                           string `csv:"Payment Amount"`
	InterestReserveExpirationDate           string `csv:"Interest Reserve Expiration Date"`
	PaymentGraceDays                        string `csv:"Payment Grace Days"`
	OriginalMaturityDate                    string `csv:"Original Maturity Date"`
	MaturityDate                            string `csv:"MaturityDate"`
	ExtendedToMaturityDate                  string `csv:"Extended to Maturity Date"`
	DaysToMaturity                          string `csv:"Days to Maturity"`
	MaturityBucket                          string `csv:"Maturity Bucket"`
	PayoffFundsReceivedByServicerDate       string `csv:"Payoff Funds Received By Servicer Date"`
	MonthsToPayoffFromPurchaseDate          string `csv:"Months to Payoff From Purchase Date"`
	InsuranceStatus                         string `csv:"Insurance Status"`
	TaxStatus                               string `csv:"Tax Status"`
	PMTStatus                               string `csv:"PMT Status"`
	PMTBucket                               string `csv:"PMT Bucket"`
	PMTTracking                             string `csv:"PMT Tracking"`
	DaysPastDue                             string `csv:"Days Past Due"`
	ACHStatus                               string `csv:"ACH Status"`
	ACHSteps                                string `csv:"ACH Steps"`
	RFD                                     string `csv:"RFD"`
	PctOfCompletion                         string `csv:"% of Completion"`
	ProjectStage                            string `csv:"Project Stage"`
	Comments                                string `csv:"Comments"`
	RehabFundsDisbursed                     string `csv:"Rehab Funds Disbursed"`
	ConstructionReserve                     string `csv:"Construction Reserve"`
	CorrectFirstPaymentDate                 string `csv:"Correct First Payment Date"`
	Check                                   string `csv:"Check"`
	Blank                                   string `csv:""`
	UnappliedPayment                        string `csv:"Unapplied Payment"`
	UpdatedValuation                        string `csv:"Updated Valuation"`
	ValuationDate                           string `csv:"Valuation Date"`
	InitialAiv                              string `csv:"Initial AIV"`
	Arv                                     string `csv:"ARV"`
	LoanPurpose                             string `csv:"Loan Purpose"`
	GrossRate                               string `csv:"Gross Rate %"`
	NetRateToAlphaFlow                      string `csv:"Net Rate to AlphaFlow"`
	BegEscrowDate                           string `csv:"Beg Escrow Date"`
	EndEscrowDate                           string `csv:"End Escrow Date"`
	DateSoldToInvestor                      string `csv:"Date Sold to Investor"`
	NetToPurchaserEachMonth                 string `csv:"Net to Purchaser Each Month"`
	NetToLenderEachMonth                    string `csv:"Net to Lender Each Month"`
	InitialLoanAmount                       string `csv:"Initial Loan Amount at origination"`
	OriginalTerm                            string `csv:"Original term (months)"`
	ERecordable                             string `csv:"E-Recordable"`
	InterestPerDiem                         string `csv:"Interest Per Diem"`
	TradeGroup                              string `csv:"Loan Trade Group"`
	BuyTradeGroup                           string `csv:"Buy Trade Group"`
	SellTradeGroup                          string `csv:"Sell Trade Group"`
	Buyer                                   string `csv:"Buyer"`
	InterestAccrualMethod                   string `csv:"Interest Accrual Method"`
}

type InterestPaymentMonitoringRecords []InterestPaymentMonitoringRecord

func TestDSV_RealData(tt *testing.T) {
	data := `Status,ARV,Net to Purchaser Each Month,Address,Total Loan Amount,Next Payment Due Date,Gross Rate %,Interest Per Diem,Postal,Current principal balance (funds to borrower),Origination Date,Initial AIV,Net to Lender Each Month,Original term (months),E-Recordable,Borrower Name,MaturityDate,Days Past Due,Correct First Payment Date,Loan Purpose,Date Boarded w/ servicer,Servicer UPB,Original Maturity Date,PMT Tracking,Loan Trade Group,City,Pool Sale,Subservicer Loan ID,SubServicer,Outstanding principal balance at AF purchase,Maturity Bucket,Construction Reserve,Sell Trade Group,Purchase Date,Extended to Maturity Date,Insurance Status,ACH Steps,Comments,State,Default Status,Payoff Status,AlphaFlow Loan ID,Current Interest Reserve,Payment Grace Days,Days to Maturity,Payoff Funds Received By Servicer Date,RFD,Loan Status,Originator,Total Interest Escrow,Ending Principal Balance,First payment date,Months to Payoff From Purchase Date,Beg Escrow Date,End Escrow Date,Reason for Boarding Delay if > 7 Days (From Date Sent to Servicer to Live),Interest on Total Loan Amount,% of Completion,Project Stage,Updated Valuation,BoardingStatus,Boarding Aging Days from Purchase Date,Tax Status,Unapplied Payment,Initial Loan Amount at origination,Unit,Payment Amount,Interest Reserve Expiration Date,PMT Status,Valuation Date,Buy Trade Group,Service Transfer,ACH Status,Rehab Funds Disbursed,Date Sold to Investor,Interest Accrual Method,County Name,Warm Body,Owner,Originator Loan ID,Net Interest Reserve (Note: Neg. Offset to Net to Lender),Previous Payment Date,Current principal balance (funds from AF/investor),First Payment Due Date to Servicer,PMT Bucket,Check,Net Rate to AlphaFlow,Buyer
Purchased,$600,000.00,,352 Angier Ave Northeast,$428,760.00,,10.700%,,30312,$216,795.00,2018-12-14,$460,000.00,,11,,Delimited LLC,,,,Value Add,,,2019-12-13,,Trade 1A,Atlanta,,,Cohen,$216,795.00,Expired,$211,965.00,Trade 1A,2018-12-07,2019-12-13,,,,GA,,,0e43abe1-5a6d-43f6-bd03-fba310f627a6,,,-784,,,,LendLendLend,$0.00,,2019-02-01,,2018-12-14,,,Yes,,,,,,,,$216,795.00,,,,,2018-12-07,Trade 1,,,$0.00,,30/360,,Frank Bloom,,1972321234,,,$428,760.00,,,,,Blackstone
Sold to investor,$600,000.00,,352 Angier Ave Northeast,$428,760.00,,10.700%,,30312,$216,795.00,2018-12-14,$460,000.00,,11,,Delimited LLC,,,,Value Add,,,2019-12-13,,,Atlanta,,,,$216,795.00,Expired,$211,965.00,,2018-12-01,2019-12-13,,,,GA,,,0e43abe1-5a6d-43f6-bd03-fba310f627a7,,,-784,,,,LendLendLend,$0.00,,2019-02-01,,2018-12-14,,,Yes,,,,,,,,$216,795.00,,,,,2018-12-07,,,,$0.00,,30/360,,Frank Bloom,,1972321234,,,$428,760.00,,,,,
Purchased,$215,000.00,,6884 NW 30th Ave,$139,750.00,,9.990%,,33309,$139,750.00,2018-11-14,,,12,,Inspiron LLC,,,,Refinance,,,2019-12-01,,Trade 2A,Fort Lauderdale,,,FCI,$139,750.00,Expired,$0.00,Trade 2A,2018-12-01,2019-12-01,,,,FL,,,e09bec63-f065-4b46-9cd4-34152c16d8c2,,,-796,,,,LendLendLend,$0.00,,2019-01-01,,2018-11-14,,,,,,,,,,,$139,750.00,,,,,,Trade 2,,,$0.00,,30/360,,Moche Baruh,,1825626346,,,$139,750.00,,,,,Jefferies Financial Group
`
	d := dsv.NewDSV(true, []byte("\n"), []byte(","), []byte("\\"), []byte("\""))

	records := &InterestPaymentMonitoringRecords{}
	err := d.Deserialize([]byte(data), records)
	if err != nil {
		tt.Logf("deserialize err: %v", err)
		tt.FailNow()
	}
	if len(*records) != 3 {
		tt.Logf("failure: row length expected=3,got=%d", len(*records))
		tt.FailNow()
	}
}

func ipmrCmp(a, b InterestPaymentMonitoringRecord) bool {
	if a.Address != b.Address {
		return false
	}
	if a.Unit != b.Unit {
		return false
	}
	if a.City != b.City {
		return false
	}
	if a.State != b.State {
		return false
	}
	if a.Postal != b.Postal {
		return false
	}
	if a.County != b.County {
		return false
	}
	if a.LoanStatus != b.LoanStatus {
		return false
	}
	if a.BorrowerName != b.BorrowerName {
		return false
	}
	if a.WarmBodyName != b.WarmBodyName {
		return false
	}
	if a.DefaultStatus != b.DefaultStatus {
		return false
	}
	if a.PayoffStatus != b.PayoffStatus {
		return false
	}
	if a.Status != b.Status {
		return false
	}
	if a.Owner != b.Owner {
		return false
	}
	if a.PoolSale != b.PoolSale {
		return false
	}
	if a.AlphaFlowLoanID != b.AlphaFlowLoanID {
		return false
	}
	if a.Originator != b.Originator {
		return false
	}
	if a.OriginatorLoanID != b.OriginatorLoanID {
		return false
	}
	if a.SubservicerLoanID != b.SubservicerLoanID {
		return false
	}
	if a.Subservicer != b.Subservicer {
		return false
	}
	if a.ServiceTransfer != b.ServiceTransfer {
		return false
	}
	if a.BoardingStatus != b.BoardingStatus {
		return false
	}
	if a.DateBoardedWithServicer != b.DateBoardedWithServicer {
		return false
	}
	if a.BoardingAgingDaysFromPurchaseDate != b.BoardingAgingDaysFromPurchaseDate {
		return false
	}
	if a.ReasonForBoardingDelay != b.ReasonForBoardingDelay {
		return false
	}
	if a.InterestOnTotalLoanAmount != b.InterestOnTotalLoanAmount {
		return false
	}
	if a.TotalInterestEscrow != b.TotalInterestEscrow {
		return false
	}
	if a.NetInterestReserve != b.NetInterestReserve {
		return false
	}
	if a.CurrentInterestReserve != b.CurrentInterestReserve {
		return false
	}
	if a.OutstandingPrincipalBalanceAtAFPurchase != b.OutstandingPrincipalBalanceAtAFPurchase {
		return false
	}
	if a.CurrentPrincipalBalanceFundsFromAf != b.CurrentPrincipalBalanceFundsFromAf {
		return false
	}
	if a.CurrentPrincipalBalanceFundsToBorrower != b.CurrentPrincipalBalanceFundsToBorrower {
		return false
	}
	if a.TotalLoanAmount != b.TotalLoanAmount {
		return false
	}
	if a.EndingPrincipalBalance != b.EndingPrincipalBalance {
		return false
	}
	if a.ServicerUPB != b.ServicerUPB {
		return false
	}
	if a.OriginationDate != b.OriginationDate {
		return false
	}
	if a.PurchaseDate != b.PurchaseDate {
		return false
	}
	if a.FirstPaymentDueDateToServicer != b.FirstPaymentDueDateToServicer {
		return false
	}
	if a.FirstPaymentDate != b.FirstPaymentDate {
		return false
	}
	if a.PreviousPaymentDate != b.PreviousPaymentDate {
		return false
	}
	if a.NextPaymentDueDate != b.NextPaymentDueDate {
		return false
	}
	if a.PaymentAmount != b.PaymentAmount {
		return false
	}
	if a.InterestReserveExpirationDate != b.InterestReserveExpirationDate {
		return false
	}
	if a.PaymentGraceDays != b.PaymentGraceDays {
		return false
	}
	if a.OriginalMaturityDate != b.OriginalMaturityDate {
		return false
	}
	if a.MaturityDate != b.MaturityDate {
		return false
	}
	if a.ExtendedToMaturityDate != b.ExtendedToMaturityDate {
		return false
	}
	if a.DaysToMaturity != b.DaysToMaturity {
		return false
	}
	if a.MaturityBucket != b.MaturityBucket {
		return false
	}
	if a.PayoffFundsReceivedByServicerDate != b.PayoffFundsReceivedByServicerDate {
		return false
	}
	if a.MonthsToPayoffFromPurchaseDate != b.MonthsToPayoffFromPurchaseDate {
		return false
	}
	if a.InsuranceStatus != b.InsuranceStatus {
		return false
	}
	if a.TaxStatus != b.TaxStatus {
		return false
	}
	if a.PMTStatus != b.PMTStatus {
		return false
	}
	if a.PMTBucket != b.PMTBucket {
		return false
	}
	if a.PMTTracking != b.PMTTracking {
		return false
	}
	if a.DaysPastDue != b.DaysPastDue {
		return false
	}
	if a.ACHStatus != b.ACHStatus {
		return false
	}
	if a.ACHSteps != b.ACHSteps {
		return false
	}
	if a.RFD != b.RFD {
		return false
	}
	if a.PctOfCompletion != b.PctOfCompletion {
		return false
	}
	if a.ProjectStage != b.ProjectStage {
		return false
	}
	if a.Comments != b.Comments {
		return false
	}
	if a.RehabFundsDisbursed != b.RehabFundsDisbursed {
		return false
	}
	if a.ConstructionReserve != b.ConstructionReserve {
		return false
	}
	if a.CorrectFirstPaymentDate != b.CorrectFirstPaymentDate {
		return false
	}
	if a.Check != b.Check {
		return false
	}
	if a.Blank != b.Blank {
		return false
	}
	if a.UnappliedPayment != b.UnappliedPayment {
		return false
	}
	if a.UpdatedValuation != b.UpdatedValuation {
		return false
	}
	if a.ValuationDate != b.ValuationDate {
		return false
	}
	if a.InitialAiv != b.InitialAiv {
		return false
	}
	if a.Arv != b.Arv {
		return false
	}
	if a.LoanPurpose != b.LoanPurpose {
		return false
	}
	if a.GrossRate != b.GrossRate {
		return false
	}
	if a.NetRateToAlphaFlow != b.NetRateToAlphaFlow {
		return false
	}
	if a.BegEscrowDate != b.BegEscrowDate {
		return false
	}
	if a.EndEscrowDate != b.EndEscrowDate {
		return false
	}
	if a.DateSoldToInvestor != b.DateSoldToInvestor {
		return false
	}
	if a.NetToPurchaserEachMonth != b.NetToPurchaserEachMonth {
		return false
	}
	if a.NetToLenderEachMonth != b.NetToLenderEachMonth {
		return false
	}
	if a.InitialLoanAmount != b.InitialLoanAmount {
		return false
	}
	if a.OriginalTerm != b.OriginalTerm {
		return false
	}
	if a.ERecordable != b.ERecordable {
		return false
	}
	if a.InterestPerDiem != b.InterestPerDiem {
		return false
	}
	if a.TradeGroup != b.TradeGroup {
		return false
	}
	if a.BuyTradeGroup != b.BuyTradeGroup {
		return false
	}
	if a.SellTradeGroup != b.SellTradeGroup {
		return false
	}
	if a.Buyer != b.Buyer {
		return false
	}
	if a.InterestAccrualMethod != b.InterestAccrualMethod {
		return false
	}
	return true
}
