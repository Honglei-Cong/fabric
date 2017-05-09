package parser

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
	"encoding/json"
	"reflect"
)

type SelectStatement struct {
	Fields  []string `json:"fields"`
	Tables  []string `json:"tables"`
	StartID int64    `json:"start_id"`
	EndID   int64    `json:"end_id"`
}

func (s SelectStatement) StatementType() StatementType {
	return Rows
}

func (s SelectStatement) StatementTag() string {
	return "SELECT"
}

func (s SelectStatement) String() string {
	return ""
}

func (s SelectStatement) Format(buf *bytes.Buffer, flags FmtFlags) {
}

func (sel *SelectStatement) Parse(stmt string) error {
	src := []byte(stmt)
	pat := `(?i:select)\s+((?:[\w*]+,\s*)*[\w*]+)\s+(?i:from)\s+(\w+(?:,\s*\w+)*)(\s*(?i:where))*([[:ascii:]]*)`
	reg := regexp.MustCompile(pat)

	fields := []byte{}
	tables := []byte{}
	conds := []byte{}

	fmt.Println(">>> stmt:", stmt)

	if reg.Match(src) {
		m := reg.FindSubmatch(src)
		//showMatch(m1)
		fields = m[1]
		tables = m[2]
		conds = m[4]
	} else {
		return errors.New("Failed to find select/from")
	}

	for _, s := range strings.Split(string(fields), ",") {
		sel.Fields = append(sel.Fields, strings.TrimSpace(s))
	}
	for _, s := range strings.Split(string(tables), ",") {
		sel.Tables = append(sel.Tables, strings.TrimSpace(s))
	}

	reg3 := regexp.MustCompile(`\s*(?i:id)\s*>\s*(\d+)`)
	if reg3.Match(conds) {
		m := reg3.FindSubmatch(conds)
		val, err := strconv.Atoi(string(m[1]))
		if err == nil {
			sel.StartID = int64(val)
		} else {
			return fmt.Errorf("invalid gt %s", string(conds))
		}
	} else {
		fmt.Println("no gt matched")
	}

	reg4 := regexp.MustCompile(`\s*(?i:id)\s*<\s*(\d+)`)
	if reg4.Match(conds) {
		m := reg4.FindSubmatch(conds)
		val, err := strconv.Atoi(string(m[1]))
		if err == nil {
			sel.EndID = int64(val)
		} else {
			return fmt.Errorf("invalid lt %s", string(conds))
		}
	} else {
		fmt.Println("no lt matched")
	}

	return nil
}

const (
	minUnicodeRuneValue   = 0            //U+0000
	maxUnicodeRuneValue   = utf8.MaxRune //U+10FFFF - maximum (and unallocated) code point
	compositeKeyNamespace = "\x00"
	emptyKeySubstitute    = "\x01"
)

func createKey(table string, id int64) string {
	return compositeKeyNamespace + table + string(minUnicodeRuneValue) + fmt.Sprintf("%016x", id)
}

func (s SelectStatement) Execute(q ledger.QueryExecutor, ns string) StatementResults {
	var res StatementResults

	Cols := []ResultColumn{
		{Name: "Key", Typ: TypeString},
	}
	allColsNeeded := false
	for _, c := range s.Fields {
		if c == "*" {
			allColsNeeded = true
		}
	}
	if !allColsNeeded {
		for _, c := range s.Fields {
			Cols = append(Cols, ResultColumn{Name: c, Typ: TypeString})
		}
	}

	var rowC *RowContainer = nil
	var row []Datum

	for _, table := range s.Tables {
		keyPrefix := compositeKeyNamespace + table + string(minUnicodeRuneValue)
		startKey := keyPrefix
		endKey := compositeKeyNamespace + table + string(utf8.MaxRune)
		if s.StartID >= 0 {
			startKey = createKey(table, s.StartID)
		}
		if s.EndID >= 0 {
			endKey = createKey(table, s.EndID)
		}
		ite, err := q.GetStateRangeScanIterator(ns, startKey, endKey)
		if err != nil {
			continue
		}
		for {
			kv, _ := ite.Next()

			fmt.Printf(">> %v \n", kv)
			if kv == nil {
				break
			}

			if row == nil {
				if allColsNeeded {
					m := make(map[string]interface{})
					err := json.Unmarshal(kv.(*queryresult.KV).Value, &m)
					if err != nil {
						fmt.Printf("Failed to unmarshal %v, err: %s\n", string(kv.(*queryresult.KV).Value), err)
					}

					for k := range m {
						Cols = append(Cols, ResultColumn{Name: k, Typ: TypeString})
					}
				}

				row = make([]Datum, len(Cols))
				rowC = NewRowContainer(Cols, 0)
			}

			row[0] = NewDString(strings.TrimLeft(kv.(*queryresult.KV).Key, keyPrefix))

			m := make(map[string]interface{})
			err = json.Unmarshal(kv.(*queryresult.KV).Value, &m)
			if err != nil {
				fmt.Printf("Failed to unmarshal value %v, err: %s\n", string(kv.(*queryresult.KV).Value), err)
			}

			for i := 1; i < len(Cols); i++ {
				v, avail := m[Cols[i].Name]
				if avail {
					switch v := v.(type) {
					case string:
						row[i] = NewDString(v)
					case int64, uint64, int, uint:
						row[i] = NewDString(fmt.Sprintf("%d", v))
					case float64:
						row[i] = NewDString(fmt.Sprintf("%d", uint64(v)))
					default:
						row[i] = NewDString(fmt.Sprintf("type: %s", reflect.TypeOf(v).Name()))
					}
				} else {
					row[i] = NewDString("null")
				}
			}

			rowC.AddRow(row)
		}
		ite.Close()
	}

	res.ResultList = append(res.ResultList, Result{
		PGTag:   "SELECT",
		Type:    Rows,
		Columns: Cols,
		Rows:    rowC,
	})

	return res
}

func Parse(sql string) (Statement, error) {
	stmt := strings.SplitN(sql, ";", 2)[0]
	s := &SelectStatement{nil, nil, -1, -1}

	if err := s.Parse(stmt); err != nil {
		return nil, err
	}
	return s, nil
}
