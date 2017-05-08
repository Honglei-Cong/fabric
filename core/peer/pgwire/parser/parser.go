package parser

import (
	"strings"
	"errors"
	"bytes"
	"github.com/hyperledger/fabric/core/ledger"
	"unicode/utf8"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"regexp"
	"strconv"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"encoding/binary"
)

type SelectStatement struct {
	Fields  []string `json:"fields"`
	Tables  []string `json:"tables"`
	StartID uint64      `json:"start_id"`
	EndID   uint64      `json:"end_id"`
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
	pat := `(?i:select)\s+((?:\w+,\s+)*\w+)\s+(?i:from)\s+(\w+(?:,\s*\w+)*)(\s*(?i:where))*([[:ascii:]]*)`
	reg := regexp.MustCompile(pat)

	fields := []byte{}
	tables := []byte{}
	conds := []byte{}

	if reg.Match(src) {
		m := reg.FindSubmatch(src)
		//showMatch(m1)
		fields = m[1]
		tables = m[2]
		conds = m[4]
	} else {
		return errors.New("Failed to find select/from")
	}

	sel.Fields = strings.Split(string(fields), ",")
	sel.Tables = strings.Split(string(tables), ",")

	reg3 := regexp.MustCompile(`\s*(?i:id)\s*>\s*(\d+)`)
	if reg3.Match(conds) {
		m := reg3.FindSubmatch(conds)
		val, err := strconv.Atoi(string(m[1]))
		if err == nil {
			sel.StartID = uint64(val)
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
			sel.EndID = uint64(val)
		} else {
			return fmt.Errorf("invalid lt %s", string(conds))
		}
	} else {
		fmt.Println("no lt matched")
	}

	return nil
}

func createKey(table string, id uint64) string {
	const (
		minUnicodeRuneValue   = 0            //U+0000
		maxUnicodeRuneValue   = utf8.MaxRune //U+10FFFF - maximum (and unallocated) code point
		compositeKeyNamespace = "\x00"
		emptyKeySubstitute    = "\x01"
	)

	idBuf := new(bytes.Buffer)
	binary.Write(idBuf, binary.BigEndian, id)
	return compositeKeyNamespace + table + string(minUnicodeRuneValue) + string(idBuf.Bytes())
}

func (s SelectStatement) Execute(q ledger.QueryExecutor) StatementResults {
	var res StatementResults

	numCols := 2
	Cols := []ResultColumn {
		{Name: "Key", Typ: TypeString},
		{Name: "Value", Typ: TypeString},
	}
	rowC := NewRowContainer(Cols, 0)
	row := make([]Datum, numCols)

	for _, table := range s.Tables {
		startKey := table
		endKey := table+string(utf8.MaxRune)
		if s.StartID >= 0 {
			startKey = createKey(table, s.StartID)
		}
		if s.EndID >= 0 {
			endKey = createKey(table, s.EndID)
		}
		ite, err := q.GetStateRangeScanIterator(table, startKey, endKey)
		if err != nil {
			continue
		}
		for {
			kv, _ := ite.Next()
			if kv == nil {
				break
			}

			row[0] = NewDString(kv.(*queryresult.KV).Key)
			row[1] = NewDString(string(kv.(*queryresult.KV).Value))
			rowC.AddRow(row)
		}
		ite.Close()
	}

	res.ResultList = append(res.ResultList, Result{
		PGTag: "SELECT",
		Type: Rows,
		Columns: Cols,
		Rows: rowC,
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
