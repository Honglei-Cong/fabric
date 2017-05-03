package parser

import (
	"strings"
	"errors"
	"bytes"
	"github.com/hyperledger/fabric/core/ledger"
	"unicode/utf8"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
)

type SelectStatement struct {
	Columns []string
	Tables []string
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

func (s *SelectStatement) Parse(toks []string) error {
	gotFrom := false
	for _, t := range toks {
		tok := strings.TrimSpace(t)
		if len(tok) > 0 {
			switch strings.ToUpper(tok) {
			case "FROM":
				gotFrom = true
			default:
				if !gotFrom {
					s.Columns = append(s.Columns, tok)
				} else {
					s.Tables = append(s.Tables, tok)
				}
			}
		}
	}

	if !gotFrom {
		return errors.New("Invalid Select statement")
	}
	return nil
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
		ite, err := q.GetStateRangeScanIterator(table, table, table+string(utf8.MaxRune))
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
	toks := strings.Split(strings.TrimSpace(stmt), " ")
	switch strings.ToUpper(toks[0]) {
	case "SELECT":
		s := &SelectStatement{}
		if err := s.Parse(toks); err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, errors.New("unknown sql " + sql)
	}

	return nil, nil
}
