package parser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/core/chaincode/shim"
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

func (s *SelectStatement) String() string {
	return fmt.Sprintf("%v, %v, %d, %d", s.Fields, s.Tables, s.StartID, s.EndID)
}

func (s *SelectStatement) Format(buf *bytes.Buffer, flags FmtFlags) {
	buf.WriteString("SELECT ")
}

func (sel *SelectStatement) Parse(stmt string) error {
	src := []byte(stmt)
	pat := `(?i:select)\s+((?:["]?[\w*]+["]?,\s*)*["]?[\w*]+["]?)\s+(?i:from)\s+(\w+(?:,\s*\w+)*)(\s*(?i:where))*([[:ascii:]]*)`
	reg := regexp.MustCompile(pat)

	fields := []byte{}
	tables := []byte{}
	conds := []byte{}

	logger.Info(">>> stmt:", stmt)

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
		sel.Fields = append(sel.Fields, strings.Trim(strings.TrimSpace(s), "'\""))
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
		logger.Debug("no gt matched")
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
		logger.Debug("no lt matched")
	}

	reg5 := regexp.MustCompile(`\s*(\d+)\s*=\s*(\d+)`)
	if reg5.Match(conds) {
		m := reg5.FindSubmatch(conds)
		if bytes.Equal(m[1], m[2]) {
			sel.StartID = -1
			sel.EndID = -1
		} else {
			sel.StartID = 1
			sel.EndID = 0
		}
	} else {
		logger.Debug("no eq matched")
	}

	return nil
}

func (s *SelectStatement) BuildChaincodePrepareRequest() (*shim.SqlQuery, error) {
	selectOp := &shim.SelectQuery{
		Fields:  s.Fields,
		Tables:  s.Tables,
		StartID: 1,
		EndID:   0,
	}

	args, err := json.Marshal(selectOp)
	if err != nil {
		return nil, err
	}

	q := &shim.SqlQuery{
		Op:    "SELECT",
		OpArg: args,
	}

	return q, nil
}

func (s *SelectStatement) BuildChaincodeQueryRequest() (*shim.SqlQuery, error) {
	selectOp := &shim.SelectQuery{
		Fields:  s.Fields,
		Tables:  s.Tables,
		StartID: s.StartID,
		EndID:   s.EndID,
	}

	args, err := json.Marshal(selectOp)
	if err != nil {
		return nil, err
	}

	q := &shim.SqlQuery{
		Op:    "SELECT",
		OpArg: args,
	}

	return q, nil
}
