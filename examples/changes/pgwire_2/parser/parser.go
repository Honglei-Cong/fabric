package parser

import (
	"fmt"
	"strings"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("pgparser")

func Parse(sql string) ([]Statement, error) {
	stmt := strings.SplitN(sql, ";", 2)[0]

	switch strings.ToUpper(stmt[:3]) {
	case "SET":
		s := &Set{}
		return []Statement{s}, nil
	case "SEL":
		s := &SelectStatement{nil, nil, -1, -1}

		if err := s.Parse(stmt); err != nil {
			return nil, err
		}
		return []Statement{s}, nil
	}

	return []Statement{}, fmt.Errorf("unsupported stmt %s", stmt)
}
