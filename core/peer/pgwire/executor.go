package pgwire

import (
	"github.com/hyperledger/fabric/core/peer/pgwire/parser"
	"fmt"
	"github.com/hyperledger/fabric/core/ledger"
)

type Executor struct {
	queryExecutor ledger.QueryExecutor
}

func NewExecutor() *Executor {
	return &Executor{}
}

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) ExecuteStatements(session *Session, stmts string) parser.StatementResults {
	fmt.Println(">>>>>>>>>>>>>>", stmts)
	res := parser.StatementResults{}
	stmt, err := parser.Parse(stmts)
	if err != nil {
		return res
	}
	if stmt == nil {
		res.Empty = true
		return res
	}

	return stmt.Execute(e.queryExecutor)
}

func (e *Executor) Prepare(
query string, session *Session, pinfo parser.PlaceholderTypes,
) (*PreparedStatement, error) {
	return nil, nil
}

