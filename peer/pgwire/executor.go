package pgwire

import (
	"fmt"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/peer/pgwire/parser"
)

type Executor struct {
	QueryExecutor ledger.QueryExecutor
	Namespace     string
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
		fmt.Println(">>>> err: ", err)
		return res
	}
	if stmt == nil {
		fmt.Println(">>>> no stmt")
		res.Empty = true
		return res
	}

	return stmt.Execute(e.QueryExecutor, e.Namespace)
}

func (e *Executor) Prepare(
	query string, session *Session, pinfo parser.PlaceholderTypes,
) (*PreparedStatement, error) {
	return nil, nil
}
