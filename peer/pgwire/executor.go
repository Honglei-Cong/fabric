package pgwire

import (
	"fmt"
	"encoding/json"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/peer/chaincode"
	"github.com/hyperledger/fabric/peer/pgwire/parser"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/peer/pgwire/pgerror"
)

type Executor struct {
	//QueryExecutor ledger.QueryExecutor
	CmdFactory *chaincode.ChaincodeCmdFactory
	Channel   string
	Namespace string
}

func NewExecutor() *Executor {
	return &Executor{}
}

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) PrepareStatements(session *Session, stmts []parser.Statement) *parser.StatementResults {
	res := &parser.StatementResults{}
	if len(stmts) == 0 {
		logger.Warning(">>>> no stmt")
		res.Empty = true
		return res
	}

	stmt := stmts[0]
	q, err := stmt.BuildChaincodePrepareRequest()
	if err != nil {
		logger.Warning(">>>>> build chaincode prepare failed:", err)
		res.Empty = true
		return res
	}

	if q == nil {
		logger.Warning(">>>>> skipped chaincode prepare :", stmt.String())
		res.Empty = true
		return res
	}

	return e.executeChaincodeQuery(q)
}

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) ExecuteStatements(session *Session, stmts []parser.Statement) *parser.StatementResults {
	res := &parser.StatementResults{}
	if len(stmts) == 0 {
		logger.Warning(">>>> no stmt")
		res.Empty = true
		return res
	}

	stmt := stmts[0]
	q, err := stmt.BuildChaincodeQueryRequest()
	if err != nil {
		logger.Warning(">>>>> build chaincode query failed:", err)
		res.Empty = true
		return res
	}

	if q == nil {
		logger.Warning(">>>>> skipped chaincode query :", stmt.String())
		res.Empty = true
		return res
	}

	return e.executeChaincodeQuery(q)
}

func (e *Executor) executeChaincodeQuery(q *shim.SqlQuery) *parser.StatementResults {
	res := &parser.StatementResults{}
	ccQuery, err := json.Marshal(q)
	if err != nil {
		logger.Warning(">>>>> marshal chaincode query failed:", err)
		res.Empty = true
		return res
	}

	ccResult, err := e.queryChaincode(ccQuery)
	if err != nil {
		logger.Warning(">>>>> chaincode query failed:", err)
		res.Empty = true
		return res
	}

	logger.Debugf(">>>>> %s \n", string(ccResult))

	queryResult := &shim.QueryResult{}
	err = json.Unmarshal(ccResult, queryResult)
	if err != nil {
		logger.Warning(">>>>> marshal chaincode response failed:", err, string(ccResult))
		res.Empty = true
		return res
	}

	return parser.BuildStatementResult(queryResult)
}

func (e *Executor) queryChaincode(ccquery []byte) ([]byte, error) {
	spec := &pb.ChaincodeSpec{
		Type: pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]),
		ChaincodeId: &pb.ChaincodeID{
			Name: e.Namespace,
		},
		Input: &pb.ChaincodeInput{
			Args: [][]byte{[]byte("sqlQuery"), ccquery},
		},
	}

	proposalResp, err := chaincode.ChaincodeInvokeOrQuery(spec, e.Channel, false, e.CmdFactory.Signer, e.CmdFactory.EndorserClient, e.CmdFactory.BroadcastClient)
	if err != nil {
		return nil, fmt.Errorf("query failed: %s", err)
	}

	if proposalResp == nil {
		return nil, fmt.Errorf("error query by endorsing: %s", err)
	}

	if proposalResp.Response.Status != 0 && proposalResp.Response.Status != 200 {
		return nil, fmt.Errorf("Error bad proposal response %d", proposalResp.Response.Status)
	}

	return proposalResp.Response.Payload, nil
}

// Prepare returns the result types of the given statement. pinfo may
// contain partial type information for placeholders. Prepare will
// populate the missing types. The PreparedStatement is returned (or
// nil if there are no results). An error is returned if there is more than
// one statement in the statement list.
func (e *Executor) Prepare(
stmts []parser.Statement, session *Session, pinfo parser.PlaceholderTypes,
) (*PreparedStatement, error) {

	prepared := &PreparedStatement{
		SQLTypes:    pinfo,
		portalNames: make(map[string]struct{}),
	}
	switch len(stmts) {
	case 0:
		return prepared, nil
	case 1:
	// ignore
	default:
		return nil, fmt.Errorf(pgerror.CodeInvalidPreparedStatementDefinitionError,
			"prepared statement had %d statements, expected 1", len(stmts))
	}
	stmt := stmts[0]
	prepared.Statement = stmt

	execResult := e.PrepareStatements(session, []parser.Statement{stmt})
	if execResult != nil && !execResult.Empty {
		prepared.Columns = execResult.ResultList[0].Columns
	}

	return prepared, nil
}
