package parser

import (
	"github.com/hyperledger/fabric/core/chaincode/shim"
)


// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  Type

	// If set, this is an implicit column; used internally.
	hidden bool

	// If set, a value won't be produced for this column; used internally.
	omitted bool
}

// ResultColumns is the type used throughout the sql module to
// describe the column types of a table.
type ResultColumns []ResultColumn

// Result corresponds to the execution of a single SQL statement.
type Result struct {
	Err error
	// The type of statement that the result is for.
	Type StatementType
	// The tag of the statement that the result is for.
	PGTag string
	// RowsAffected will be populated if the statement type is "RowsAffected".
	RowsAffected int
	// Columns will be populated if the statement type is "Rows". It will contain
	// the names and types of the columns returned in the result set in the order
	// specified in the SQL statement. The number of columns will equal the number
	// of values in each Row.
	Columns ResultColumns
	// Rows will be populated if the statement type is "Rows". It will contain
	// the result set of the result.
	// TODO(nvanbenschoten): Can this be streamed from the planNode?
	Rows *RowContainer
}

// ResultList represents a list of results for a list of SQL statements.
// There is one result object per SQL statement in the request.
type ResultList []Result

// StatementResults represents a list of results from running a batch of
// SQL statements, plus some meta info about the batch.
type StatementResults struct {
	ResultList
	// Indicates that after parsing, the request contained 0 non-empty statements.
	Empty bool
}

// Close ensures that the resources claimed by the results are released.
func (s *StatementResults) Close() {
	s.ResultList.Close()
}

// Close ensures that the resources claimed by the results are released.
func (rl ResultList) Close() {
	for _, r := range rl {
		r.Close()
	}
}

// Close ensures that the resources claimed by the result are released.
func (r *Result) Close() {
	//// The Rows pointer may be nil if the statement returned no rows or
	//// if an error occurred.
	if r.Rows != nil {
		r.Rows.Close()
	}
}

func BuildStatementResult(queryResult *shim.QueryResult) *StatementResults {
	res := &StatementResults{}
	Cols := []ResultColumn{}
	for _, col := range queryResult.Cols {
		Cols = append(Cols, ResultColumn{Name: col, Typ: TypeString})
	}

	logger.Debugf(">>>> cols: %v \n", Cols)

	rowC := NewRowContainer(Cols, 0)
	row := make([]Datum, len(Cols))

	for _, r := range queryResult.Rows {
		logger.Debugf(">>>> row: %v \n", r)
		for i, c := range Cols {
			v, avail := r[c.Name]
			if avail {
				row[i] = NewDString(v)
			} else {
				row[i] = NewDString("null")
			}
		}

		rowC.AddRow(row)
	}

	res.ResultList = append(res.ResultList, Result{
		PGTag:   queryResult.QueryOP,
		Type:    Rows,
		Columns: Cols,
		Rows:    rowC,
	})

	return res
}

