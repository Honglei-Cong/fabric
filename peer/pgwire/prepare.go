package pgwire

import (
	"github.com/hyperledger/fabric/peer/pgwire/parser"
	"context"
)

// PreparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
type PreparedStatement struct {
	Statement   parser.Statement
	SQLTypes    parser.PlaceholderTypes
	Columns     parser.ResultColumns
	portalNames map[string]struct{}

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.
}

// PreparedStatements is a mapping of PreparedStatement names to their
// corresponding PreparedStatements.
type PreparedStatements struct {
	session *Session
	stmts   map[string]*PreparedStatement
}

func makePreparedStatements(s *Session) PreparedStatements {
	return PreparedStatements{
		session: s,
		stmts:   make(map[string]*PreparedStatement),
	}
}

// Get returns the PreparedStatement with the provided name.
func (ps PreparedStatements) Get(name string) (*PreparedStatement, bool) {
	stmt, ok := ps.stmts[name]
	return stmt, ok
}

// Exists returns whether a PreparedStatement with the provided name exists.
func (ps PreparedStatements) Exists(name string) bool {
	_, ok := ps.Get(name)
	return ok
}

// New creates a new PreparedStatement with the provided name and corresponding
// query string, using the given PlaceholderTypes hints to assist in inferring
// placeholder types.
func (ps PreparedStatements) NewFromString(
ctx context.Context, e *Executor, name, query string, placeholderHints parser.PlaceholderTypes,
) (*PreparedStatement, error) {
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return ps.New(e, name, stmts, len(query), placeholderHints)
}

// New creates a new PreparedStatement with the provided name and corresponding
// query statements, using the given PlaceholderTypes hints to assist in
// inferring placeholder types.
//
// ps.session.Ctx() is used as the logging context for the prepare operation.
func (ps PreparedStatements) New(
e *Executor,
name string,
stmts []parser.Statement,
queryLen int,
placeholderHints parser.PlaceholderTypes,
) (*PreparedStatement, error) {
	// Prepare the query. This completes the typing of placeholders.
	stmt, err := e.Prepare(stmts, ps.session, placeholderHints)
	if err != nil {
		return nil, err
	}

	ps.stmts[name] = stmt
	return stmt, nil
}

// Delete removes the PreparedStatement with the provided name from the PreparedStatements.
// The method returns whether a statement with that name was found and removed.
func (ps PreparedStatements) Delete(name string) bool {
	if stmt, ok := ps.Get(name); ok {
		if ps.session.PreparedPortals.portals != nil {
			for portalName := range stmt.portalNames {
				if _, ok := ps.session.PreparedPortals.Get(name); ok {
					delete(ps.session.PreparedPortals.portals, portalName)
				}
			}
		}
		delete(ps.stmts, name)
		return true
	}
	return false
}

// closeAll de-registers all statements and portals from the monitor.
func (ps PreparedStatements) closeAll(s *Session) {
}

// PreparedPortal is a PreparedStatement that has been bound with query arguments.
type PreparedPortal struct {
	Stmt  *PreparedStatement
	Qargs parser.QueryArguments

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.
}

// PreparedPortals is a mapping of PreparedPortal names to their corresponding
// PreparedPortals.
type PreparedPortals struct {
	session *Session
	portals map[string]*PreparedPortal
}

func makePreparedPortals(s *Session) PreparedPortals {
	return PreparedPortals{
		session: s,
		portals: make(map[string]*PreparedPortal),
	}
}

// Get returns the PreparedPortal with the provided name.
func (pp PreparedPortals) Get(name string) (*PreparedPortal, bool) {
	portal, ok := pp.portals[name]
	return portal, ok
}

// Exists returns whether a PreparedPortal with the provided name exists.
func (pp PreparedPortals) Exists(name string) bool {
	_, ok := pp.Get(name)
	return ok
}

// New creates a new PreparedPortal with the provided name and corresponding
// PreparedStatement, binding the statement using the given QueryArguments.
func (pp PreparedPortals) New(
name string, stmt *PreparedStatement, qargs parser.QueryArguments,
) (*PreparedPortal, error) {
	portal := &PreparedPortal{
		Stmt:  stmt,
		Qargs: qargs,
	}
	stmt.portalNames[name] = struct{}{}

	pp.portals[name] = portal
	return portal, nil
}

// Delete removes the PreparedPortal with the provided name from the PreparedPortals.
// The method returns whether a portal with that name was found and removed.
func (pp PreparedPortals) Delete(name string) bool {
	if portal, ok := pp.Get(name); ok {
		delete(portal.Stmt.portalNames, name)
		delete(pp.portals, name)
		return true
	}
	return false
}
