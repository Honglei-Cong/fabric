package pgwire

import (
	"net"
	"golang.org/x/net/context"
	"time"
)

// TxnStateEnum represents the state of a SQL txn.
type TxnStateEnum int

//go:generate stringer -type=TxnStateEnum
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled back.
	NoTxn TxnStateEnum = iota
	// A txn is in scope.
	Open
	// The txn has encountered a (non-retriable) error.
	// Statements will be rejected until a COMMIT/ROLLBACK is seen.
	Aborted
	// The txn has encountered a retriable error.
	// Statements will be rejected until a RESTART_TRANSACTION is seen.
	RestartWait
	// The KV txn has been committed successfully through a RELEASE.
	// Statements are rejected until a COMMIT is seen.
	CommitWait
)

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database string
	User     string
}

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user), txnState is intended to be stored as part of a user Session.
type txnState struct {
	State TxnStateEnum
}

type Session struct {
	// Info about the open transaction (if any).
	TxnState txnState

	PreparedStatements PreparedStatements
	PreparedPortals    PreparedPortals

	Location              *time.Location
}

func NewSession(ctx context.Context, args SessionArgs, e *Executor, remote net.Addr) *Session {
	return &Session{}
}

// Finish releases resources held by the Session.
func (s *Session) Finish(e *Executor) {
}

// CopyEnd ends the COPY mode. Any buffered data is discarded.
func (session *Session) CopyEnd() {
}

