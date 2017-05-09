// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Ben Darnell

package pgwire

import (
	"io"
	"net"
	"time"

	"golang.org/x/net/context"

	"errors"
	"fmt"

	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/peer/pgwire/envutil"
	"github.com/hyperledger/fabric/core/peer/pgwire/parser"
	"github.com/hyperledger/fabric/core/peer/pgwire/pgerror"
	"github.com/hyperledger/fabric/core/peer/pgwire/syncutil"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("pgwire")

const (
	// ErrSSLRequired is returned when a client attempts to connect to a
	// secure server in cleartext.
	ErrSSLRequired = "cleartext connections are not permitted"

	// ErrDraining is returned when a client attempts to connect to a server
	// which is not accepting client connections.
	ErrDraining = "server is not accepting clients"

	ErrFailedGetDatabaseName = "Failed to get database name"

	ErrFailedFoundDatabase = "Failed to found database"
)

const (
	version30  = 196608
	versionSSL = 80877103
)

const (
	// drainMaxWait is the amount of time a draining server gives to sessions
	// with ongoing transactions to finish work before cancellation.
	drainMaxWait = 10 * time.Second
	// cancelMaxWait is the amount of time a draining server gives to sessions
	// to react to cancellation and return before a forceful shutdown.
	cancelMaxWait = 1 * time.Second
)

// connReservationBatchSize determines for how many connections memory
// is pre-reserved at once.
var connReservationBatchSize = 5

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

// cancelChanMap keeps track of channels that are closed after the associated
// cancellation function has been called and the cancellation has taken place.
type cancelChanMap map[chan struct{}]context.CancelFunc

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	cfg      *Config
	executor *Executor

	mu struct {
		syncutil.Mutex
		// connCancelMap entries represent connections started when the server
		// was not draining. Each value is a function that can be called to
		// cancel the associated connection. The corresponding key is a channel
		// that is closed when the connection is done.
		connCancelMap cancelChanMap
		draining      bool
	}
}

// noteworthySQLMemoryUsageBytes is the minimum size tracked by the
// client SQL pool before the pool start explicitly logging overall
// usage growth in the log.
var noteworthySQLMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SQL_MEMORY_USAGE", 100*1024*1024)

// noteworthyConnMemoryUsageBytes is the minimum size tracked by the
// connection monitor before the monitor start explicitly logging overall
// usage growth in the log.
var noteworthyConnMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_CONN_MEMORY_USAGE", 2*1024*1024)

// MakeServer creates a Server.
func MakeServer(cfg *Config, executor *Executor) *Server {
	server := &Server{
		cfg:      cfg,
		executor: executor,
	}

	server.mu.Lock()
	server.mu.connCancelMap = make(cancelChanMap)
	server.mu.Unlock()

	return server
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	var buf readBuffer
	_, err := buf.readUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.getUint32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL
}

// IsDraining returns true if the server is not currently accepting
// connections.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// SetDraining (when called with 'true') prevents new connections from being
// served and waits a reasonable amount of time for open connections to
// terminate before canceling them.
// An error will be returned when connections that have been cancelled have not
// responded to this cancellation and closed themselves in time. The server
// will remain in draining state, though open connections may continue to
// exist.
// When called with 'false', switches back to the normal mode of operation in
// which connections are accepted.
// The RFC on drain modes has more information regarding the specifics of
// what will happen to connections in different states:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/drain_modes.md
func (s *Server) SetDraining(drain bool) error {
	return s.setDrainingImpl(drain, drainMaxWait, cancelMaxWait)
}

func (s *Server) setDrainingImpl(
	drain bool, drainWait time.Duration, cancelWait time.Duration,
) error {
	// This anonymous function returns a copy of s.mu.connCancelMap if there are
	// any active connections to cancel. We will only attempt to cancel
	// connections that were active at the moment the draining switch happened.
	// It is enough to do this because:
	// 1) If no new connections are added to the original map all connections
	// will be cancelled.
	// 2) If new connections are added to the original map, it follows that they
	// were added when s.mu.draining = false, thus not requiring cancellation.
	// These connections are not our responsibility and will be handled when the
	// server starts draining again.
	connCancelMap := func() cancelChanMap {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.draining == drain {
			return nil
		}
		s.mu.draining = drain
		if !drain {
			return nil
		}

		connCancelMap := make(cancelChanMap)
		for done, cancel := range s.mu.connCancelMap {
			connCancelMap[done] = cancel
		}
		return connCancelMap
	}()
	if len(connCancelMap) == 0 {
		return nil
	}

	// Spin off a goroutine that waits for all connections to signal that they
	// are done and reports it on allConnsDone. The main goroutine signals this
	// goroutine to stop work through quitWaitingForConns.
	allConnsDone := make(chan struct{})
	quitWaitingForConns := make(chan struct{})
	defer close(quitWaitingForConns)
	go func() {
		defer close(allConnsDone)
		for done := range connCancelMap {
			select {
			case <-done:
			case <-quitWaitingForConns:
				return
			}
		}
	}()

	// Wait for all connections to finish up to drainWait.
	select {
	case <-time.After(drainWait):
	case <-allConnsDone:
	}

	// Cancel the contexts of all sessions if the server is still in draining
	// mode.
	if stop := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mu.draining {
			return true
		}
		for _, cancel := range connCancelMap {
			// There is a possibility that different calls to SetDraining have
			// overlapping connCancelMaps, but context.CancelFunc calls are
			// idempotent.
			cancel()
		}
		return false
	}(); stop {
		return nil
	}

	select {
	case <-time.After(cancelWait):
		return fmt.Errorf("some sessions did not respond to cancellation within %s", cancelWait)
	case <-allConnsDone:
	}
	return nil
}

// ServeConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	s.mu.Lock()
	draining := s.mu.draining
	if !draining {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		done := make(chan struct{})
		s.mu.connCancelMap[done] = cancel
		defer func() {
			cancel()
			close(done)
			s.mu.Lock()
			delete(s.mu.connCancelMap, done)
			s.mu.Unlock()
		}()
	}
	s.mu.Unlock()

	var buf readBuffer
	_, err := buf.readUntypedMsg(conn)
	if err != nil {
		return err
	}
	version, err := buf.getUint32()
	if err != nil {
		return err
	}
	errSSLRequired := false
	//if version == versionSSL {
	//	if len(buf.msg) > 0 {
	//		return fmt.Errorf("unexpected data after SSLRequest: %q", buf.msg)
	//	}
	//
	//	if s.cfg.Insecure {
	//		if _, err := conn.Write(sslUnsupported); err != nil {
	//			return err
	//		}
	//	} else {
	//		if _, err := conn.Write(sslSupported); err != nil {
	//			return err
	//		}
	//		tlsConfig, err := s.cfg.GetServerTLSConfig()
	//		if err != nil {
	//			return err
	//		}
	//		conn = tls.Server(conn, tlsConfig)
	//	}
	//
	//	n, err := buf.readUntypedMsg(conn)
	//	if err != nil {
	//		return err
	//	}
	//	version, err = buf.getUint32()
	//	if err != nil {
	//		return err
	//	}
	//} else if !s.cfg.Insecure {
	//	errSSLRequired = true
	//}

	if version == version30 {
		// We make a connection before anything. If there is an error
		// parsing the connection arguments, the connection will only be
		// used to send a report of that error.
		v3conn := makeV3Conn(conn, s.executor)
		defer v3conn.finish(ctx)

		if v3conn.sessionArgs, err = parseOptions(buf.msg); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		if errSSLRequired {
			return v3conn.sendInternalError(ErrSSLRequired)
		}
		if draining {
			return v3conn.sendError(newAdminShutdownErr(errors.New(ErrDraining)))
		}

		v3conn.sessionArgs.User = parser.Name(v3conn.sessionArgs.User).Normalize()
		if err := v3conn.handleAuthentication(ctx, s.cfg.Insecure); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		if s.executor.QueryExecutor == nil {
			if len(v3conn.sessionArgs.Channel) == 0 || len(v3conn.sessionArgs.Namespace) == 0 {
				return v3conn.sendError(errors.New(ErrFailedGetDatabaseName))
			}

			lgr := peer.GetLedger(v3conn.sessionArgs.Channel)
			if lgr == nil {
				return v3conn.sendError(errors.New(ErrFailedFoundDatabase))
			}
			s.executor.Namespace = v3conn.sessionArgs.Namespace
			s.executor.QueryExecutor, err = lgr.NewQueryExecutor()
			if err != nil {
				return v3conn.sendInternalError(err.Error())
			}
		}

		err := v3conn.serve(ctx, s.IsDraining)
		if err != nil {
			logger.Warningf("v3conn server: %s", err)
		}
		// If the error that closed the connection is related to an
		// administrative shutdown, relay that information to the client.
		if code, ok := pgerror.PGCode(err); ok && code == pgerror.CodeAdminShutdownError {
			return v3conn.sendError(err)
		}
		return err
	}

	return fmt.Errorf("unknown protocol version %d", version)
}
