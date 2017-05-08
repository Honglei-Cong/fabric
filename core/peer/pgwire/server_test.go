package pgwire

import (
	"net"
	"testing"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"golang.org/x/net/context"
)

type UnresolvedAddr struct {
	NetworkField string `protobuf:"bytes,1,opt,name=network_field,json=networkField" json:"network_field"`
	AddressField string `protobuf:"bytes,2,opt,name=address_field,json=addressField" json:"address_field"`
}

// Network returns the address's network name.
func (a *UnresolvedAddr) Network() string {
	return a.NetworkField
}

// IsEmpty returns true if the address has no network or address specified.
func (a UnresolvedAddr) IsEmpty() bool {
	return a == (UnresolvedAddr{})
}

// String returns the address's string form.
func (a *UnresolvedAddr) String() string {
	return a.AddressField
}

func NewUnresolvedAddr(network, addr string) *UnresolvedAddr {
	return &UnresolvedAddr{
		NetworkField: network,
		AddressField: addr,
	}
}

var TestAddr = NewUnresolvedAddr("tcp", "127.0.0.1:6543")

func TestConnection(t *testing.T) {

	ln, err := net.Listen(TestAddr.Network(), TestAddr.String())
	defer func() {
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	expectedRead := []byte("expectedRead")
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- func() error {
			c, err := ln.Accept()
			if err != nil {
				return err
			}
			defer c.Close()

			readTimeoutConn := newReadTimeoutConn(c, ctx.Err)
			readBytes := make([]byte, len(expectedRead))
			readTimeoutConn.Read(readBytes)
			_, err = readTimeoutConn.Read(make([]byte, 1))
			return err
		}()
	}()

	c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Write(expectedRead); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("goroutine returned: %v", err)
	default:
	}

	cancel()
	if err := <-errChan; err != context.Canceled {
		t.Fatalf("unexpected err: %v", err)
	}
}

type testQueryIterator struct {
	MaxCount   int
	Counter    int
	TestResult *queryresult.KV
}

func (ite *testQueryIterator) Next() (commonledger.QueryResult, error) {
	if ite.Counter < ite.MaxCount {
		ite.Counter++
		return ite.TestResult, nil
	}
	return nil, nil
}

func (ite *testQueryIterator) Close() {
}

type testQueryExecutor struct {
}

func (q *testQueryExecutor) GetState(namespace string, key string) ([]byte, error) {
	return nil, nil
}

func (q *testQueryExecutor) GetStateMultipleKeys(namespace string, keys []string) ([][]byte, error) {
	return nil, nil
}

func (q *testQueryExecutor) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (commonledger.ResultsIterator, error) {
	return &testQueryIterator{
		MaxCount: 10,
		TestResult: &queryresult.KV{
			Key:   "test-key",
			Value: []byte("test-value"),
		},
	}, nil
}

func (q *testQueryExecutor) ExecuteQuery(namespace, query string) (commonledger.ResultsIterator, error) {
	return nil, nil
}

func (q *testQueryExecutor) Done() {
}

func TestServer(t *testing.T) {
	ln, err := net.Listen(TestAddr.Network(), TestAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	exec := NewExecutor()
	exec.queryExecutor = &testQueryExecutor{}
	server := MakeServer(&Config{}, exec)
	err = server.ServeConn(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
}
