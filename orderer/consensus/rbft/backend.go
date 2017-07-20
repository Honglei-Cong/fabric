package rbft

import (
	"sync"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"fmt"
	"time"
	"github.com/pingcap/tidb/_vendor/src/google.golang.org/grpc"
	"github.com/pingcap/tidb/_vendor/src/golang.org/x/net/context"
	"io"
	"google.golang.org/grpc/transport"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/orderer/common/filter"
	"github.com/golang/protobuf/proto"
)

type Executable interface {
	Execute(*Backend)
}

type Backend struct {
	conn        *ConnManager
	lock        sync.Mutex
	peers       map[string]chan <- *MultiChainMessage
	persist     *Persist

	self        *PeerInfo
	peerInfo    map[string]*PeerInfo

	chains      map[string]*chainImpl
	lastBatches map[string]*RequestBatch
	supports    map[string]multichannel.ConsenterSupport
}

type StackConfig struct {
	ListenAddr string
	ListenPort uint16
	Tls config.TLS
	DataDir string
}

func makeBftStackConfig(conf *config.TopLevel) *StackConfig {
	return StackConfig{
		ListenAddr: conf.General.BftCommAddress,
		ListenPort: conf.General.BftCommPort,
		Tls: conf.General.TLS,
		DataDir: conf.FileLedger.Location,
	}
}

func NewBackend(conn *ConnManager, persist *Persist)(*Backend, error) {
	c := &Backend{
		conn: conn,
		peers: make(map[string]chan *MultiChainMessage),
		peerInfo: make(map[string]*PeerInfo),
		supports: make(map[string]*multichannel.ConsenterSupport),
		chains: make(map[string]*chainImpl),
		lastBatches: make(map[string]*RequestBatch),
	}

	c.self = &conn.Self
	c.persist = persist

	RegisterConsensusServer(conn.Server, c)
	return c, nil
}

func (b *Backend) Consensus(_ *Handshake, srv Consensus_ConsensusServer) error {
	pi := getPeerInfo(srv)
	peer, ok := b.peerInfo[pi.Fingerprint()]

	if !ok || !peer.Cert().Equal(pi.Cert()) {
		logger.Infof("rejecting connection from unknown replica %s", pi)
		return fmt.Errorf("unknown peer certificate")
	}
	logger.Infof("connection from replica %d (%s)", peer.addr, pi)

	ch := make(chan *MultiChainMessage)
	b.lock.Lock()
	if oldch, ok := b.peers[peer.addr]; ok {
		logger.Debugf("replacing connection from replica %d", peer.addr)
		close(oldch)
	}
	b.peers[peer.addr] = ch
	b.lock.Unlock()

	for chainID, _ := range b.supports {
		if chain, ok := b.chains[chainID]; ok {
			chain.enqueueConnection(peer.addr)
		} else {
			logger.Warningf("failed to get chain %s", chainID)
		}
	}

	var err error
	for msg := range ch {
		err = srv.Send(msg)
		if err != nil {
			b.lock.Lock()
			delete(b.peers, peer.addr)
			b.lock.Unlock()

			logger.Infof("lost connection from replica %d (%s): %s", peer.addr, pi, err)
		}
	}

	return err
}

func (b *Backend) GetMyId() string {
	return b.self.addr
}

func (b *Backend) initBftChain(chainID string, consenter *consenterImpl, support multichannel.ConsenterSupport, meta *BftMetadata) (*RBFT, error) {
	b.supports[chainID] = support

	var peersInfo []*PeerInfo
	for addr, cert := range support.SharedConfig().BftPeers() {
		if _, found := b.peerInfo[addr]; !found {
			pi, err := NewPeerInfo(addr, cert)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse peer info: %s", err)
			}
			peersInfo = append(peersInfo, pi)
		}
	}
	for _, peer := range peersInfo {
		go b.connectWorker(peer)
	}
	return NewRBFT(b.GetMyId(), chainID, meta, b)
}

func (b *Backend) connectWorker(peer *PeerInfo) {
	timeout := 1 * time.Second
	delay := time.After(0)

	for {
		<- delay
		delay = time.After(timeout)

		conn, err := b.conn.DialPeer(peer, grpc.WithBlock(), grpc.WithTimeout(timeout))
		if err != nil {
			logger.Warningf("failed to connect to replica %s: %s", peer.addr, err)
			continue
		}

		ctx := context.TODO()
		client := NewConsensusClient(conn)
		consensus, err := client.Consensus(ctx, &Handshake{})
		if err != nil {
			logger.Warningf("failed to create consensus stream with replica %s: %s", peer.addr, err)
			continue
		}

		logger.Infof("connection with replica %s: OK", peer.addr)
		for {
			msg, err := consensus.Recv()
			if err == io.EOF || err == transport.ErrConnClosing {
				break
			}
			if err != nil {
				logger.Warningf("consensus stream with replica %s broke: %s", peer.addr, err)
				break
			}

			if chain, ok := b.chains[msg.ChainID]; ok {
				chain.enqueueForReceive(msg.Msg, peer.addr)
			} else {
				logger.Warningf("consensus stream failed to get chain %s", msg.ChainID)
			}
		}
	}
}

func (b *Backend) Validate(chainID string, req *Request) ([][]*Request, [][]filter.Committer, bool) {
	return nil, nil, false
}

func (b *Backend) Cut(chainID string)([]*Request, []filter.Committer) {
	envbatch := b.supports[chainID].BlockCutter().Cut()
	return toRequestBatch(envbatch), committers
}

func toRequestBatch(envelopes []*cb.Envelope) []*Request {
	rqs := make([]*Request, 0, len(envelopes))
	for _, e := range envelopes {
		requestbytes, err := proto.Marshal(e)
		if err != nil {
			logger.Panicf("Cannot marshal envelope: %s", err)
		}
		rq := &Request{Payload: requestbytes}
		rqs = append(rqs, rq)
	}
	return rqs
}
