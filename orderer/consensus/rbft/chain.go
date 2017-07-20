package rbft

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

func newChain(consenter *consenterImpl, support multichannel.ConsenterSupport, meta *BftMetadata) (*chainImpl, error) {
	var err error
	if consenter.bftChains == nil {
		consenter.connManager, err = NewConnManager(consenter.config)
		if err != nil {
			return nil, fmt.Errorf("Failed to init conn-mgr: %s", err)
		}
		consenter.persist, err = NewPersist(consenter.config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to init persist store: %s", err)
		}
		consenter.backend, err = NewBackend(consenter.connManager, consenter.persist)
		if err != nil {
			return nil, fmt.Errorf("Failed to init bft backend: %s", err)
		}
		consenter.bftChains = make(map[string]*RBFT)
	}

	bftInst, err := consenter.backend.initBftChain(support.ChainID(), consenter, support, meta)
	if err != nil {
		return nil, fmt.Errorf("Failed to init bft chain: %s", err)
	}

	consenter.bftChains[support.ChainID()] = bftInst

	lastCutBlockNumber := getLastCutBlockNumber(support.Height())

	eventChan := make(chan Executable)

	errorChan := make(chan struct{})
	close(errorChan)

	return &chainImpl{
		backend:            consenter.backend,
		support:            support,
		lastCutBlockNumber: lastCutBlockNumber,

		queue: eventChan,
		bft:   bftInst,

		errorChan: errorChan,
		haltChan:  make(chan struct{}),
		startChan: make(chan struct{}),
	}, nil
}

type chainImpl struct {
	backend Backend
	support multichannel.ConsenterSupport

	lastSeq            int64
	lastCutBlockNumber uint64

	queue chan Executable
	bft   *RBFT

	errorChan chan struct{}
	haltChan  chan struct{}
	startChan chan struct{}
}

func (chain *chainImpl) Errored() <-chan struct{} {
	return chain.errorChan
}

func (chain *chainImpl) Start() {
	go startChainThread(chain)
}

func (chain *chainImpl) Halt() {

}

func (chain *chainImpl) Enqueue(env *cb.Envelope) bool {
	logger.Debugf("[channel: %s] Enqueueing envelope...", chain.support.ChainID())
	select {
	case <-chain.startChan: // The Start phase has completed
		select {
		case <-chain.haltChan: // The chain has been halted, stop here
			logger.Warningf("[channel: %s] Will not enqueue, consenter for this channel has been halted", chain.support.ChainID())
			return false
		default: // The post path
			marshaledEnv, err := utils.Marshal(env)
			if err != nil {
				return false
			}
			chain.enqueueRequest(marshaledEnv)
			return true
		}
	default: // Not ready yet
		logger.Warningf("[channel: %s] Will not enqueue, consenter for this channel hasn't started yet", chain.support.ChainID())
		return false
	}

}

func startChainThread(chain *chainImpl) {
	chain.processMessagesToBlocks()
}

func (chain *chainImpl) processMessagesToBlocks() ([]uint64, error) {
	defer func() {
		select {
		case <-chain.errorChan:
		default:
			close(chain.errorChan)
		}
	}()

	for {
		select {
		case e := <-chain.queue:
			e.Execute(chain)
		case <-chain.haltChan:
			return

		}
	}
}

func getLastCutBlockNumber(blockchainHeight uint64) uint64 {
	return blockchainHeight - 1
}

func getChainBftMetadata(metadataValue []byte, chainID string) *BftMetadata {
	if metadataValue != nil {
		metadata := &BftMetadata{}
		if err := proto.Unmarshal(metadataValue, metadata); err != nil {
			logger.Panicf("[channel: %s] failed to unmarshal orderer metadata in last block", chainID)
		}
		return metadata
	}
	return nil
}
