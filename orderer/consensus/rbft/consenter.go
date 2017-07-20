package rbft

import (
	"github.com/hyperledger/fabric/common/flogging"
	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
)

const pkgLogID = "orderer/rbft"

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

func New(cfg localconfig.TopLevel) multichannel.Consenter {
	return &consenterImpl{
		config: makeBftStackConfig(cfg),
	}
}

type consenterImpl struct {
	config      *StackConfig
	backend     *Backend
	connManager *ConnManager
	persist     *Persist

	bftChains map[string]*RBFT
}

func (consenter *consenterImpl) HandleChain(support multichannel.ConsenterSupport, metadata *cb.Metadata) (multichannel.Chain, error) {
	meta := getChainBftMetadata(metadata, support.ChainID())
	return newChain(consenter, support, meta)
}
