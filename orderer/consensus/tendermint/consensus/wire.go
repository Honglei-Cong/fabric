// Modified for Fabric
// Originally Copyright (c) 2013-2018 Tendermint
// https://github.com/tendermint/tendermint/blob/master/LICENSE

package consensus

import (
	"github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto"
)

var cdc = amino.NewCodec()

func init() {
	RegisterConsensusMessages(cdc)
	RegisterWALMessages(cdc)
	crypto.RegisterAmino(cdc)
}
