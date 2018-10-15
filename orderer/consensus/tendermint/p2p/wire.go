// Modified for Fabric
// Originally Copyright (c) 2013-2018 Tendermint
// https://github.com/tendermint/tendermint/blob/master/LICENSE

package p2p

import (
	"github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto"
)

var cdc = amino.NewCodec()

func init() {
	crypto.RegisterAmino(cdc)
}
