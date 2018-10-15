// Modified for Fabric
// Originally Copyright (c) 2013-2018 Tendermint
// https://github.com/tendermint/tendermint/blob/master/LICENSE

// +build go1.10

package conn

// Go1.10 has a proper net.Conn implementation that
// has the SetDeadline method implemented as per
//  https://github.com/golang/go/commit/e2dd8ca946be884bb877e074a21727f1a685a706
// lest we run into problems like
//  https://github.com/tendermint/tendermint/issues/851

import "net"

func NetPipe() (net.Conn, net.Conn) {
	return net.Pipe()
}
