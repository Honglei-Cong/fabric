// Modified for Fabric
// Originally Copyright (c) 2013-2018 Tendermint
// https://github.com/tendermint/tendermint/blob/master/LICENSE

// +build gofuzz

package consensus

import (
	"bytes"
	"io"
)

func Fuzz(data []byte) int {
	dec := NewWALDecoder(bytes.NewReader(data))
	for {
		msg, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			if msg != nil {
				panic("msg != nil on error")
			}
			return 0
		}
		var w bytes.Buffer
		enc := NewWALEncoder(&w)
		err = enc.Encode(msg)
		if err != nil {
			panic(err)
		}
	}
	return 1
}
