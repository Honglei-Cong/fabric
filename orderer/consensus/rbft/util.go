package rbft

import (
	"encoding/base64"
	"github.com/golang/protobuf/proto"
	"golang.org/x/crypto/sha3"
)

// ComputeCryptoHash should be used in openchain code so that we can change the actual algo used for crypto-Hash at one place
func ComputeCryptoHash(data []byte) []byte {
	hash := make([]byte, 64)
	sha3.ShakeSum256(hash, data)
	return hash
}

func Hash(msg interface{}) string {
	var raw []byte
	switch converted := msg.(type) {
	case *Request:
		raw, _ = proto.Marshal(converted)
	case *RequestBatch:
		raw, _ = proto.Marshal(converted)
	default:
		logger.Error("Asked to Hash non-supported message type, ignoring")
		return ""
	}
	return base64.StdEncoding.EncodeToString(ComputeCryptoHash(raw))
}


