package rbft

import (
	"encoding/pem"
	"fmt"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"io/ioutil"
)

type BftConfig struct {
	N uint64
	F uint64
}

type ChainConfig struct {
	Consensus *BftConfig
	Peers     map[string][]byte
}

func makeChainConfig(conf *config.TopLevel) *ChainConfig {
	bftCfg := BftConfig{
		PeerCommAddr: conf.General.BftCommAddress,
		PeerCommPort: conf.General.BftCommPort,
		DataDir:      conf.FileLedger.Location,
		Tls:          conf.General.TLS,
	}
	peers := make(map[string][]byte)
	for addr, cert := range conf.BFT.Peers {
		peers[addr], _ = parseCertPem(cert)
	}
	return &ChainConfig{
		Consensus: bftCfg,
		Peers:     peers,
	}
}

func parseCertPem(certFile string) ([]byte, error) {
	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	var b *pem.Block
	for {
		b, certBytes = pem.Decode(certBytes)
		if b == nil {
			break
		}
		if b.Type == "CERTIFICATE" {
			break
		}
	}
	if b == nil {
		return nil, fmt.Errorf("no certificate found")
	}

	return b.Bytes, nil
}
