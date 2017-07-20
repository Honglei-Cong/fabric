package rbft

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/pingcap/tidb/_vendor/src/google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/transport"
	"net"
)

type PeerInfo struct {
	addr string
	cert *x509.Certificate
	cp   *x509.CertPool
}

type ConnManager struct {
	Server    *grpc.Server
	Listener  net.Listener
	Self      PeerInfo
	tlsConfig *tls.Config
	Cert      *tls.Certificate
}

func NewConnManager(config *StackConfig) (*ConnManager, error) {
	c := &ConnManager{}
	certFile := config.Tls.Certificate
	keyFile := config.Tls.PrivateKey
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}

	c.Cert = &cert
	c.Self, _ = NewPeerInfo("self", cert.Certificate[0])

	c.tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequestClientCert,
		InsecureSkipVerify: true,
	}

	addr := config.ListenAddr + ":" + config.ListenPort
	c.Listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	serverTls := c.tlsConfig
	serverTls.ServerName = addr
	c.Server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTls)))
	go c.Server.Serve(c.Listener)
	return c, nil
}

func (c *ConnManager) DialPeer(peer PeerInfo, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return dialPeer(&c.tlsConfig.Certificates[0], peer, opts...)
}

type patchedAuthenticator struct {
	credentials.TransportCredentials
	pinnedCert    *x509.Certificate
	tunneledError error
}

func dialPeer(cert *tls.Certificate, peer PeerInfo, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	clientTLS := &tls.Config{InsecureSkipVerify: true}
	if cert != nil {
		clientTLS.Certificates = []tls.Certificate{*cert}
	}

	creds := credentials.NewTLS(clientTLS)
	patchedCreds := &patchedAuthenticator{
		TransportCredentials: creds,
		pinnedCert:           peer.cert,
	}
	opts = append(opts, grpc.WithTransportCredentials(patchedCreds))
	conn, err := grpc.Dial(peer.addr, opts...)
	if err != nil {
		if patchedCreds.tunneledError != nil {
			err = patchedCreds.tunneledError
		}
		return nil, err
	}

	return conn, nil
}

func NewPeerInfo(addr string, cert []byte) (_ PeerInfo, err error) {
	var p PeerInfo

	p.addr = addr
	p.cert, err = x509.ParseCertificate(cert)
	if err != nil {
		return
	}
	p.cp = x509.NewCertPool()
	p.cp.AddCert(p.cert)
	return p, nil
}

func (pi *PeerInfo) Fingerprint() string {
	return fmt.Sprintf("%x", sha256.Sum256(pi.cert.Raw))
}

func (pi *PeerInfo) Cert() *x509.Certificate {
	cert := *pi.cert
	return &cert
}

func (pi PeerInfo) String() string {
	return fmt.Sprintf("%.6s [%s]", pi.Fingerprint(), pi.addr)
}

func getPeerInfo(s grpc.Stream) PeerInfo {
	var pi PeerInfo

	ctx := s.Context()
	trs, ok := transport.StreamFromContext(ctx)
	if ok {
		pi.addr = trs.ServerTransport().RemoteAddr().String()
	}

	p, _ := peer.FromContext(ctx)
	switch creds := p.AuthInfo.(type) {
	case credentials.TLSInfo:
		state := creds.State
		if len(state.PeerCertificates) > 0 {
			pi.cert = state.PeerCertificates[0]
		}
	}

	return pi
}
