package rbft

type msgEvent struct {
	chainId string
	msg     *Message
	src     string
}

func (m *msgEvent) Execute(chain *chainImpl) {
	chain.bft.Receive(m.msg, m.src)
}

type requestEvent struct {
	chainId string
	req     []byte
}

func (r *requestEvent) Execute(chain *chainImpl) {
	chain.bft.Request(r.req)
}

type connectionEvent struct {
	chainID  string
	peerAddr string
}

func (c *connectionEvent) Execute(chain *chainImpl) {
	chain.bft.Connection(c.peerAddr)
}

type timerEvent struct {
	chainID string
	timer *Timer
}

func (t *timerEvent) Execute(chain *chainImpl) {
	chain.bft.Timer(t.timer)
}

func (b *chainImpl) enqueueRequest(req []byte) {
	go func() {
		b.queue <- &requestEvent{chainId: b.support.ChainID(), req: req}
	}()
}

func (b *chainImpl) enqueueForReceive(msg *Message, src string) {
	go func() {
		b.queue <- &msgEvent{chainId: b.support.ChainID(), msg: msg, src: src}
	}()
}

func (b *chainImpl) enqueueConnection(peerAddr string) {
	go func() {
		b.queue <- &connectionEvent{chainID: b.support.ChainID(), peerAddr: peerAddr}
	}()
}

func (b *chainImpl) enqueueTimer(timer *Timer) {
	go func() {
		b.queue <- &timerEvent{chainID: b.support.ChainID(), timer: timer}
	}()
}