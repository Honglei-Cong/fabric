package rbft

import (
	"fmt"
	"github.com/hyperledger/fabric/orderer/common/filter"
	"time"
)

type Receiver interface {
	Receive(msg *Message, src uint64)
	Request(req []byte)
	Connection(replica string)
	GetChainId() string
}

type Canceller interface {
	Cancel()
}

type reqInfo struct {
	subject        PQ
	timeout        Canceller
	preprep        *PrePrepare
	prep           map[uint64]*PQ
	commit         map[uint64]*PQ
	checkpoint     map[uint64]*Checkpoint
	prepared       bool
	committed      bool
	checkpointDone bool
	committers     []filter.Committer
}

type replicaInfo struct {
	backLog          []*Message
	hello            *Hello
	signedViewChange *Signed
	viewChange       *ViewChange
}

type dummyCanceller struct{}

func (d dummyCanceller) Cancel() {}

type qidx struct {
	d string
	n uint64
}

type vcidx struct {
	v  uint64
	id uint64
}

type msgID struct {
	v uint64
	n uint64
}

type msgCert struct {
	digest      string
	prePrepare  *PrePrepare
	sentPrepare bool
	prepare     []*Prepare
	sentCommit  bool
	commit      []*Commit
}

type RBFT struct {
	NumReplica       uint64
	NumFailedReplica uint64
	ChkptPeriod      uint64
	LogSize          uint64
	LowWatermark     uint64
	id               string
	chainId          string
	backend          *Backend

	activeView  bool
	lastExec    uint64
	seqNo       uint64
	view        uint64
	checkpoints map[uint64]string
	pset        map[uint64]*PQ
	qset        map[qidx]*PQ

	currentExec        *uint64
	timerActive        bool          // is the timer running?
	vcResendTimer      *Timer        // timer triggering resend of a view change
	newViewTimer       *Timer        // timeout triggering a view change
	nullRequestTimer   *Timer        // timeout triggering a null request
	nullRequestTimeout time.Duration // duration for this timeout
	requestTimeout     time.Duration // progress timeout for requests
	vcResendTimeout    time.Duration // timeout before resending view change
	newViewTimeout     time.Duration // progress timeout for new views
	lastNewViewTimeout time.Duration

	skipInProgress   bool
	stateTransfering bool
	highStateTarget *stateUpdateTarget
	hChkpts map[string]uint64 // highest checkpoint seq# observed for each replica

	batches    [][]*Request
	batchTimer Canceller

	outstandingReqBatches map[string]*RequestBatch
	missingReqBatches     map[string]bool

	lastNewViewSent   *NewView
	viewChangeTimeout time.Duration
	viewChangeTimer   Canceller

	viewChangePeriod uint64
	viewChangeSeqNo  uint64

	replicaState []replicaInfo
	pending      map[string]*Request
	validated    map[string]bool

	primaryCommitters [][]filter.Committer

	reqBatchStore   map[string]*RequestBatch
	certStore       map[msgID]*msgCert
	checkpointStore map[Checkpoint]bool
	viewChangeStore map[vcidx]*ViewChange
	newViewStore    map[uint64]*NewView
}

func NewRBFT(id string, chainID string, config *BftMetadata, backend *Backend) (*RBFT, error) {
	if config.N < config.F*3+1 {
		return nil, fmt.Errorf("invalid combination of (%d, %d)", config.N, config.F)
	}

	r := &RBFT{
		NumReplica:       config.N,
		NumFailedReplica: config.F,
		ChkptPeriod:      config.K,
		LogSize:          config.L,
		LowWatermark:     config.LowWatermark,
		id:               id,
		chainId:          chainID,
		backend:          backend,

		lastExec: config.Seq,

		viewChangeTimer:   dummyCanceller{},
		replicaState:      make([]replicaInfo, config.N),
		pending:           make(map[string]*Request),
		validated:         make(map[string]bool),
		batches:           make([][]*Request, 0, 3),
		primaryCommitters: make([][]filter.Committer, 0),
	}

	r.view = 0

	return r, nil
}

func (r *RBFT) Receive(m *Message, src string) {
	var err error

	if h := m.GetHello(); h != nil {
		err = r.handleHello(h, src)
	} else if req := m.GetRequestBatch(); req != nil {
		err = r.handleRequest(req)
	} else if vs := m.GetViewChange(); vs != nil {
		err = r.handleViewChange(vs)
	} else if nv := m.GetNewView(); nv != nil {
		err = r.handleNewView(nv)
	} else {
		if r.testBacklogMessage(m, src) {
			logger.Debugf("replica %d: message for future seq, storing for later", r.id)
			r.recordBacklogMsg(m, src)
			return
		}

		if pp := m.GetPrePrepare(); pp != nil {
			r.handlePreprepare(pp, src)
			return
		} else if p := m.GetPrepare(); p != nil {
			r.handlePrepare(p)
			return
		} else if c := m.GetCommit(); c != nil {
			r.handleCommit(c)
			return
		} else if chk := m.GetCheckpoint(); chk != nil {
			r.handleCheckpoint(chk)
			return
		} else {
			logger.Warningf("replica %d: received invalid message from %d", r.id, src)
		}
	}

	if err != nil {
		logger.Warningf("bft recv handle failure: %s", err)
	}
}

func (r *RBFT) Timer(t *Timer) {

}

func (r *RBFT) Request(req []byte) {
	// submit  to leader
}

func (r *RBFT) Connection(replica string) {
	// new peer connected
}

func (r *RBFT) handleHello(h *Hello, src string) error {
	seq, err := r.checkBatch(h.Batch, false, true)
	if err != nil {
		logger.Warningf("replica %s: invalid hello batch from %s: %s", r.id, src, err)
		return
	}

	if r.backend.lastBatches[r.chainId].Seq < seq {
		blockOK, committers := r.getCommittersFromBatch(h.Batch)
		if blockOK {
			r.deliverBatch(h.Batch, committers)
		} else {
			logger.Debugf("replica %s: get hello from %s with an error block", r.id, src)
		}
	}

	r.handleNewView(h.NewView)
	r.replicaState[src].hello = h
	r.discardBacklog(src)
	r.processBacklog()
	return nil
}

func (r *RBFT) resubmitRequestBatches() {

	if r.primary(r.view) != r.id {
		return
	}

	var submissionOrder []*RequestBatch
outer:
	for d, reqBatch := range r.outstandingReqBatches {
		for _, cert := range r.certStore {
			if cert.digest == d {
				continue outer
			}
		}
		submissionOrder = append(submissionOrder, reqBatch)
	}

	if len(submissionOrder) == 0 {
		return
	}

	for _, reqBatch := range submissionOrder {
		r.handleRequest(reqBatch)
	}
}
