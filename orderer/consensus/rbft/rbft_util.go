package rbft

import (
	"github.com/hyperledger/fabric/orderer/common/filter"
	"time"
	"sort"
	"encoding/base64"
	"fmt"
)

func (r *RBFT) intersectionQuorum() int {
	return (r.NumReplica + r.NumFailedReplica + 2) / 2
}

func (r *RBFT) allCorrectReplicasQuorum() int {
	return (r.NumFailedReplica - r.NumFailedReplica)
}

func (r *RBFT) inW(n uint64) bool {
	return n > r.LowWatermark && n <= r.LowWatermark + r.LogSize
}

func (r *RBFT) inWV(v, n uint64) bool {
	return r.view == v && r.inW(n)
}

func (r *RBFT) moveWatermarks(n uint64) {
	h := n / r.ChkptPeriod * r.ChkptPeriod

	for t := range r.checkpointStore {
		if t.Seq.SeqNo <= h {
			delete(r.checkpointStore, t)
		}
	}

	for n := range r.pset {
		if n < h {
			delete(r.pset, n)
		}
	}
	for idx := range r.qset {
		if idx.n < h {
			delete(r.qset, idx)
		}
	}
	for n := range r.checkpoints {
		if n < h {
			delete(r.checkpoints, n)
			r.persistDeleteCheckpoint(n)
		}
	}

	r.LowWatermark = h
	r.resubmitRequestBatches()
}

func (r *RBFT) prePrepared(digest string, v, n uint64) bool {
	_, inLog := r.reqBatchStore[digest]
	if digest != "" && !inLog {
		return false
	}

	if q, ok := r.qset[qidx{digest, n}]; ok && q.View == v {
		return true
	}

	cert := r.certStore[msgID{v, n}]
	if cert != nil {
		p := cert.prePrepare
		if p != nil && p.Seq.View == v && p.Seq.SeqNo == n && p.BatchDigest == digest {
			return true
		}
	}

	return false
}

func (r *RBFT) prepared(digest string, v, n uint64) bool {
	if !r.prePrepared(digest, v, n) {
		return false
	}

	if p, ok := r.pset[n]; ok && p.View == v && p.BatchDigest == digest {
		return true
	}

	quorum := 0
	cert := r.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	for _, p := range cert.prepare {
		if p.Seq.View == v && p.Seq.SeqNo == n && p.BatchDigest == digest {
			quorum++
		}
	}
	return quorum >= r.intersectionQuorum() - 1
}

func (r *RBFT) committed(digest string, v, n uint64) bool {
	if !r.prepared(digest, v, n) {
		return false
	}

	quorum := 0
	cert := r.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	for _, c := range cert.commit {
		if c.Seq.View == v && c.Seq.SeqNo == n {
			quorum++
		}
	}

	return quorum >= r.intersectionQuorum() - 1
}

func (r *RBFT) checkBatch(b *RequestBatch, checkData bool, needSigs bool) (uint64, error) {
	if checkData {

	}

	if needSigs {

	}

	return b.Seq, nil
}

func (r *RBFT) getCommittersFromBatch(reqBatch *RequestBatch) (bool, []filter.Committer) {
	batches := make([][]*Request, 0, 1)
	comms := [][]filter.Committer{}
	for _, req := range reqBatch.Batch {
		b, c, valid := r.backend.Validate(r.chainId, req)
		if !valid {
			return false, nil
		}
		batches = append(batches, b...)
		comms = append(comms, c...)
	}
	if len(batches) > 1 || len(batches) != len(comms) {
		return false, nil
	}
	if len(batches) == 0 {
		_, committer := r.backend.Cut(r.chainId)
		return true, committer
	} else {
		return true, comms[0]
	}
}

func (r *RBFT) getCert(v uint64, n uint64) (cert *msgCert) {
	idx := &msgID{v, n}
	cert, ok := r.certStore[idx]
	if ok {
		return
	}
	cert = &msgCert{}
	r.certStore[idx] = cert
	return
}

func (r *RBFT) calcPSet() map[uint64]*PQ {
	pset := make(map[uint64]*PQ)
	for n, p := range r.pset {
		pset[n] = p
	}

	for idx, cert := range r.certStore {
		if cert.prePrepare == nil {
			continue
		}

		digest := cert.digest
		if !r.prepared(digest, idx.v, idx.n) {
			continue
		}
		if p, ok := pset[idx.n]; ok && p.View > idx.v {
			continue
		}
		pset[idx.n] = &PQ{
			SeqNo: idx.n,
			View: idx.v,
			BatchDigest: digest,
		}
	}

	return pset
}

func (r *RBFT) calcQSet() map[qidx]*PQ {
	qset := make(map[qidx]*PQ)
	for n, q := range r.qset {
		qset[n] = q
	}
	for idx, cert := range r.certStore {
		if cert.prePrepare == nil {
			continue
		}
		digest := cert.digest
		if !r.prePrepared(digest, idx.v, idx.n) {
			continue
		}
		qi := qidx{digest, idx.n}
		if q, ok := qset[qi]; ok && q.View > idx.v {
			continue
		}

		qset[qi] = &PQ{
			SeqNo: idx.n,
			View: idx.v,
			BatchDigest: digest,
		}
	}
	return qset
}

func (r *RBFT) sign(s interface{}) {

}

func (r *RBFT) verify(s interface{}) error {
	return nil
}

func (r *RBFT) startTimerIfOutstandingRequests() {
	if r.skipInProgress || r.currentExec != nil {
		return
	}

	if len(r.outstandingReqBatches) > 0 {
		r.softStartTimer(r.requestTimeout, "outstanding request batches")
	} else if r.nullRequestTimeout > 0 {
		timeout := r.nullRequestTimeout
		if r.primary(r.view) != r.id {
			timeout += r.requestTimeout
		}
		r.nullRequestTimer.Reset(timeout)
	}
}

func (r *RBFT) softStartTimer(dur time.Duration, reason string) {
	r.timerActive = true
	r.newViewTimer.SoftReset(dur)
}

func (r *RBFT) startTimer(dur time.Duration, reason string) {
	r.timerActive = true
	r.newViewTimer.Reset(dur)
}

func (r *RBFT) stopTimer() {
	r.timerActive = false
	r.newViewTimer.Stop()
}

func (r *RBFT) updateViewChangeSeqNo() {
	if r.viewChangePeriod <= 0 {
		return
	}

	// FIXME: ensure the view change always occurs at a checkpoint boundary
	r.viewChangeSeqNo = r.seqNo + r.viewChangePeriod * r.ChkptPeriod - r.seqNo % r.ChkptPeriod
	logger.Debugf("replica %s update view change seq nu to %d", r.id, r.viewChangeSeqNo)
}

type sortableUint64Slice []uint64

func (a sortableUint64Slice) Len() int {
	return len(a)
}
func (a sortableUint64Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a sortableUint64Slice) Less(i, j int) bool {
	return a[i] < a[j]
}

func (r *RBFT)weakCheckpointSetOutOfRange(chkpt *Checkpoint) bool {
	highwatermark := r.LowWatermark + r.LogSize

	if chkpt.Seq.SeqNo < highwatermark {
		delete(r.hChkpts, chkpt.Id)
	} else {
		r.hChkpts[chkpt.Id] = chkpt.Seq.SeqNo

		if len(r.hChkpts) >= r.NumFailedReplica + 1 {
			chkptSeqNumArray := make([]uint64, len(r.hChkpts))
			idx := 0
			for replicaId, hchkpt := range r.hChkpts {
				chkptSeqNumArray[idx] = hchkpt
				idx++
				if hchkpt < highwatermark {
					delete(r.hChkpts, replicaId)
				}
			}

			sort.Sort(sortableUint64Slice(chkptSeqNumArray))
			if m := chkptSeqNumArray[len(chkptSeqNumArray)-(r.NumFailedReplica+1)]; m > highwatermark {
				r.reqBatchStore = make(map[string]*RequestBatch)
				r.persistDelAllRequestBatches()
				r.moveWatermarks(m)
				r.outstandingReqBatches = make(map[string]*RequestBatch)
				r.skipInProgress = true
				r.consumer.InvalidateState()
				r.stopTimer()
			}
		}
	}

	return false
}

func (r *RBFT)witnessCheckpointWeakCert(chkpt *Checkpoint) {
	checkpointMembers := make([]string, r.NumFailedReplica+1)
	for c := range r.checkpointStore {
		if c.Seq.SeqNo == chkpt.Seq.SeqNo && c.Id == chkpt.Id {
			checkpointMembers = append(checkpointMembers, c.Replica_Id)
		}
	}

	snapshotId, err := base64.StdEncoding.DecodeString(chkpt.Id)
	if err != nil {
		err := fmt.Errorf("replica %s received a weak checkpoint cert which can not be decodec(%s)", r.id, chkpt.Id)
		logger.Error(err.Error())
		return
	}

	target := &stateUpdateTarget{
		checkpointMessage: &checkpointMessage{
			id: snapshotId,
			seqNo: chkpt.Seq.SeqNo,
		},
		replicas: checkpointMembers,
	}
	r.updateHighStateTarget(target)

	if r.skipInProgress {
		r.retryStateTransfer(target)
	}
}

func (r *RBFT) assignSequenceNumbers(vset []*ViewChange, lowWatermark uint64)(msgList map[uint64]string) {
	msgList = make(map[uint64]string)
	maxN := lowWatermark + 1
    nLoop:
	for n := lowWatermark + 1; n < lowWatermark + r.LogSize; n++ {
		for _, m := range vset {
			for _, em := range m.Pset {
				quorum := 0
			    mpLoop:
				for _, mp := range vset {
					if mp.LowWatermark >= n {
						continue
					}
					for _, emp := range mp.Pset {
						if n == emp.SeqNo && !(emp.View < em.View || (emp.View == em.View && emp.BatchDigest == em.BatchDigest)) {
							continue mpLoop
						}
					}
					quorum++
				}
				if quorum < r.intersectionQuorum() {
					continue
				}

				quorum = 0
				for _, mp := range vset {
					for _, emp := range mp.Qset {
						if n == emp.SeqNo && emp.View >= em.View && emp.BatchDigest == em.BatchDigest {
							quorum++
						}
					}
				}
				if quorum < r.NumFailedReplica + 1 {
					continue
				}

				msgList[n] = em.BatchDigest
				maxN = n
				continue nLoop
			}
		}

		quorum := 0
	    nullLoop:
		for _, m := range vset {
			for _, em := range m.Pset {
				if em.SeqNo == n {
					continue nullLoop
				}
			}
			quorum++
		}
		if quorum >= r.intersectionQuorum() {
			msgList[n] = ""
			continue nLoop
		}
		return nil
	}

	for n, msg := range msgList {
		if n > maxN && msg == "" {
			delete(msgList, n)
		}
	}
	return
}
