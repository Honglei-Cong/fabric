package rbft

import (
	"fmt"
	"encoding/base64"
)

func (r *RBFT) handleRequest(req *RequestBatch) error {
	digest := Hash(req)

	r.reqBatchStore[digest] = req
	r.outstandingReqBatches[digest] = req
	r.persistRequestBatch(digest)

	if r.activeView {
		r.softStartTimer(r.requestTimeout, fmt.Sprintf("new request batch %s", digest))
	}
	if r.primary(r.view) == r.id && r.activeView {
		err := r.sendPrePrepare(req, digest)
		if err != nil {
			logger.Warningf("replica %s failed to send pp for req batch %s", r.id, digest)
		}
	} else {
		logger.Debugf("replica %s is backup, not sending pp for req batch %s", r.id, digest)
	}

	return nil
}

func (r *RBFT) sendPrePrepare(req *RequestBatch, digest string) error {
	n := r.seqNo + 1
	for _, cert := range r.certStore {
		if p := cert.prePrepare; p != nil {
			if p.Seq.View == r.view && p.Seq.SeqNo != n && p.BatchDigest == digest && digest != "" {
				return fmt.Errorf("other pp found with same digest for req batch %s, (%d vs %d)", digest, p.Seq.SeqNo, n)
			}
		}
	}
	if !r.inWV(r.view, n) || n > r.LowWatermark + r.LogSize / 2 {
		return fmt.Errorf("primary %s not sending pp for batch %s, out-of-seq number", r.id, digest)
	}
	if n > r.viewChangeSeqNo {
		return fmt.Errorf("primary %s about to switch to next primary, not sending pp with seq %d", r.id, n)
	}

	logger.Debugf("primary %s broadcasting pp for view=%d/seq=%d and digest %s", r.id, r.view, n, digest)
	r.seqNo = n
	pp := &PrePrepare{
		Seq: &SeqView{
			View: r.view,
			SeqNo: n,
		},
		BatchDigest: digest,
		ReplicaId: r.id,
	}

	cert := r.getCert(r.view, n)
	cert.prePrepare = pp
	cert.digest = digest

	r.persistQSet()
	r.innerBroadcast(&MultiChainMessage{ChainID: r.chainId,
	Msg:&Message{Payload: pp}})
	r.maybeSendCommit(digest, r.view, n)

	return nil
}

func (r *RBFT) handlePreprepare(pp *PrePrepare, src string) error {
	if src == r.id {
		return nil
	}
	if !r.activeView {
		return fmt.Errorf("replica %s: ignoring pp as in view-change", r.id)
	}
	if pp.ReplicaId != r.primary(r.view) {
		return fmt.Errorf("replica %s: pp from non-primary %s", r.id, src)
	}
	if !r.inWV(pp.Seq.View, pp.Seq.SeqNo) {
		return nil
	}

	if pp.Seq.SeqNo > r.viewChangeSeqNo {
		logger.Info("replica %s received pp %d, which should be from next primary", r.id, pp.Seq.SeqNo)
		r.sendViewChange()
		return nil
	}

	cert := r.getCert(pp.Seq.View, pp.Seq.SeqNo)
	if cert.digest != "" && cert.digest != pp.BatchDigest {
		logger.Warningf("pp with same view/seq, but different digest (%s vs %s)", cert.digest, pp.BatchDigest)
		r.sendViewChange()
		return nil
	}

	cert.prePrepare = pp
	cert.digest = pp.BatchDigest

	// store the request batch if we
	// TODO
	if _, ok := r.reqBatchStore[pp.BatchDigest]; !ok && pp.BatchDigest != "" {
		digest := Hash(pp.Batch)
		if digest != pp.BatchDigest {
			return fmt.Errorf("pp and request digest do not match: %s vs %s", digest, pp.BatchDigest)
		}
		r.reqBatchStore[digest] = pp.Batch
		r.outstandingReqBatches[digest] = pp.Batch
		r.persistRequestBatch(digest)
	}

	r.softStartTimer(r.requestTimeout, fmt.Sprintf("new pp for request batch %s", pp.BatchDigest))

	if r.primary(r.view) != r.id && r.prePrepared(pp.BatchDigest, pp.Seq.View, pp.Seq.SeqNo) && !cert.sentPrepare {
		prep := &Prepare{
			Seq: pp.Seq,
			BatchDigest: pp.BatchDigest,
			ReplicaId: r.id,
		}

		cert.sentPrepare = true
		r.persistQSet()
		r.handlePrepare(prep)
		return r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId,Msg: &Message{Payload:prep}})
	}

	return nil
}

func (r *RBFT) handlePrepare(prep *Prepare) error {
	if r.primary(prep.Seq.View) == prep.ReplicaId {
		return nil
	}
	if !r.inWV(prep.Seq.View, prep.Seq.SeqNo) {
		return nil
	}

	cert := r.getCert(prep.Seq.View, prep.Seq.SeqNo)
	for _, prevPrep := range cert.prepare {
		if prevPrep.ReplicaId == prep.ReplicaId {
			return nil
		}
	}
	cert.prepare = append(cert.prepare, prep)
	r.persistPSet()

	return r.maybeSendCommit(prep.BatchDigest, prep.Seq.View, prep.Seq.SeqNo)
}

func (r *RBFT) maybeSendCommit(digest string, v uint64, n uint64) error {
	cert := r.getCert(v, n)
	if r.prepared(digest, v, n) && !cert.sentCommit {
		commit := &Commit{
			Seq: &SeqView{v, n},
			BatchDigest: digest,
			ReplicaId: r.id,
		}
		cert.sentCommit = true
		r.handleCommit(commit)
		return r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId,Msg:&Message{Payload: commit}})
	}

	return nil
}

func (r *RBFT) handleCommit(commit *Commit) error {
	if !r.inWV(commit.Seq.View, commit.Seq.SeqNo) {
		return nil
	}

	cert := r.getCert(commit.Seq.View, commit.Seq.SeqNo)
	for _, prevCommit := range cert.commit {
		if prevCommit.ReplicaId == commit.ReplicaId {
			return nil
		}
	}

	cert.commit = append(cert.commit, commit)
	if r.committed(commit.BatchDigest, commit.Seq.View, commit.Seq.SeqNo) {
		r.stopTimer()
		r.lastNewViewTimeout = r.newViewTimeout
		delete(r.outstandingReqBatches, commit.BatchDigest)

		r.executeOutstanding()

		if commit.Seq.SeqNo == r.viewChangeSeqNo {
			r.sendViewChange()
		}
	}

	return nil
}

func (r *RBFT) executeOutstanding() {
	if r.currentExec != nil {
		return
	}

	for idx := range r.certStore {
		if r.executeOne(idx) {
			break
		}
	}

	r.startTimerIfOutstandingRequests()
}

func (r *RBFT) executeOne(idx msgID) bool {
	cert := r.certStore[idx]

	if idx.n != r.lastExec+1 || cert == nil || cert.prePrepare == nil {
		return false
	}

	if r.skipInProgress {
		return false
	}

	digest := cert.digest
	reqBatch := r.reqBatchStore[digest]
	if !r.committed(digest, idx.v, idx.n) {
		return false
	}

	currentExec := idx.n
	r.currentExec = &currentExec

	if digest == "" {
		r.execDoneSync()
	} else {
		r.consumer.execute(idx.n, reqBatch)
	}
	return true
}

func (r *RBFT) execDoneSync() {
	if r.currentExec != nil {
		r.lastExec = *r.currentExec
		if r.lastExec % r.ChkptPeriod == 0 {
			r.Checkpoint(r.lastExec, r.consumer.getState())
		}
	} else {
		logger.Criticalf("Replica %s execDoneSync, flagging ourselves as out of date", r.id)
		r.skipInProgress = true
	}

	r.currentExec = nil
	// FIXME: r.executeOutstanding()
}

func (r *RBFT) Checkpoint(seqNo uint64, id []byte) {
	if seqNo % r.ChkptPeriod != 0 {
		return
	}

	idAsString := base64.StdEncoding.EncodeToString(id)
	chkpt := &Checkpoint{
		Seq: &SeqView {r.view, seqNo},
		CheckpointDigest: idAsString,
		Id: r.id,
	}
	r.checkpoints[seqNo] = idAsString

	r.persistCheckpoint(seqNo, id)
	r.handleCheckpoint(chkpt)
	r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId,Msg:&Message{Payload:chkpt}})
}

func (r *RBFT) handleCheckpoint(chkpt *Checkpoint) {

	if r.weakCheckpointSetOutOfRange(chkpt) {
		return
	}

	if !r.inW(chkpt.Seq.SeqNo) {
		return
	}

	r.checkpointStore[*chkpt] = true

	diffValues := make(map[string]struct{})
	diffValues[chkpt.Id] = struct {}{}

	matching := 0
	for c := range r.checkpointStore {
		if c.Seq.SeqNo == chkpt.Seq.SeqNo {
			if c.Id == chkpt.Id {
				matching++
			} else {
				if _, found := diffValues[c.Id]; !found {
					diffValues[c.Id] = struct {}{}
				}
			}
		}
	}

	if count := len(diffValues); count > r.NumFailedReplica+1 {
		logger.Panicf("failed to find stable certificate for seqNo %d (%d diff values)", chkpt.Seq.SeqNo, count)
	}

	if matching == r.NumFailedReplica+1 {
		r.witnessCheckpointWeakCert(chkpt)
	}

	if matching < r.intersectionQuorum() {
		return
	}

	if _, ok := r.checkpoints[chkpt.Seq.SeqNo]; !ok {
		if r.skipInProgress {
			logSafetyBound := r.LowWatermark + r.LogSize / 2
			if chkpt.Seq.SeqNo >= logSafetyBound {
				r.moveWatermarks(chkpt.Seq.SeqNo)
			}
		}
		return nil
	}

	r.moveWatermarks(chkpt.Seq.SeqNo)
	r.processNewView()
}

func (r *RBFT) fetchRequestBatches() error {
	for digest := range r.missingReqBatches {
		fetch := &FetchRequestBatch{
			BatchDigest: digest,
			ReplicaId: r.id,
		}
		msg := &MultiChainMessage{ChainID:r.chainId,Msg:&Message{Payload:fetch}}
		r.innerBroadcast(msg)
	}

	return nil
}
