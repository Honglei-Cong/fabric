package rbft

import (
	"fmt"
	"reflect"
	"encoding/base64"
)

type checkpointMessage struct {
	seqNo uint64
	id []byte
}

type stateUpdateTarget struct {
	checkpointMessage
	replicas []string
}

func (r *RBFT) sendViewChange() error {
	r.stopTimer()

	delete(r.newViewStore, r.view)
	r.view++
	r.activeView = false

	r.pset = r.calcPSet()
	r.qset = r.calcQSet()

	// clear old msg
	for idx := range r.certStore {
		if idx.v < r.view {
			delete(r.certStore, idx)
		}
	}
	for idx := range r.viewChangeStore {
		if idx.v < r.view {
			delete(r.viewChangeStore, idx)
		}
	}

	vc := &ViewChange{
		View: r.view,
		LowWatermark: r.LowWatermark,
		ReplicaId: r.id,
	}

	for n, id := range r.hChkpts {
		vc.Cset = append(vc.Cset, &ViewChange_C{
			SequenceNumber: n,
			Id: id,
		})
	}

	for _, p := range r.pset {
		vc.Pset = append(vc.Pset, p)
	}
	for _, q := range r.qset {
		vc.Qset = append(vc.Qset, q)
	}

	r.sign(vc)

	r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId, Msg:&Message{Payload:vc}})
	r.vcResendTimer.Reset(r.vcResendTimeout)
	return r.handleViewChange(vc)
}

func (r *RBFT) handleViewChange(vc *ViewChange) error {
	if err := r.verify(vc); err != nil {
		logger.Warningf("replica %s, incorrect signature in view change: %s", r.id, err)
		return err
	}
	if vc.View < r.view {
		logger.Warningf("replica %s, view-change msg for old view (%d vs %d)", r.id, vc.View, r.view)
		return nil
	}

	if !r.correctViewChange(vc) {
		logger.Warningf("replica %s found incorrect view change", r.id)
		return nil
	}
	if _, ok := r.viewChangeStore[vcidx{vc.View, vc.ReplicaId}]; ok {
		logger.Infof("replica %s already has a view change msg for view %d from replica %s", r.id, vc.View, vc.ReplicaId)
		return nil
	}

	r.viewChangeSeqNo[vcidx{vc.View, vc.ReplicaId}] = vc

	replicas := make(map[string]bool)
	minview := uint64(0)
	for idx := range r.viewChangeStore {
		if idx.v < r.view {
			continue
		}
		replicas[idx.id] = true
		if minview == 0 || idx.v < minview {
			minview = idx.v
		}
	}
	if len(replicas) > r.NumFailedReplica + 1 {
		logger.Infof("replica %s, received f+1 view change, trigger view change to %d", r.id, minview)
		r.view = minview - 1
		return r.sendViewChange()
	}

	quorum := 0
	for idx := range r.viewChangeStore {
		if idx.v == r.view {
			quorum++
		}
	}
	if !r.activeView && vc.View == r.view && quorum >= r.allCorrectReplicasQuorum() {
		r.vcResendTimer.Stop()
		r.startTimer(r.lastNewViewTimeout, "new view change")
		r.lastNewViewTimeout = 2 * r.lastNewViewTimeout

		// TODO:  return viewChangeQuorumEvent{}
	}

	return nil
}

func (r *RBFT) getViewChanges() (vset []*ViewChange){
	for _, vc := range r.viewChangeStore {
		vset = append(vset, vc)
	}
	return
}

func (r *RBFT) selectInitialCheckpoint(vset []*ViewChange)(Checkpoint ViewChange_C, ok bool, replicas []string) {

}

func (r *RBFT) sendNewView() error {
	if _, ok := r.newViewStore[r.view]; ok {
		return nil
	}

	vset := r.getViewChanges()
	cp, ok, _ := r.selectInitialCheckpoint(vset)
	if !ok {
		logger.Warningf("replica %s could not find consistent checkpoint: %+v", r.id, r.viewChangeStore)
		return nil
	}

	msgList := r.assignSequenceNumbers(vset, cp.SequenceNumber)
	if msgList == nil {
		return fmt.Errorf("replica %s failed to assign seqno for new view", r.id)
	}

	nv := &NewView{
		View: r.view,
		Vset: vset,
		Xset: msgList,
		ReplicaId: r.id,
	}

	r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId,Msg:Message{Payload:nv}})
	r.newViewStore[r.view] = nv
	return r.processNewView()
}

func (r *RBFT) handleNewView(nv *NewView) error {
	if (!nv.View > 0 && nv.View >= r.view && r.primary(nv.View) == nv.ReplicaId && r.newViewStore[nv.View] == nil) {
		logger.Infof("replica %s rejecting invalid new-view from %s, v: %d", r.id, nv.ReplicaId, nv.View)
		return nil
	}

	for _, vc := range nv.Vset {
		if err := r.verify(vc); err != nil {
			return fmt.Errorf("replica %s found incorrect view-change signature: %s", r.id, err)
		}
	}

	r.newViewStore[nv.View] = nv
	return r.processNewView()
}

func (r *RBFT) processNewView() error {
	nv, ok := r.newViewStore[r.view]
	if !ok {
		logger.Debugf("replica %s ignore processNewView as not find view % in its view store", r.id, r.view)
		return nil
	}

	if r.activeView {
		return nil
	}

	cp, ok, replicas := r.selectInitialCheckpoint(nv.Vset)
	if !ok {
		logger.Warningf("replica %s could not determine init checkpoint %+v", r.id, r.viewChangeStore)
		return r.sendViewChange()
	}

	speculativeLastExec := r.lastExec
	if r.currentExec != nil {
		speculativeLastExec = *r.currentExec
	}

	if speculativeLastExec < cp.SequenceNumber {
		canExecuteToTarget := true
		outer:
		for seqNo := speculativeLastExec + 1; seqNo < cp.SequenceNumber; seqNo++ {
			found := false
			for idx, cert := range r.certStore {
				if idx.n != seqNo {
					continue
				}
				quorum := 0
				for _, p := range cert.commit {
					if p.Seq.View == idx.v && p.Seq.SeqNo == seqNo {
						quorum++
					}
				}
				if quorum < r.intersectionQuorum() {
					continue
				}
				found = true
				break
			}

			if !found {
				canExecuteToTarget = false
				break outer
			}
		}

		if canExecuteToTarget {
			return nil
		}

		logger.Infof("replica %s cannot execute to the view change checkpoint with seqNo %d", r.id, cp.SequenceNumber)
	}

	msgList := r.assignSequenceNumbers(nv.Vset, cp.SequenceNumber)
	if msgList == nil {
		return r.sendViewChange()
	}

	if !(len(msgList) == 0 && len(nv.Xset) == 0) && !reflect.DeepEqual(msgList, nv.Xset) {
		logger.Warningf("replica %d failed to verify new view xset (%+v vs %+v)", r.id, msgList, nv.Xset)
		return r.sendViewChange()
	}

	if r.LowWatermark < cp.SequenceNumber {
		r.moveWatermarks(cp.SequenceNumber)
	}

	if speculativeLastExec < cp.SequenceNumber {
		snapshotID, err := base64.StdEncoding.DecodeString(cp.Id)
		if nil != err {
			return fmt.Errorf("replica %s received a view change whose hash could not be decoded(%s)", r.id, cp.Id)
		}

		target := &stateUpdateTarget{
			checkpointMessage: checkpointMessage{
				seqNo: cp.SequenceNumber,
				id: snapshotID,
			},
			replicas: replicas,
		}

		r.updateHighStateTarget(target)
		r.stateTransfer(target)
	}

	newReqBatchMissing := false
	for n, d := range nv.Xset {
		if n <= r.LowWatermark {
			continue
		} else {
			if d == "" {
				continue
			}

			if _, ok := r.reqBatchStore[d]; !ok {
				if _, ok := r.missingReqBatches[d]; !ok {
					logger.Warningf("replica %s request to fetch batch %s", r.id, d)
					newReqBatchMissing = true
					r.missingReqBatches[d] = true
				}
			}
		}
	}
	if len(r.missingReqBatches) == 0 {
		return r.processNewView2(nv)
	} else if newReqBatchMissing {
		r.fetchRequestBatches()
	}

	return nil
}

func (r *RBFT) processNewView2(nv *NewView) error {
	r.stopTimer()

	r.activeView = true
	delete(r.newViewStore, r.view-1)

	r.seqNo = r.LowWatermark
	for n, d := range nv.Xset {
		if n < r.LowWatermark {
			continue
		}
		reqBatch, ok := r.reqBatchStore[d]
		if !ok && d != "" {
			logger.Criticalf("replica %s missing req batch for seq %d with digest %s !!!", r.id, n, d)
		}
		pp := &PrePrepare{
			Seq: &SeqView{ View: r.view, SeqNo: n},
			BatchDigest: d,
			ReplicaId: r.id,
		}
		cert := r.getCert(r.view, n)
		cert.prePrepare = pp
		cert.digest = d
		if n > r.seqNo {
			r.seqNo = n
		}
		r.persistQSet()
	}

	r.updateViewChangeSeqNo()

	if r.primary(r.view) != r.id {
		for n, d := range nv.Xset {
			p := &Prepare{
				Seq: &SeqView{View: r.view, SeqNo: n},
				BatchDigest: d,
				ReplicaId: r.id,
			}
			if n > r.LowWatermark {
				cert := r.getCert(r.view, n)
				cert.sentPrepare = true
				r.handlePrepare(p)
			}
			r.innerBroadcast(&MultiChainMessage{ChainID:r.chainId,Msg:&Message{Payload:p}})
		}
	} else {
		r.resubmitRequestBatches()
	}

	r.startTimerIfOutstandingRequests()

	// TODO: return viewChangedEvent{}
	return nil
}

func (r *RBFT) correctViewChange(vc *ViewChange) bool {
	for _, p := range append(vc.Pset, vc.Qset...) {
		if !(p.View < vc.View && p.SeqNo > vc.LowWatermark && p.SeqNo <= vc.LowWatermark + r.LogSize) {
			return false
		}
	}

	for _, c := range vc.Cset {
		if !(c.SequenceNumber >= vc.LowWatermark && c.SequenceNumber <= vc.LowWatermark + r.LogSize) {
			return false
		}
	}

	return true
}

func (r *RBFT) updateHighStateTarget(target *stateUpdateTarget) {
	if r.highStateTarget != nil && r.highStateTarget.seqNo >= target.seqNo {
		return
	}
	r.highStateTarget = target
}

func (r *RBFT) startTransfer(optional *stateUpdateTarget) {
	if !r.skipInProgress {
		r.skipInProgress = true
		r.consumer.invalidateState()
	}
	r.retryStateTransfer(optional)
}

func (r *RBFT) retryStateTransfer(optional *stateUpdateTarget) {
	if r.currentExec != nil {
		return
	}
	if r.stateTransfering {
		return
	}

	target := optional
	if target == nil {
		if r.highStateTarget == nil {
			return
		}
		target = r.highStateTarget
	}

	r.stateTransfering = true
	r.consumer.skipTo(target.seqNo, target.id, target.replicas)
}
