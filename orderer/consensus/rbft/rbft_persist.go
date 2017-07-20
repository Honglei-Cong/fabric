package rbft

import (
	"github.com/golang/protobuf/proto"
	"fmt"
	"encoding/base64"
)

func (r *RBFT) persistPSet() error {
	var pset []*PQ
	for _, p := range r.calcPSet() {
		pset = append(pset, p)
	}
	return r.persistPQSet("pset", pset)
}

func (r *RBFT) persistQSet() error {
	var qset []*PQ
	for _, q := range r.calcQSet() {
		qset = append(qset, q)
	}
	return r.persistPQSet("qset", qset)
}

func (r *RBFT) persistPQSet(key string, set []*PQ) error {
	raw, err := proto.Marshal(&PQSet{set})
	if err != nil {
		return fmt.Errorf("Failed to persist %s: %s", key, err)
	}
	return r.backend.persist.StoreState(key, raw)
}

func (r *RBFT) restorePQSet(key string) []*PQ {
	raw, err := r.backend.persist.ReadState(key)
	if err != nil {
		logger.Debugf("replica %s could not restore state %s: %s", r.id, key, err)
		return nil
	}
	val := &PQSet{}
	if err := proto.Unmarshal(raw, val); err != nil {
		logger.Errorf("replica %s could not unmarshal %s: %s", r.id, key, err)
		return nil
	}
	return val.GetSet()
}

func (r *RBFT) restoreState() {
	updateSeqView := func(set []*PQ) {
		for _, e := range set {
			if r.view < e.View {
				r.view = e.View
			}
			if r.seqNo < e.SeqNo {
				r.seqNo = e.SeqNo
			}
		}
	}


	set := r.restorePQSet("pset")
	for _, e := range set {
		r.pset[e.SeqNo] = e
	}
	updateSeqView(set)

	set = r.restorePQSet("qset")
	for _, e := range set {
		r.qset[qidx{e.BatchDigest, e.SeqNo}] = e
	}
	updateSeqView(set)

	reqBatchesPacked, err := r.backend.persist.ReadStateSet("reqBatch.")
	if err == nil {
		for k, v := range reqBatchesPacked {
			reqBatch := &RequestBatch{}
			if err := proto.Unmarshal(v, reqBatch); err != nil {
				logger.Warningf("replica %d failed to restore request batch %s : %s", r.id, k, err)
			} else {
				r.reqBatchStore[Hash(reqBatch)] = reqBatch
			}
		}
	} else {
		logger.Warningf("replica %s failed to restore reqBatchStore: %s", r.id, err)
	}

	chpts, err := r.backend.persist.ReadStateSet("chkpt.")
	if err == nil {
		lowWatermark := r.lastExec
		for k, id := range chpts {
			var seqNo uint64
			if _, err := fmt.Sscanf("chkpt.%d", &seqNo); err != nil {
				logger.Warningf("replica %s failed to restore chkpt key %s: %s", r.id, k, err)
			} else {
				idAsString := base64.StdEncoding.EncodeToString(id)
				r.checkpoints[seqNo] = idAsString
				if seqNo < lowWatermark {
					lowWatermark = seqNo
				}
			}
		}
		r.moveWatermarks(lowWatermark)
	} else {
		logger.Warningf("replica %d could not restore checkpoint: %s", r.id, err)
	}
}

func (r *RBFT) persistDeleteCheckpoint(seqNo uint64) {
	key := fmt.Sprintf("chkpt.%d", seqNo)
	r.backend.persist.DelState(key)
}

func (r *RBFT) persistCheckpoint(seqNo uint64, id []byte) {
	key := fmt.Sprintf("chkpt.%d", seqNo)
	r.backend.persist.StoreState(key, id)
}

func (r *RBFT) persistRequestBatch(digest string) error {
	reqBatch := r.reqBatchStore[digest]
	d, err := proto.Marshal(reqBatch)
	if err != nil {
		return fmt.Errorf("replica %d marshal reqbatch failed: %s", r.id, err)
	}
	return r.backend.persist.StoreState("reqBatch."+digest, d)
}

func (r *RBFT) persistDelRequestBatch(digest string) {
	r.backend.persist.DelState("reqBatch."+digest)
}

func (r *RBFT) persistDelAllRequestBatches() {
	reqBatches, err := r.backend.persist.ReadStateSet("reqBatch.")
	if err == nil {
		for k := range reqBatches {
			r.backend.persist.DelState(k)
		}
	}
}
