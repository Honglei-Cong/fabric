package rbft

import "fmt"

const maxBacklogSeq = 4
const msgPerSeq = 3 // (pre)prepare, commit, checkpoint

func (r *RBFT) testBacklogMessage(m *Message, src uint64) bool {
	test := func(seq *SeqView) bool {
		if !r.activeView {
			return true
		}
		if seq.SeqNo > r.cur.subject.Seq.Seq || seq.View > r.view {
			return true
		}
		return false
	}

	if pp := m.GetPrePrepare(); pp != nil {
		return test(pp.Seq) && !r.cur.checkpointDone
	} else if p := m.GetPrepare(); p != nil {
		return test(p.Seq)
	} else if c := m.GetCommit(); c != nil {
		return test(c.Seq)
	} else if chk := m.GetCheckpoint(); chk != nil {
		return test(&SeqView{SeqNo: chk.Seq})
	}
	return false
}

func (r *RBFT) recordBacklogMsg(m *Message, src uint64) {
	if src == r.id {
		panic(fmt.Sprintf("should never have to backlog my own message (replica ID: %d)", src))
	}

	r.replicaState[src].backLog = append(r.replicaState[src].backLog, m)

	if len(r.replicaState[src].backLog) > maxBacklogSeq*msgPerSeq {
		logger.Debugf("replica %d: backlog for %d full, discarding and reconnecting", r.id, src)
		r.discardBacklog(src)
		r.backend.Reconnect(r.chainId, src)
	}
}

