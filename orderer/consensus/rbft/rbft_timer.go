package rbft

import (
	"time"
)

type Timer struct {
	chain *chainImpl
	name string
	dur time.Duration
	timer *time.Timer
}

func (chain *chainImpl)CreateTimer(name string, dur time.Duration) *Timer {
	t := &Timer{
		chain: chain,
		name: name,
		dur: dur,
	}

	if dur > 0 {
		t.timer = time.AfterFunc(t.dur, func() {
			t.chain.enqueueTimer(t)
		})
	}
	return t
}

func (t *Timer)SoftReset(dur time.Duration) {
	t.dur = dur
	if t.timer == nil {
		t.timer = time.AfterFunc(t.dur, func() {
			t.chain.enqueueTimer(t)
		})
	}
}

func (t *Timer)Reset(duration time.Duration) {
	t.dur = duration
	if t.timer != nil {
		t.timer.Reset(duration)
	} else if t.dur > 0 {
		t.timer = time.AfterFunc(t.dur, func() {
			t.chain.enqueueTimer(t)
		})
	}
}

func (t *Timer)Stop() {
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}
