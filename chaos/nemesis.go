package chaos

// The nemesis: a seeded schedule of disruptions applied to the KV cluster.
// It is deliberately the only thing in this package with its own private
// *rand.Rand, so "which nodes get hit, in what order, with what kind of
// event" is a pure function of the scenario's seed and nothing else.

import (
	"math/rand"
	"sync"
	"time"
)

// nemesisEvent is the un-timestamped decision; harness.go stamps Index/At.
type nemesisEvent struct {
	kind  NemesisEventKind
	node  int
	delay time.Duration // for Pause/Crash: how long until auto-heal/restart
}

// chooseEvent picks one eligible (kind, node) pair. Eligibility rules:
//
//   - Heal only applies to a node that is partitioned but not crashed.
//   - Partition and Pause only apply to a node that is neither, AND only if
//     taking it down would still leave a majority up. Raft promises to
//     tolerate any MINORITY of simultaneous failures, never more — a
//     nemesis that pushes the cluster below quorum isn't testing that
//     promise, it is testing something else (whether Raft correctly
//     refuses to operate without quorum, which it does, and always will:
//     no amount of harness patience fixes a cluster that has no majority
//     left to elect a leader from). The first version of this harness had
//     no such bound; seeds 2 and 4 partitioned three of five nodes with no
//     Heal ever drawn for any of them before the run ended, and the
//     "violation" it reported was Raft correctly sitting unavailable
//     forever, not a bug in Raft. See docs/phase6.md.
//   - Crash is bound by the SAME majority rule (a crash also removes a
//     vote), independent of Partition/Pause: the two kinds share one "how
//     many are currently down" budget.
//
// Returns nil if nothing is eligible (e.g. every node already crashed, or
// the majority bound leaves only Heal available and nothing is healable —
// the harness treats that as "skip this tick", never as blocking forever).
func chooseEvent(c *kvCluster, r *rand.Rand, pauseDur time.Duration) *nemesisEvent {
	down := 0
	var healable, freeNodes, crashable []int
	for i := 0; i < c.n; i++ {
		p, cr := c.isPartitioned(i), c.isCrashed(i)
		if p || cr {
			down++
		}
		switch {
		case p && !cr:
			healable = append(healable, i)
		case !p && !cr:
			freeNodes = append(freeNodes, i)
		}
		if !cr {
			crashable = append(crashable, i)
		}
	}
	majority := c.n/2 + 1
	roomToTakeDown := c.n - majority - down // how many MORE can go down and still leave a majority

	type option struct {
		kind NemesisEventKind
		pool []int
	}
	var options []option
	if len(healable) > 0 {
		options = append(options, option{EventHeal, healable})
	}
	if roomToTakeDown > 0 {
		if len(freeNodes) > 0 {
			options = append(options, option{EventPartition, freeNodes})
			options = append(options, option{EventPause, freeNodes})
		}
		if len(crashable) > 0 {
			options = append(options, option{EventCrash, crashable})
		}
	}
	if len(options) == 0 {
		return nil
	}
	opt := options[r.Intn(len(options))]
	node := opt.pool[r.Intn(len(opt.pool))]

	ev := &nemesisEvent{kind: opt.kind, node: node}
	switch opt.kind {
	case EventPause:
		ev.delay = pauseDur
	case EventCrash:
		ev.delay = time.Duration(300+r.Intn(700)) * time.Millisecond
	}
	return ev
}

// apply carries out ev against c. Pause and Crash schedule their own
// auto-recovery after ev.delay and return immediately; the nemesis loop is
// never blocked waiting for a disruption to end, exactly like a real
// nemesis (it moves on to the next decision while this one plays out).
//
// pending is incremented before scheduling a delayed recovery and marked
// Done once it lands. The harness waits on it after the run ends, INSTEAD
// OF guessing a fixed settle time — several crashes can stack up (nothing
// stops the nemesis from taking out a majority in quick succession before
// earlier restarts have fired), and a fixed sleep short enough to be cheap
// in the common case is exactly the sleep that is too short the one time
// four of five nodes went down together. This is a real bug the first
// version of this harness had (see docs/phase6.md).
func (ev *nemesisEvent) apply(c *kvCluster, pending *sync.WaitGroup) {
	switch ev.kind {
	case EventHeal:
		c.heal(ev.node)
	case EventPartition:
		c.partition(ev.node)
	case EventPause:
		c.partition(ev.node)
		pending.Add(1)
		go func(n int, d time.Duration) {
			defer pending.Done()
			time.Sleep(d)
			c.heal(n)
		}(ev.node, ev.delay)
	case EventCrash:
		c.crash(ev.node)
		pending.Add(1)
		go func(n int, d time.Duration) {
			defer pending.Done()
			time.Sleep(d)
			c.start(n)
		}(ev.node, ev.delay)
	}
}
