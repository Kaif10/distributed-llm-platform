package sched

// Leader election for the reaper, done with the same lease-in-the-KV trick
// the jobs themselves use.
//
// # Why elect a leader at all when Reap is already safe to run anywhere
//
// Every step of Reap is a CAS, so N reapers running concurrently cannot
// corrupt anything; they just do redundant reads and lose CASes to each
// other. Leadership here is therefore purely an EFFICIENCY device: with ten
// scheduler replicas, one reaper scanning is plenty, and the other nine
// should not multiply the KV read load by ten. Nothing about correctness
// depends on the lease being honoured. If two replicas both believe they
// lead (clock skew, a long GC pause across the lease boundary), the worst
// case is a brief period of double scanning.
//
// Contrast with the job leases in queue.go, where the lease is ALSO only for
// liveness and the fencing token carries the safety. The rule generalises:
// use a lease to decide who should do work; use a fence (or here, the fact
// that every write is a CAS) to make sure a stale lease-holder cannot do
// harm. A design where correctness depends on a lease not expiring early is
// a design that is wrong on every real machine.

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// leaderRecord is stored at <prefix>/leader as "holder|untilMs".
type leaderRecord struct {
	holder  string
	untilMs int64
}

func parseLeader(b []byte) leaderRecord {
	s := string(b)
	i := strings.LastIndexByte(s, '|')
	if i < 0 {
		return leaderRecord{}
	}
	u, _ := strconv.ParseInt(s[i+1:], 10, 64)
	return leaderRecord{holder: s[:i], untilMs: u}
}

func (r leaderRecord) bytes() []byte {
	return []byte(r.holder + "|" + strconv.FormatInt(r.untilMs, 10))
}

// tryLead acquires or renews the leader lease for holder. It returns true if
// holder leads for at least ttl from now.
func (q *Queue) tryLead(ctx context.Context, holder string, ttl time.Duration) (bool, error) {
	raw, found, err := q.kv.Get(ctx, q.keyLeader())
	if err != nil {
		return false, err
	}
	now := q.nowMs()
	if found {
		cur := parseLeader(raw)
		if cur.holder != holder && cur.untilMs > now {
			return false, nil // someone else leads, and their lease is live
		}
		// Either it is ours (renew) or it has expired (take it over).
	}
	next := leaderRecord{holder: holder, untilMs: now + ttl.Milliseconds()}
	swapped, _, err := q.kv.CAS(ctx, q.keyLeader(), raw, !found, next.bytes())
	if err != nil {
		return false, err
	}
	return swapped, nil
}

// Leader returns the current leader record, for status displays.
func (q *Queue) Leader(ctx context.Context) (holder string, until time.Time, err error) {
	raw, found, err := q.kv.Get(ctx, q.keyLeader())
	if err != nil || !found {
		return "", time.Time{}, err
	}
	r := parseLeader(raw)
	return r.holder, time.UnixMilli(r.untilMs), nil
}

// RunReaper runs until ctx is done. Every interval it tries to acquire or
// renew the leader lease (ttl = 3*interval, so a leader survives two missed
// ticks before anyone else takes over) and, while leading, calls Reap.
// KV errors are swallowed and retried next tick: a reaper that crashes on a
// transient error is worse than one that skips a beat.
func RunReaper(ctx context.Context, q *Queue, holder string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ttl := 3 * interval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lead, err := q.tryLead(ctx, holder, ttl)
		if err != nil || !lead {
			continue
		}
		_, _ = q.Reap(ctx)
	}
}
