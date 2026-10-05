package gateway

import (
	"sync"
	"time"
)

// kvBreaker is a minimal circuit breaker over the gateway's KV-dependent
// steps (rate limit, cache lookup, cache store).
//
// # Why a breaker on top of the per-call budget
//
// Options.KVCallTimeout already bounds each KV call, so no request waits out
// the KV client's full retry budget. But during an outage every request
// would still spend that bound twice (limiter, then cache) before failing
// open, and every one of them adds load to a KV that is trying to elect a
// leader. After a run of consecutive failures the breaker opens and the
// KV-dependent steps are skipped outright (fail open / cache miss) for a
// cool-down, so a request during an outage costs nothing extra. When the
// cool-down ends, ONE request is let through as a probe (half-open): if its
// KV call succeeds the breaker closes, if not it re-opens for another
// cool-down. One probe, not all of them, because a still-dead KV would
// otherwise stall the whole burst that arrives at the end of the cool-down.
//
// What counts: a KV error or a budget timeout is a failure; any answer from
// the KV (including ratelimit.ErrContended, which means the KV is up and
// busy) is a success. A call abandoned because the client went away is
// neither, since it says nothing about the KV.
type kvBreaker struct {
	threshold int           // consecutive failures that open it; <= 0 disables
	cooldown  time.Duration // how long it stays open before probing
	now       func() time.Time

	mu        sync.Mutex
	failures  int       // consecutive, reset by any success
	openUntil time.Time // zero when closed
	probing   bool      // a half-open probe is in flight
	probeAt   time.Time // when that probe was admitted
	trips     uint64    // closed/half-open -> open transitions
}

func newKVBreaker(threshold int, cooldown time.Duration, now func() time.Time) *kvBreaker {
	return &kvBreaker{threshold: threshold, cooldown: cooldown, now: now}
}

// allow reports whether a KV-dependent step should touch the KV now. A
// false means skip it and take the step's outage behaviour instead.
func (b *kvBreaker) allow() bool {
	if b.threshold <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.IsZero() {
		return true
	}
	now := b.now()
	if now.Before(b.openUntil) {
		return false
	}
	// Half-open. Admit one probe at a time. A probe that never reports back
	// (it should always report, but a wedged breaker is the worst failure
	// mode a breaker can have) is presumed lost after one more cool-down.
	if b.probing && now.Sub(b.probeAt) < b.cooldown {
		return false
	}
	b.probing = true
	b.probeAt = now
	return true
}

// success records a KV call that got an answer; it closes the breaker.
func (b *kvBreaker) success() {
	if b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	b.failures = 0
	b.openUntil = time.Time{}
	b.probing = false
	b.mu.Unlock()
}

// failure records a KV error or timeout.
func (b *kvBreaker) failure() {
	if b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	// A failed probe re-opens immediately; otherwise open on the threshold.
	// A failure reported by a call that started before the breaker opened
	// (requests in flight when it tripped) just extends nothing: the
	// breaker is already open and its cool-down stands.
	if b.probing || (b.openUntil.IsZero() && b.failures >= b.threshold) {
		b.openUntil = b.now().Add(b.cooldown)
		b.probing = false
		b.trips++
	}
}

func (b *kvBreaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openUntil.IsZero()
}

func (b *kvBreaker) tripCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trips
}
