// Package ratelimit is a distributed token-bucket rate limiter: one bucket
// per tenant, stored in the KV, updated with compare-and-swap, so any number
// of gateway replicas enforce a single shared budget.
//
// # Why a token bucket
//
// A token bucket admits bursts up to Burst and sustains Rate tokens per
// second after that. It is the standard shape for API quotas because it
// forgives short spikes (a client sending 20 requests at once after being
// idle) without letting a sustained flood through. The bucket is two
// numbers: tokens remaining, and when it was last refilled.
//
// # Why CAS, and why the refill is computed by the reader
//
// The naive distributed design gives each gateway its own bucket and
// divides the budget by the number of gateways. That breaks the moment the
// gateway count changes, and it lets a tenant that happens to hit one
// gateway exhaust "its" share while the others sit idle. One shared bucket
// avoids both, but now two gateways may read the bucket at the same instant
// and both try to take the last token. CAS is what makes exactly one of
// them win: the loser sees the bucket changed underneath it, re-reads, and
// finds no token left.
//
// Refill is computed lazily from (now - lastRefill) at read time rather than
// by a background ticker. There is no process that "owns" the bucket to run
// a ticker in; lazy refill needs none, and it is exactly correct: the
// bucket's value at time t is a pure function of its last stored state and t.
//
// # Time
//
// "now" is the gateway's clock. Two gateways with skewed clocks will
// disagree slightly about how much has refilled, so the effective rate is
// fuzzy by the skew; it can never let a tenant take more than Burst tokens
// in one instant (that bound is enforced by the CAS'd value, not by time).
// Same rule as Phase 4: clocks affect liveness/precision, CAS guards the
// hard limit.
package ratelimit

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"dsys/kvapi"
)

// Limit is a tenant's rate in tokens per second and its burst capacity.
type Limit struct {
	Rate  float64
	Burst float64
}

// Options configures a Limiter.
type Options struct {
	// Prefix namespaces bucket keys. Default "ratelimit".
	Prefix string
	// Rate and Burst are the defaults for tenants not listed in Tenants.
	Rate  float64
	Burst float64
	// Tenants overrides the default per tenant id.
	Tenants map[string]Limit
	// Clock supplies now. Tests inject a fake.
	Clock func() time.Time
	// MaxAttempts bounds CAS retries under contention before giving up
	// with ErrContended. Default 32.
	MaxAttempts int
}

// ErrContended is returned when the bucket kept changing under us for
// MaxAttempts consecutive tries. Treat it as "try again shortly"; it means
// many gateways are hammering one tenant's bucket at once.
var ErrContended = errors.New("ratelimit: bucket contended, retry")

// Limiter enforces per-tenant limits over a shared KV.
type Limiter struct {
	kv   kvapi.KV
	opts Options
}

// New returns a Limiter. Buckets are created on first use, full.
func New(kv kvapi.KV, opts Options) *Limiter {
	if opts.Prefix == "" {
		opts.Prefix = "ratelimit"
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 32
	}
	if opts.Rate <= 0 {
		opts.Rate = 10
	}
	if opts.Burst <= 0 {
		opts.Burst = opts.Rate * 2
	}
	return &Limiter{kv: kv, opts: opts}
}

// LimitFor returns the effective limit for tenant.
func (l *Limiter) LimitFor(tenant string) Limit {
	if lim, ok := l.opts.Tenants[tenant]; ok {
		if lim.Burst <= 0 {
			lim.Burst = lim.Rate * 2
		}
		return lim
	}
	return Limit{Rate: l.opts.Rate, Burst: l.opts.Burst}
}

// bucket is the stored record: 16 bytes, tokens as float64 bits then
// lastRefill as unix ms.
type bucket struct {
	tokens float64
	lastMs int64
}

func encodeBucket(b bucket) []byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[0:8], math.Float64bits(b.tokens))
	binary.BigEndian.PutUint64(out[8:16], uint64(b.lastMs))
	return out[:]
}

func decodeBucket(raw []byte) (bucket, bool) {
	if len(raw) != 16 {
		return bucket{}, false
	}
	return bucket{
		tokens: math.Float64frombits(binary.BigEndian.Uint64(raw[0:8])),
		lastMs: int64(binary.BigEndian.Uint64(raw[8:16])),
	}, true
}

// refill returns the bucket's state at nowMs, given its last stored state.
func refill(b bucket, lim Limit, nowMs int64) bucket {
	if nowMs > b.lastMs {
		elapsed := float64(nowMs-b.lastMs) / 1000.0
		b.tokens = math.Min(lim.Burst, b.tokens+elapsed*lim.Rate)
	}
	b.lastMs = nowMs
	return b
}

// Take tries to remove n tokens from tenant's bucket. On success ok is
// true. On refusal ok is false and retryAfter is how long until n tokens
// will have refilled, for a Retry-After hint.
func (l *Limiter) Take(ctx context.Context, tenant string, n float64) (ok bool, retryAfter time.Duration, err error) {
	lim := l.LimitFor(tenant)
	if n > lim.Burst {
		// Can never be satisfied; say so with a finite hint.
		return false, time.Duration(float64(time.Second) * (n / lim.Rate)), nil
	}
	key := l.opts.Prefix + "/" + tenant

	for attempt := 0; attempt < l.opts.MaxAttempts; attempt++ {
		raw, found, err := l.kv.Get(ctx, key)
		if err != nil {
			return false, 0, err
		}
		nowMs := l.opts.Clock().UnixMilli()
		var b bucket
		if found {
			var okDecode bool
			b, okDecode = decodeBucket(raw)
			if !okDecode {
				// Corrupt or foreign record; start fresh rather than fail
				// every request for this tenant forever.
				found = false
			}
		}
		if !found {
			b = bucket{tokens: lim.Burst, lastMs: nowMs}
		}
		b = refill(b, lim, nowMs)

		if b.tokens < n {
			// Refused. Do not write: nothing changed that others need to
			// see, and skipping the write keeps a refused tenant from
			// generating KV traffic. The hint is exact given the rate.
			deficit := n - b.tokens
			return false, time.Duration(float64(time.Second) * (deficit / lim.Rate)), nil
		}
		b.tokens -= n

		// The CAS is the whole point: if another gateway consumed tokens
		// between our Get and now, expected no longer matches, we lose, and
		// we re-read. Two gateways can never both take the same token.
		swapped, _, err := l.kv.CAS(ctx, key, raw, !found, encodeBucket(b))
		if err != nil {
			return false, 0, err
		}
		if swapped {
			return true, 0, nil
		}
	}
	return false, 0, ErrContended
}

// Peek returns the bucket's current token count without taking any.
func (l *Limiter) Peek(ctx context.Context, tenant string) (float64, error) {
	lim := l.LimitFor(tenant)
	raw, found, err := l.kv.Get(ctx, l.opts.Prefix+"/"+tenant)
	if err != nil {
		return 0, err
	}
	nowMs := l.opts.Clock().UnixMilli()
	if !found {
		return lim.Burst, nil
	}
	b, ok := decodeBucket(raw)
	if !ok {
		return lim.Burst, nil
	}
	return refill(b, lim, nowMs).tokens, nil
}
