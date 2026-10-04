// Package router answers two questions for the gateway: which inference
// workers are alive right now, and which one should serve this prompt.
//
// # Discovery by lease
//
// Workers register themselves with a lease (Phase 4's idea, reused): a
// registration is valid until LeaseUntil and must be renewed before then.
// A worker that crashes simply stops renewing and drops out of routing when
// its lease lapses; no failure detector, no gossip, no health-check sweeper.
// The registry lives in the KV under one key so every gateway replica sees
// the same live set. One key means one CAS hot spot when many workers renew
// at once; at the scale here (tens of workers renewing every second) that is
// nothing. The exercise is per-worker keys plus a listing primitive.
//
// # Placement by rendezvous hashing
//
// Inference servers keep the attention KV cache of recent prompts and can
// skip prefill for a prefix they have already computed. Two requests that
// share a long system prompt therefore cost far less if they hit the same
// worker. We want a function
//
//	place(prefix, liveWorkers) -> worker
//
// that is stable (same prefix, same worker, on every gateway, with no
// coordination) and that moves as few prefixes as possible when a worker
// joins or leaves. Rendezvous (highest-random-weight) hashing does exactly
// that: score every worker by hash(prefix, workerID) and take the maximum.
// Remove a worker and only the prefixes it was winning move (each to its
// runner-up); add one and it steals only the prefixes where it now scores
// highest, about 1/n of them. Compare with hash(prefix) mod n, where a
// change in n reshuffles almost everything.
//
// Pure affinity would let one hot prefix pin one worker while the others
// idle, so placement is load-aware: walk workers in score order and take
// the first below MaxInflight. Affinity is a preference, capacity is a rule.
package router

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	"dsys/kvapi"
)

// Worker is one registered inference worker.
type Worker struct {
	ID           string
	Addr         string
	Model        string
	Inflight     int32
	LeaseUntilMs int64
}

// Options configures a Registry.
type Options struct {
	Prefix string // KV key namespace; default "workers"
	Clock  func() time.Time
	// CacheTTL is how long a gateway reuses its last read of the live set
	// before going back to the KV. Registrations are eventually visible
	// within this window, which is fine: a worker that just joined will
	// start getting traffic a few hundred ms later. 0 means 200ms.
	CacheTTL time.Duration
}

// Registry is the shared worker set.
type Registry struct {
	kv   kvapi.KV
	opts Options

	mu       sync.Mutex
	cached   []Worker
	cachedAt time.Time // zeroed by invalidate to force a re-read
	loaded   bool      // cached holds a successful read (the outage fallback)
}

// NewRegistry returns a Registry over kv.
func NewRegistry(kv kvapi.KV, opts Options) *Registry {
	if opts.Prefix == "" {
		opts.Prefix = "workers"
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 200 * time.Millisecond
	}
	return &Registry{kv: kv, opts: opts}
}

func (r *Registry) key() string { return r.opts.Prefix + "/index" }

func encodeWorkers(ws []Worker) []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(ws)
	return buf.Bytes()
}

func decodeWorkers(raw []byte) ([]Worker, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var ws []Worker
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&ws); err != nil {
		return nil, err
	}
	return ws, nil
}

// ErrContended: the index kept changing for too many attempts.
var ErrContended = errors.New("router: registry contended, retry")

// Register upserts w with a lease of ttl from now, pruning expired entries
// in passing. Returns the lease deadline.
func (r *Registry) Register(ctx context.Context, w Worker, ttl time.Duration) (time.Time, error) {
	for attempt := 0; attempt < 32; attempt++ {
		raw, found, err := r.kv.Get(ctx, r.key())
		if err != nil {
			return time.Time{}, err
		}
		ws, err := decodeWorkers(raw)
		if err != nil {
			ws = nil // corrupt index: rebuild from this registration
		}
		now := r.opts.Clock()
		w.LeaseUntilMs = now.Add(ttl).UnixMilli()
		next := make([]Worker, 0, len(ws)+1)
		replaced := false
		for _, o := range ws {
			if o.ID == w.ID {
				next = append(next, w)
				replaced = true
				continue
			}
			if o.LeaseUntilMs > now.UnixMilli() {
				next = append(next, o)
			}
		}
		if !replaced {
			next = append(next, w)
		}
		sort.Slice(next, func(i, j int) bool { return next[i].ID < next[j].ID })
		swapped, _, err := r.kv.CAS(ctx, r.key(), raw, !found, encodeWorkers(next))
		if err != nil {
			return time.Time{}, err
		}
		if swapped {
			r.invalidate()
			return time.UnixMilli(w.LeaseUntilMs), nil
		}
	}
	return time.Time{}, ErrContended
}

// Deregister removes w immediately (a graceful shutdown).
func (r *Registry) Deregister(ctx context.Context, id string) error {
	for attempt := 0; attempt < 32; attempt++ {
		raw, found, err := r.kv.Get(ctx, r.key())
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		ws, _ := decodeWorkers(raw)
		next := ws[:0:0]
		for _, o := range ws {
			if o.ID != id {
				next = append(next, o)
			}
		}
		swapped, _, err := r.kv.CAS(ctx, r.key(), raw, false, encodeWorkers(next))
		if err != nil {
			return err
		}
		if swapped {
			r.invalidate()
			return nil
		}
	}
	return ErrContended
}

func (r *Registry) invalidate() {
	r.mu.Lock()
	r.cachedAt = time.Time{}
	r.mu.Unlock()
}

// Live returns workers whose lease has not expired, from a short-lived
// local cache.
//
// If the KV read fails (an election, a partition) and this Registry has
// read the set before, Live serves that last known set, still filtered by
// lease, instead of an error. The control plane being briefly unavailable
// should not take inference down while the workers themselves are healthy.
// It stays safe because the lease is the staleness bound: workers cannot
// renew while the KV is down either, so a long outage drains the set as
// leases lapse, exactly as if each worker had stopped renewing. The failed
// read also restamps cachedAt, so during an outage the KV is retried once
// per CacheTTL rather than by every request.
func (r *Registry) Live(ctx context.Context) ([]Worker, error) {
	now := r.opts.Clock()
	r.mu.Lock()
	if !r.cachedAt.IsZero() && now.Sub(r.cachedAt) < r.opts.CacheTTL {
		ws := filterLive(r.cached, now)
		r.mu.Unlock()
		return ws, nil
	}
	r.mu.Unlock()

	raw, _, err := r.kv.Get(ctx, r.key())
	if err != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		if !r.loaded {
			return nil, err
		}
		r.cachedAt = now
		return filterLive(r.cached, now), nil
	}
	ws, err := decodeWorkers(raw)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cached = ws
	r.cachedAt = now
	r.loaded = true
	r.mu.Unlock()
	return filterLive(ws, now), nil
}

func filterLive(ws []Worker, now time.Time) []Worker {
	out := make([]Worker, 0, len(ws))
	for _, w := range ws {
		if w.LeaseUntilMs > now.UnixMilli() {
			out = append(out, w)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Placement: pure functions over a live set.
// ---------------------------------------------------------------------------

// Prefix returns the first n runes of prompt, the part that decides
// placement. Requests sharing this much text share prefill work.
func Prefix(prompt string, n int) string {
	if n <= 0 {
		n = 256
	}
	i := 0
	for pos := range prompt {
		if i == n {
			return prompt[:pos]
		}
		i++
	}
	return prompt
}

func fnv64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// mix64 is the murmur3/splitmix64 finalizer: it avalanches, so flipping one
// input bit flips about half the output bits.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// score is the rendezvous weight of id for prefix.
//
// The obvious implementation, fnv64a(prefix + separator + id), is WRONG
// here, and wrong in a way that looks fine until you measure it. FNV-1a
// finishes with h = (h ^ lastByte) * prime, so two ids differing only in
// their final byte (w0..w4, pod-1..pod-9, the way real worker ids look)
// produce scores related by a fixed multiplicative step. The comparison
// "which id scores highest" then depends far more on the id than on the
// prefix, and one worker wins most of the keyspace: the first version of
// this function gave one worker in five 50% of all prefixes, which
// TestRendezvousIsStableAndBalanced caught.
//
// Hashing each side separately, scrambling the id's hash by an odd
// constant, and passing the combination through a real finalizer gives the
// independence rendezvous hashing assumes.
func score(prefix, id string) uint64 {
	return mix64(fnv64(prefix) ^ (fnv64(id) * 0x9E3779B97F4A7C15))
}

// Ranked returns workers ordered by rendezvous score for prefix, best
// first. Ties (practically impossible with a 64-bit hash) break by ID.
func Ranked(workers []Worker, prefix string) []Worker {
	out := make([]Worker, len(workers))
	copy(out, workers)
	sort.Slice(out, func(i, j int) bool {
		si, sj := score(prefix, out[i].ID), score(prefix, out[j].ID)
		if si != sj {
			return si > sj
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Pick chooses the worker for prefix: the best-scoring one that is below
// maxInflight, or, if all are at capacity, the best-scoring one anyway
// (queueing at the affine worker beats losing the cache). exclude is a
// worker ID to skip, used to choose a hedge target different from the
// primary. ok=false if there are no candidates.
func Pick(workers []Worker, prefix string, maxInflight int, exclude string) (Worker, bool) {
	ranked := Ranked(workers, prefix)
	var fallback *Worker
	for i := range ranked {
		w := ranked[i]
		if w.ID == exclude {
			continue
		}
		if fallback == nil {
			fallback = &ranked[i]
		}
		if maxInflight <= 0 || int(w.Inflight) < maxInflight {
			return w, true
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return Worker{}, false
}

// PickBounded is rendezvous hashing with bounded loads (Mirrokni, Thorup and
// Zadimoghaddam, "Consistent Hashing with Bounded Loads", 2018). It walks the
// same ranking as Pick but skips any worker already carrying at least
//
//	ceil(factor * (totalInflight + 1) / n)
//
// streams, i.e. more than factor times the fleet average counting this
// request. This fixes the regression Phase 5 measured: pure affinity
// balances PREFIXES, not load, so 12 prefixes hashed onto 4 workers put
// ~85% of traffic on two of them. With a bound, a hot worker's overflow
// goes to that prefix's NEXT-ranked worker. That is still deterministic
// per prefix, so the overflow builds a warm cache too, instead of being
// sprayed at random. When load is even, nothing is skipped and routing is
// exactly Pick's.
//
// MaxInflight is an absolute cap a hot spot rarely reaches; the bound here is
// relative to the fleet, which is what makes it trip on imbalance itself.
// factor must be > 1; 1.25 is the value the paper's analysis and common
// production use settle on.
func PickBounded(workers []Worker, prefix string, factor float64, exclude string) (Worker, bool) {
	n, total := 0, 0
	for _, w := range workers {
		if w.ID == exclude {
			continue
		}
		n++
		total += int(w.Inflight)
	}
	if n == 0 {
		return Worker{}, false
	}
	bound := int(math.Ceil(factor * float64(total+1) / float64(n)))
	ranked := Ranked(workers, prefix)
	var first *Worker
	for i := range ranked {
		w := &ranked[i]
		if w.ID == exclude {
			continue
		}
		if first == nil {
			first = w
		}
		if int(w.Inflight) < bound {
			return *w, true
		}
	}
	// Unreachable while factor >= 1 (some worker is at or below average),
	// but never fail a request over it.
	return *first, true
}

// PickLeastLoaded ignores the prefix: the baseline the benchmark compares
// against. Ties break by ID so it is deterministic.
func PickLeastLoaded(workers []Worker, exclude string) (Worker, bool) {
	var best *Worker
	for i := range workers {
		w := &workers[i]
		if w.ID == exclude {
			continue
		}
		if best == nil || w.Inflight < best.Inflight || (w.Inflight == best.Inflight && w.ID < best.ID) {
			best = w
		}
	}
	if best == nil {
		return Worker{}, false
	}
	return *best, true
}
