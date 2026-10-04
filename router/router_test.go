package router

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	return bytes.Clone(v), ok, nil
}

func (k *memKV) Put(_ context.Context, key string, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = bytes.Clone(value)
	return nil
}

func (k *memKV) CAS(_ context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.m[key]
	matches := (expectAbsent && !ok) || (!expectAbsent && ok && bytes.Equal(cur, expected))
	if matches {
		k.m[key] = bytes.Clone(value)
		return true, bytes.Clone(cur), nil
	}
	return false, bytes.Clone(cur), nil
}

type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

var bg = context.Background()

func TestRegistryLeaseExpiry(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	kv := newMemKV()
	r := NewRegistry(kv, Options{Clock: clk.now, CacheTTL: time.Millisecond})

	if _, err := r.Register(bg, Worker{ID: "a", Addr: "a:1"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register(bg, Worker{ID: "b", Addr: "b:1"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	live, _ := r.Live(bg)
	if len(live) != 2 {
		t.Fatalf("live=%d want 2", len(live))
	}

	// b renews, a does not; a lapses.
	clk.advance(2 * time.Second)
	_, _ = r.Register(bg, Worker{ID: "b", Addr: "b:1", Inflight: 3}, 3*time.Second)
	clk.advance(2 * time.Second)
	live, _ = r.Live(bg)
	if len(live) != 1 || live[0].ID != "b" || live[0].Inflight != 3 {
		t.Fatalf("live=%+v want only b with inflight 3", live)
	}

	// A second registry (another gateway) over the same KV sees the same set.
	r2 := NewRegistry(kv, Options{Clock: clk.now, CacheTTL: time.Millisecond})
	live2, _ := r2.Live(bg)
	if len(live2) != 1 || live2[0].ID != "b" {
		t.Fatalf("other gateway sees %+v", live2)
	}

	if err := r.Deregister(bg, "b"); err != nil {
		t.Fatal(err)
	}
	if live, _ := r.Live(bg); len(live) != 0 {
		t.Fatalf("after deregister: %+v", live)
	}
}

// failingKV wraps memKV and fails every operation while fail is set, the way
// a KV in the middle of a leader election or behind a partition does.
type failingKV struct {
	*memKV
	fail atomic.Bool
	gets atomic.Int64
}

var errKVDown = fmt.Errorf("kv unavailable: no leader")

func (k *failingKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	k.gets.Add(1)
	if k.fail.Load() {
		return nil, false, errKVDown
	}
	return k.memKV.Get(ctx, key)
}

func (k *failingKV) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	if k.fail.Load() {
		return false, nil, errKVDown
	}
	return k.memKV.CAS(ctx, key, expected, expectAbsent, value)
}

// A KV outage must not take routing down with it: Live keeps serving the
// last live set it read, still filtered by lease, so healthy workers keep
// serving. Leases are the bound on staleness: once they lapse (workers
// cannot renew while the KV is down either), the set drains.
func TestLiveServesLastKnownSetWhenKVFails(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	kv := &failingKV{memKV: newMemKV()}
	r := NewRegistry(kv, Options{Clock: clk.now, CacheTTL: 100 * time.Millisecond})
	for _, id := range []string{"a", "b"} {
		if _, err := r.Register(bg, Worker{ID: id, Addr: id + ":1"}, 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if live, err := r.Live(bg); err != nil || len(live) != 2 {
		t.Fatalf("before outage: live=%+v err=%v", live, err)
	}

	kv.fail.Store(true)
	clk.advance(time.Second) // past CacheTTL: Live must go back to the KV
	live, err := r.Live(bg)
	if err != nil || len(live) != 2 {
		t.Fatalf("KV down, leases valid: live=%+v err=%v; want the last known 2 workers", live, err)
	}
	// While the KV is down, a failed read is not retried on every call:
	// the stale set is reused for another CacheTTL.
	before := kv.gets.Load()
	for i := 0; i < 10; i++ {
		_, _ = r.Live(bg)
	}
	if n := kv.gets.Load() - before; n != 0 {
		t.Fatalf("%d KV reads within one CacheTTL of a failed read; want 0", n)
	}

	clk.advance(3 * time.Second) // leases lapse
	if live, err := r.Live(bg); err != nil || len(live) != 0 {
		t.Fatalf("KV down, leases lapsed: live=%+v err=%v; want empty", live, err)
	}

	// A registry that never read the set has nothing to fall back on.
	r2 := NewRegistry(kv, Options{Clock: clk.now})
	if _, err := r2.Live(bg); err == nil {
		t.Fatal("no last-known set: want the KV error")
	}

	// Recovery: reads go back to the KV.
	kv.fail.Store(false)
	if _, err := r.Register(bg, Worker{ID: "c", Addr: "c:1"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if live, err := r.Live(bg); err != nil || len(live) != 1 || live[0].ID != "c" {
		t.Fatalf("after recovery: live=%+v err=%v", live, err)
	}
}

func TestConcurrentRegistrationsAllLand(t *testing.T) {
	clk := &fakeClock{}
	kv := newMemKV()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := NewRegistry(kv, Options{Clock: clk.now})
			if _, err := r.Register(bg, Worker{ID: fmt.Sprintf("w%02d", i)}, time.Minute); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	r := NewRegistry(kv, Options{Clock: clk.now})
	live, _ := r.Live(bg)
	if len(live) != 20 {
		t.Fatalf("live=%d want 20: a CAS race lost a registration", len(live))
	}
}

func workers(n int) []Worker {
	ws := make([]Worker, n)
	for i := range ws {
		ws[i] = Worker{ID: fmt.Sprintf("w%d", i), LeaseUntilMs: 1 << 62}
	}
	return ws
}

func TestRendezvousIsStableAndBalanced(t *testing.T) {
	ws := workers(4)
	counts := map[string]int{}
	for i := 0; i < 2000; i++ {
		p := fmt.Sprintf("system prompt number %d: you are a helpful assistant", i)
		a, _ := Pick(ws, p, 0, "")
		b, _ := Pick(ws, p, 0, "")
		if a.ID != b.ID {
			t.Fatalf("placement not stable for %q", p)
		}
		counts[a.ID]++
	}
	// Tight bounds on purpose: +/-20% of the 500 expected. A weak hash
	// passes a loose bound and still skews badly in production.
	for id, c := range counts {
		if c < 400 || c > 600 {
			t.Fatalf("worker %s got %d of 2000 prefixes; expected roughly 500 each: %v", id, c, counts)
		}
	}
	t.Logf("distribution over 4 workers: %v", counts)
}

// Removing one worker must move only the prefixes it owned. That is the
// property that makes rendezvous hashing worth the name: with hash mod n,
// removing a worker reshuffles nearly everything.
func TestRendezvousMinimalDisruption(t *testing.T) {
	all := workers(5)
	without := append([]Worker{}, all[:4]...) // drop w4
	moved, movedFromRemoved, total := 0, 0, 3000
	for i := 0; i < total; i++ {
		p := fmt.Sprintf("prefix-%d", i)
		before, _ := Pick(all, p, 0, "")
		after, _ := Pick(without, p, 0, "")
		if before.ID != after.ID {
			moved++
			if before.ID == "w4" {
				movedFromRemoved++
			}
		}
	}
	if moved != movedFromRemoved {
		t.Fatalf("%d prefixes moved but only %d were on the removed worker: unrelated prefixes were reshuffled", moved, movedFromRemoved)
	}
	frac := float64(moved) / float64(total)
	if frac < 0.15 || frac > 0.25 {
		t.Fatalf("moved fraction %.3f, want about 1/5", frac)
	}
	t.Logf("removing 1 of 5 workers moved %.1f%% of prefixes, all of them from the removed worker", 100*frac)
}

func TestPickIsLoadAwareAndCanExclude(t *testing.T) {
	ws := workers(3)
	p := "the shared system prompt"
	ranked := Ranked(ws, p)
	first, _ := Pick(ws, p, 4, "")
	if first.ID != ranked[0].ID {
		t.Fatalf("idle pick %s != top-ranked %s", first.ID, ranked[0].ID)
	}
	// Saturate the affine worker: placement spills to the runner-up.
	for i := range ws {
		if ws[i].ID == ranked[0].ID {
			ws[i].Inflight = 4
		}
	}
	second, _ := Pick(ws, p, 4, "")
	if second.ID != ranked[1].ID {
		t.Fatalf("busy pick %s != runner-up %s", second.ID, ranked[1].ID)
	}
	// Excluding the primary gives the hedge target: the best OTHER worker.
	hedge, _ := Pick(ws, p, 0, ranked[0].ID)
	if hedge.ID != ranked[1].ID {
		t.Fatalf("hedge pick %s != runner-up %s", hedge.ID, ranked[1].ID)
	}
	// All saturated: still returns the affine worker rather than nothing.
	for i := range ws {
		ws[i].Inflight = 9
	}
	sat, ok := Pick(ws, p, 4, "")
	if !ok || sat.ID != ranked[0].ID {
		t.Fatalf("saturated pick %v %v", sat, ok)
	}
	if _, ok := Pick(nil, p, 4, ""); ok {
		t.Fatal("no workers must give ok=false")
	}
}

func TestPickLeastLoaded(t *testing.T) {
	ws := workers(3)
	ws[0].Inflight, ws[1].Inflight, ws[2].Inflight = 5, 1, 1
	w, _ := PickLeastLoaded(ws, "")
	if w.ID != "w1" {
		t.Fatalf("got %s want w1 (tie broken by id)", w.ID)
	}
	w, _ = PickLeastLoaded(ws, "w1")
	if w.ID != "w2" {
		t.Fatalf("excluding w1 got %s want w2", w.ID)
	}
}

func TestPrefixIsRuneSafe(t *testing.T) {
	if got := Prefix("héllo wörld", 5); got != "héllo" {
		t.Fatalf("%q", got)
	}
	if got := Prefix("short", 10); got != "short" {
		t.Fatalf("%q", got)
	}
}

func fleet(n int) []Worker {
	ws := make([]Worker, n)
	for i := range ws {
		ws[i] = Worker{ID: fmt.Sprintf("w%d", i)}
	}
	return ws
}

// With even load the bound never trips, so bounded-load routing must be
// exactly plain affinity: the cache benefit is untouched when nothing is hot.
func TestPickBoundedEqualsPickWhenBalanced(t *testing.T) {
	ws := fleet(4)
	for i := range ws {
		ws[i].Inflight = 3
	}
	for p := 0; p < 200; p++ {
		prefix := fmt.Sprintf("system prompt %d", p)
		a, _ := Pick(ws, prefix, 0, "")
		b, _ := PickBounded(ws, prefix, 1.25, "")
		if a.ID != b.ID {
			t.Fatalf("prefix %q: Pick=%s PickBounded=%s with even load", prefix, a.ID, b.ID)
		}
	}
}

// A hot preferred worker is skipped in favour of the prefix's NEXT-ranked
// worker (not a random or least-loaded one), so the overflow is sticky too.
func TestPickBoundedSpillsToNextRanked(t *testing.T) {
	ws := fleet(4)
	prefix := "a hot system prompt"
	ranked := Ranked(ws, prefix)
	for i := range ws {
		if ws[i].ID == ranked[0].ID {
			ws[i].Inflight = 10 // far above 1.25 x average
		}
	}
	got, _ := PickBounded(ws, prefix, 1.25, "")
	if got.ID != ranked[1].ID {
		t.Fatalf("spilled to %s, want the next-ranked worker %s", got.ID, ranked[1].ID)
	}
}

// The regression this exists to fix: a skewed prefix mix piles onto a few
// workers under pure affinity. Place requests one at a time (no completions)
// and compare the busiest worker under each policy.
func TestPickBoundedCapsTheHotSpot(t *testing.T) {
	const n, requests, factor = 4, 400, 1.25
	// 12 prefixes with Zipf-ish popularity: prefix k gets weight 12-k.
	var arrivals []string
	for k := 0; k < 12; k++ {
		for j := 0; j < 12-k; j++ {
			arrivals = append(arrivals, fmt.Sprintf("system prompt #%d", k))
		}
	}
	place := func(bounded bool) []int32 {
		ws := fleet(n)
		for i := 0; i < requests; i++ {
			prefix := arrivals[i%len(arrivals)]
			var w Worker
			if bounded {
				w, _ = PickBounded(ws, prefix, factor, "")
			} else {
				w, _ = Pick(ws, prefix, 0, "")
			}
			for j := range ws {
				if ws[j].ID == w.ID {
					ws[j].Inflight++
				}
			}
		}
		loads := make([]int32, n)
		for i := range ws {
			loads[i] = ws[i].Inflight
		}
		return loads
	}
	maxOf := func(xs []int32) int32 {
		m := xs[0]
		for _, x := range xs {
			m = max(m, x)
		}
		return m
	}
	plain, bounded := place(false), place(true)
	t.Logf("pure affinity loads %v (max %d); bounded %v (max %d); bound %d",
		plain, maxOf(plain), bounded, maxOf(bounded), int32(factor*requests/n)+1)
	if limit := int32(factor*requests/n) + 1; maxOf(bounded) > limit {
		t.Fatalf("bounded-load max %d exceeds %d", maxOf(bounded), limit)
	}
	if maxOf(bounded) >= maxOf(plain) {
		t.Fatalf("bounded-load did not reduce the hot spot: %d vs %d", maxOf(bounded), maxOf(plain))
	}
}
