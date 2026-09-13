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
