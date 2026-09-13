package ratelimit

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memKV: mutex-guarded in-memory linearizable KV (same as sched's test fake).
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

func newLimiter(t *testing.T, rate, burst float64) (*Limiter, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	return New(newMemKV(), Options{Rate: rate, Burst: burst, Clock: clk.now}), clk
}

var bg = context.Background()

func TestBurstThenRefill(t *testing.T) {
	l, clk := newLimiter(t, 10, 5)
	admitted := 0
	for i := 0; i < 10; i++ {
		ok, _, err := l.Take(bg, "a", 1)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			admitted++
		}
	}
	if admitted != 5 {
		t.Fatalf("admitted %d at t=0, want burst=5", admitted)
	}
	ok, retry, _ := l.Take(bg, "a", 1)
	if ok || retry != 100*time.Millisecond {
		t.Fatalf("refused take: ok=%v retry=%v want 100ms (1 token at 10/s)", ok, retry)
	}
	clk.advance(300 * time.Millisecond) // 3 tokens refill
	admitted = 0
	for i := 0; i < 10; i++ {
		if ok, _, _ := l.Take(bg, "a", 1); ok {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("admitted %d after 300ms, want 3", admitted)
	}
	clk.advance(time.Hour) // refill caps at burst, not rate*3600
	if tok, _ := l.Peek(bg, "a"); tok != 5 {
		t.Fatalf("peek=%v want burst cap 5", tok)
	}
}

func TestTenantsIsolatedAndOverridable(t *testing.T) {
	clk := &fakeClock{}
	l := New(newMemKV(), Options{Rate: 1, Burst: 2, Clock: clk.now, Tenants: map[string]Limit{"vip": {Rate: 100, Burst: 50}}})
	for i := 0; i < 2; i++ {
		if ok, _, _ := l.Take(bg, "free", 1); !ok {
			t.Fatal("free should have burst 2")
		}
	}
	if ok, _, _ := l.Take(bg, "free", 1); ok {
		t.Fatal("free exhausted")
	}
	for i := 0; i < 50; i++ {
		if ok, _, _ := l.Take(bg, "vip", 1); !ok {
			t.Fatalf("vip refused at %d", i)
		}
	}
	// Another tenant's exhaustion did not touch vip, and vice versa.
	if ok, _, _ := l.Take(bg, "other", 1); !ok {
		t.Fatal("other tenant should start full")
	}
}

func TestTakeLargerThanBurstIsRefusedWithHint(t *testing.T) {
	l, _ := newLimiter(t, 10, 5)
	ok, retry, err := l.Take(bg, "a", 6)
	if err != nil || ok || retry <= 0 {
		t.Fatalf("ok=%v retry=%v err=%v", ok, retry, err)
	}
}

// The invariant that justifies putting the bucket in a shared KV: N
// "gateways" (goroutines) hammering one tenant concurrently, with the clock
// frozen, admit EXACTLY burst tokens in total. Not burst+1. Not burst per
// gateway. Then, after the clock advances, exactly the refilled amount.
func TestConcurrentGatewaysShareOneBudget(t *testing.T) {
	const gateways, perGateway = 16, 50
	const burst = 40.0
	kv := newMemKV()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	// Each goroutine gets its own Limiter instance, as separate processes would.
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < gateways; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := New(kv, Options{Rate: 100, Burst: burst, Clock: clk.now})
			for i := 0; i < perGateway; i++ {
				ok, _, err := l.Take(bg, "shared", 1)
				if err != nil {
					t.Error(err)
					return
				}
				if ok {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != int64(burst) {
		t.Fatalf("%d gateways admitted %d tokens total with the clock frozen; want exactly %v", gateways, admitted.Load(), burst)
	}

	clk.advance(200 * time.Millisecond) // 20 tokens at 100/s
	admitted.Store(0)
	for g := 0; g < gateways; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := New(kv, Options{Rate: 100, Burst: burst, Clock: clk.now})
			for i := 0; i < perGateway; i++ {
				if ok, _, _ := l.Take(bg, "shared", 1); ok {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 20 {
		t.Fatalf("after 200ms refill admitted %d, want exactly 20", admitted.Load())
	}
}
