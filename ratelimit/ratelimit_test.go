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

// countingKV counts CAS calls, to measure how much KV traffic leasing saves.
type countingKV struct {
	*memKV
	cas atomic.Int64
}

func (k *countingKV) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	k.cas.Add(1)
	return k.memKV.CAS(ctx, key, expected, expectAbsent, value)
}

// Leasing must never over-admit: leased tokens have left the shared bucket,
// so however many gateways lease concurrently, the fleet admits at most the
// budget. It may under-admit by tokens stranded in other gateways' leases,
// bounded by gateways x (lease size - 1).
func TestLeasingConservesTheBudget(t *testing.T) {
	const gateways, perGateway = 16, 50
	const burst, fraction = 400.0, 0.05 // lease size 20
	kv := newMemKV()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < gateways; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := New(kv, Options{Rate: 100, Burst: burst, Clock: clk.now, LeaseFraction: fraction})
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
	got := admitted.Load()
	if got > int64(burst) {
		t.Fatalf("leasing over-admitted: %d > budget %v", got, burst)
	}
	if minOK := int64(burst) - gateways*(20-1); got < minOK {
		t.Fatalf("leasing under-admitted beyond its bound: %d < %d", got, minOK)
	}
	t.Logf("%d gateways, budget %v, lease 20: admitted %d (stranded in leases: %d)", gateways, burst, got, int64(burst)-got)
}

// The point of leasing: most Takes never touch the KV.
func TestLeasingCutsKVTraffic(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	exact := &countingKV{memKV: newMemKV()}
	leased := &countingKV{memKV: newMemKV()}
	le := New(exact, Options{Rate: 1000, Burst: 1000, Clock: clk.now})
	ll := New(leased, Options{Rate: 1000, Burst: 1000, Clock: clk.now, LeaseFraction: 0.1})
	for i := 0; i < 500; i++ {
		if ok, _, err := le.Take(bg, "t", 1); !ok || err != nil {
			t.Fatalf("exact take %d: ok=%v err=%v", i, ok, err)
		}
		if ok, _, err := ll.Take(bg, "t", 1); !ok || err != nil {
			t.Fatalf("leased take %d: ok=%v err=%v", i, ok, err)
		}
	}
	t.Logf("500 takes: %d CAS exact, %d CAS leased", exact.cas.Load(), leased.cas.Load())
	if exact.cas.Load() != 500 {
		t.Fatalf("exact limiter did %d CAS for 500 takes, want 500", exact.cas.Load())
	}
	if leased.cas.Load() > 5 {
		t.Fatalf("leased limiter did %d CAS for 500 takes with lease 100, want 5", leased.cas.Load())
	}
}

// Unused leased tokens expire and are dropped, never handed back: after
// LeaseTTL the next Take must go back to the shared bucket.
func TestLeaseExpires(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	kv := &countingKV{memKV: newMemKV()}
	l := New(kv, Options{Rate: 1, Burst: 100, Clock: clk.now, LeaseFraction: 0.1, LeaseTTL: time.Second})
	if ok, _, _ := l.Take(bg, "t", 1); !ok {
		t.Fatal("first take refused")
	}
	if ok, _, _ := l.Take(bg, "t", 1); !ok || kv.cas.Load() != 1 {
		t.Fatalf("second take should be served from the lease: ok=%v cas=%d", ok, kv.cas.Load())
	}
	clk.advance(1500 * time.Millisecond)
	if ok, _, _ := l.Take(bg, "t", 1); !ok || kv.cas.Load() != 2 {
		t.Fatalf("after expiry the take should lease again: ok=%v cas=%d", ok, kv.cas.Load())
	}
	// Shared bucket: 100 - 10 (first lease) - 10 (second) + ~1.5 refill.
	if left, _ := l.Peek(bg, "t"); left < 81 || left > 82 {
		t.Fatalf("expired lease tokens were returned or double-counted: shared bucket has %.1f", left)
	}
}

// Tiny budgets keep exact semantics: a lease under 2 tokens is not worth
// the stranding, so the limiter falls back to one CAS per Take.
func TestTinyBudgetStaysExact(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	l := New(newMemKV(), Options{Rate: 2, Burst: 3, Clock: clk.now, LeaseFraction: 0.1})
	n := 0
	for i := 0; i < 10; i++ {
		if ok, _, _ := l.Take(bg, "t", 1); ok {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("burst 3 admitted %d with leasing configured, want exactly 3", n)
	}
}

// With one gateway nothing can be stranded elsewhere, so leasing must admit
// EXACTLY the budget: any token double-counted between the lease and the
// shared bucket shows up here. (The multi-gateway test above cannot catch a
// small over-admission: tokens stranded in other leases mask it.)
func TestLeasingSingleGatewayIsExact(t *testing.T) {
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	l := New(newMemKV(), Options{Rate: 100, Burst: 400, Clock: clk.now, LeaseFraction: 0.05})
	n := 0
	for i := 0; i < 1000; i++ {
		if ok, _, err := l.Take(bg, "t", 1); err != nil {
			t.Fatal(err)
		} else if ok {
			n++
		}
	}
	if n != 400 {
		t.Fatalf("one leasing gateway admitted %d of a 400 budget, want exactly 400", n)
	}
}

// Regression: two gateways whose clocks disagree by 200ms. refill() used to
// set lastMs = now even when now < lastMs, so the slow-clock gateway moved
// lastMs BACK and the fast-clock one then re-credited the same 200ms on its
// next Take, every time: the limit was effectively gone. Skew may shift
// WHEN tokens refill by the skew; it must not multiply the rate.
func TestClockSkewDoesNotMultiplyRate(t *testing.T) {
	const rate, burst = 10.0, 20.0
	const skew = 200 * time.Millisecond
	const simulated, step = 20 * time.Second, 10 * time.Millisecond
	for _, fraction := range []float64{0, 0.2} {
		kv := newMemKV()
		slow, fast := &fakeClock{}, &fakeClock{}
		slow.ms.Store(1_700_000_000_000)
		fast.ms.Store(1_700_000_000_000 + skew.Milliseconds())
		a := New(kv, Options{Rate: rate, Burst: burst, Clock: slow.now, LeaseFraction: fraction})
		b := New(kv, Options{Rate: rate, Burst: burst, Clock: fast.now, LeaseFraction: fraction})
		admitted := 0
		for elapsed := time.Duration(0); elapsed < simulated; elapsed += step {
			for _, l := range []*Limiter{a, b} {
				ok, _, err := l.Take(bg, "t", 1)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					admitted++
				}
			}
			slow.advance(step)
			fast.advance(step)
		}
		want := burst + rate*simulated.Seconds()
		t.Logf("lease fraction %.1f: admitted %d over %v (budget %.0f = burst + rate x time)", fraction, admitted, simulated, want)
		if float64(admitted) > 1.2*want || float64(admitted) < 0.8*want {
			t.Fatalf("lease fraction %.1f: admitted %d with %v clock skew, want within 20%% of %.0f", fraction, admitted, skew, want)
		}
	}
}
