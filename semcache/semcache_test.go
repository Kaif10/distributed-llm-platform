package semcache

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// memKV: an in-memory linearizable KV, copied from sched/queue_test.go. One
// mutex makes every operation atomic, which is exactly the guarantee the
// real store gives (through Raft).
// ---------------------------------------------------------------------------

type memKV struct {
	mu   sync.Mutex
	m    map[string][]byte
	gets atomic.Int64
	cas  atomic.Int64
}

func newMemKV() *memKV { return &memKV{m: make(map[string][]byte)} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.gets.Add(1)
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
	k.cas.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.m[key]
	var matches bool
	if expectAbsent {
		matches = !ok
	} else {
		matches = ok && bytes.Equal(cur, expected)
	}
	if matches {
		k.m[key] = bytes.Clone(value)
		return true, bytes.Clone(cur), nil
	}
	return false, bytes.Clone(cur), nil
}

// delete simulates eviction by an external actor (a KV GC, a lost shard).
func (k *memKV) delete(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, key)
}

// fakeClock lets tests move time without sleeping.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}
func (c *fakeClock) now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

var bg = context.Background()

func newTestCache(kv *memKV, opts Options) (*Cache, *fakeClock) {
	clk := newFakeClock()
	opts.Clock = clk.now
	return New(kv, NewNGramEmbedder(0), opts), clk
}

const (
	promptA    = "Summarize the following article about climate policy."
	promptA2   = "Summarise the following article about climate policy" // spelling variant, no period
	promptFar  = "Write a haiku about cats"
	answerA    = "The article argues that carbon pricing ..."
	answerHaik = "Soft paws on the sill ..."
)

// ---------------------------------------------------------------------------
// Exact path
// ---------------------------------------------------------------------------

func TestExactHitAfterStore(t *testing.T) {
	c, _ := newTestCache(newMemKV(), Options{})

	if _, hit, err := c.Lookup(bg, promptA); err != nil || hit {
		t.Fatalf("before store: hit=%v err=%v", hit, err)
	}
	if err := c.Store(bg, promptA, answerA); err != nil {
		t.Fatal(err)
	}
	text, hit, err := c.Lookup(bg, promptA)
	if err != nil || !hit || text != answerA {
		t.Fatalf("after store: text=%q hit=%v err=%v", text, hit, err)
	}
	// Normalisation: case and whitespace do not change the exact key.
	text, hit, _ = c.Lookup(bg, "  summarize THE following   article about climate policy. ")
	if !hit || text != answerA {
		t.Fatalf("normalised variant: text=%q hit=%v", text, hit)
	}

	s := c.Stats()
	if s.Lookups != 3 || s.ExactHits != 2 || s.NearHits != 0 || s.Misses != 1 || s.Stores != 1 || s.LocalVectors != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

// ---------------------------------------------------------------------------
// Near path
// ---------------------------------------------------------------------------

func TestNearHit(t *testing.T) {
	c, _ := newTestCache(newMemKV(), Options{})
	emb := c.emb

	// Make the threshold choice visible: log the raw similarities.
	va, _ := emb.Embed(bg, promptA)
	va2, _ := emb.Embed(bg, promptA2)
	vf, _ := emb.Embed(bg, promptFar)
	simVariant := Cosine(va, va2)
	simFar := Cosine(va, vf)
	t.Logf("cosine(%q, %q) = %.4f", promptA, promptA2, simVariant)
	t.Logf("cosine(%q, %q) = %.4f", promptA, promptFar, simFar)
	t.Logf("threshold = %.2f", c.Options().Threshold)

	if err := c.Store(bg, promptA, answerA); err != nil {
		t.Fatal(err)
	}
	text, hit, err := c.Lookup(bg, promptA2)
	if err != nil || !hit || text != answerA {
		t.Fatalf("spelling variant: text=%q hit=%v err=%v (sim %.4f)", text, hit, err, simVariant)
	}
	if _, hit, _ := c.Lookup(bg, promptFar); hit {
		t.Fatalf("unrelated prompt hit (sim %.4f)", simFar)
	}
	s := c.Stats()
	if s.NearHits != 1 || s.Misses != 1 || s.ExactHits != 0 {
		t.Fatalf("stats: %+v", s)
	}
}

// Regression: two DIFFERENT questions behind the same long system prompt
// embed at cosine ~0.93, above Threshold, because n-gram cosine is
// dominated by the shared text. Before the near-duplicate check, the second
// question was served the first one's answer. A genuine variant of the long
// prompt (a typo in the question) must still hit.
func TestNearHitRejectsDifferentQuestionSharingSystemPrompt(t *testing.T) {
	c, _ := newTestCache(newMemKV(), Options{})
	sys := "You are a helpful teaching assistant for a university course on distributed systems. " +
		"You explain consensus, replication, sharding, leases and fault tolerance clearly and " +
		"accurately, in plain English, in at most two short sentences.\n\n"
	q1 := sys + "User: What does the leader do in the Raft consensus algorithm?\nAssistant:"
	q2 := sys + "User: Why does a distributed lock need a fencing token?\nAssistant:"
	q1typo := sys + "User: What does the leader do in the Raft concensus algorithm?\nAssistant:"

	v1, _ := c.emb.Embed(bg, normalise(q1))
	v2, _ := c.emb.Embed(bg, normalise(q2))
	sim := Cosine(v1, v2)
	t.Logf("cosine(q1, q2) = %.4f, threshold = %.2f", sim, c.Options().Threshold)
	if sim < c.Options().Threshold {
		t.Logf("note: embedder no longer puts these above threshold; the check below still guards it")
	}

	if err := c.Store(bg, q1, "the leader replicates the log"); err != nil {
		t.Fatal(err)
	}
	if text, hit, _ := c.Lookup(bg, q2); hit {
		t.Fatalf("different question served a cached answer: %q", text)
	}
	if text, hit, _ := c.Lookup(bg, q1typo); !hit || text != "the leader replicates the log" {
		t.Fatalf("typo variant of a long prompt missed: hit=%v text=%q", hit, text)
	}
	if s := c.Stats(); s.NearHits != 1 || s.Lookups != s.ExactHits+s.NearHits+s.Misses {
		t.Fatalf("stats: %+v", s)
	}
}

func TestEditDistanceWithin(t *testing.T) {
	cases := []struct {
		a, b string
		k    int
		want bool
	}{
		{"", "", 0, true},
		{"abc", "abc", 0, true},
		{"kitten", "sitting", 3, true},
		{"kitten", "sitting", 2, false},
		{"summarize", "summarise", 1, true},
		{"abc", "", 3, true},
		{"abcd", "", 3, false},
		{"flaw", "lawn", 2, true},
	}
	for _, tc := range cases {
		if got := editDistanceWithin(tc.a, tc.b, tc.k); got != tc.want {
			t.Errorf("editDistanceWithin(%q, %q, %d) = %v, want %v", tc.a, tc.b, tc.k, got, tc.want)
		}
		if got := editDistanceWithin(tc.b, tc.a, tc.k); got != tc.want {
			t.Errorf("editDistanceWithin(%q, %q, %d) = %v, want %v (swapped)", tc.b, tc.a, tc.k, got, tc.want)
		}
	}
}

// A near hit whose KV entry has vanished (evicted, GC'd) is a miss, and the
// dangling index entry is dropped so the scan does not keep finding it.
func TestNearHitDanglingKeyIsMiss(t *testing.T) {
	kv := newMemKV()
	c, _ := newTestCache(kv, Options{})
	if err := c.Store(bg, promptA, answerA); err != nil {
		t.Fatal(err)
	}
	kv.delete(c.keyExact(normalise(promptA)))

	if _, hit, _ := c.Lookup(bg, promptA2); hit {
		t.Fatal("hit on a key the KV no longer holds")
	}
	if n := c.Stats().LocalVectors; n != 0 {
		t.Fatalf("dangling index entry not dropped: LocalVectors=%d", n)
	}
}

// ---------------------------------------------------------------------------
// Two gateways over one KV
// ---------------------------------------------------------------------------

func TestTwoGatewaysShareExactNotNear(t *testing.T) {
	kv := newMemKV()
	a, _ := newTestCache(kv, Options{})
	b, _ := newTestCache(kv, Options{})

	if err := a.Store(bg, promptA, answerA); err != nil {
		t.Fatal(err)
	}

	// Shared metadata: B sees A's Store on the exact path immediately.
	text, hit, err := b.Lookup(bg, promptA)
	if err != nil || !hit || text != answerA {
		t.Fatalf("B exact lookup: text=%q hit=%v err=%v", text, hit, err)
	}

	// Local index: B has now indexed promptA (learned from the exact hit),
	// so the variant is a near hit on B too. Use a third replica that has
	// seen nothing to show the documented miss.
	c, _ := newTestCache(kv, Options{})
	if _, hit, _ := c.Lookup(bg, promptA2); hit {
		t.Fatal("C near lookup hit with an empty local index; vectors are not shared")
	}
	if c.Stats().LocalVectors != 0 {
		t.Fatalf("C indexed something: %+v", c.Stats())
	}

	// B learned from its exact hit, so its near path now works.
	text, hit, _ = b.Lookup(bg, promptA2)
	if !hit || text != answerA {
		t.Fatalf("B near lookup after learning: text=%q hit=%v", text, hit)
	}
	if s := b.Stats(); s.ExactHits != 1 || s.NearHits != 1 || s.LocalVectors != 1 {
		t.Fatalf("B stats: %+v", s)
	}

	// And once C serves an exact hit itself, it learns too.
	if _, hit, _ := c.Lookup(bg, promptA); !hit {
		t.Fatal("C exact lookup missed")
	}
	if _, hit, _ := c.Lookup(bg, promptA2); !hit {
		t.Fatal("C near lookup missed after learning")
	}
}

// ---------------------------------------------------------------------------
// TTL
// ---------------------------------------------------------------------------

func TestTTLExpiry(t *testing.T) {
	kv := newMemKV()
	c, clk := newTestCache(kv, Options{TTL: time.Minute})

	if err := c.Store(bg, promptA, answerA); err != nil {
		t.Fatal(err)
	}
	clk.advance(59 * time.Second)
	if _, hit, _ := c.Lookup(bg, promptA); !hit {
		t.Fatal("fresh entry missed")
	}
	if _, hit, _ := c.Lookup(bg, promptA2); !hit {
		t.Fatal("fresh near entry missed")
	}

	clk.advance(2 * time.Second) // 61s > TTL
	if _, hit, _ := c.Lookup(bg, promptA); hit {
		t.Fatal("expired exact entry hit")
	}
	if _, hit, _ := c.Lookup(bg, promptA2); hit {
		t.Fatal("expired near entry hit")
	}
	if n := c.Stats().LocalVectors; n != 0 {
		t.Fatalf("expired key still indexed: LocalVectors=%d", n)
	}

	// A fresh Store overwrites the stale entry and it is live again.
	if err := c.Store(bg, promptA, "updated"); err != nil {
		t.Fatal(err)
	}
	text, hit, _ := c.Lookup(bg, promptA2)
	if !hit || text != "updated" {
		t.Fatalf("after re-store: text=%q hit=%v", text, hit)
	}
}

// ---------------------------------------------------------------------------
// Bounded local index
// ---------------------------------------------------------------------------

func TestMaxLocalEviction(t *testing.T) {
	const maxLocal = 200
	c, _ := newTestCache(newMemKV(), Options{MaxLocal: maxLocal})
	for i := 0; i < maxLocal+50; i++ {
		if err := c.Store(bg, fmt.Sprintf("prompt number %d about topic %d", i, i*7), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if n := c.Stats().LocalVectors; n != maxLocal {
		t.Fatalf("LocalVectors=%d want %d", n, maxLocal)
	}
	// FIFO: the first 50 were evicted from the index but are still in the
	// KV, so they remain exact hits; the newest is still a near hit.
	if _, hit, _ := c.Lookup(bg, "prompt number 0 about topic 0"); !hit {
		t.Fatal("evicted-from-index prompt should still be an exact hit")
	}
	if !c.opts.Index.Has(c.keyExact(normalise(fmt.Sprintf("prompt number %d about topic %d", maxLocal+49, (maxLocal+49)*7)))) {
		t.Fatal("newest insertion evicted")
	}
	if c.opts.Index.Has(c.keyExact(normalise("prompt number 1 about topic 7"))) {
		t.Fatal("oldest insertion not evicted")
	}
	// The exact hit above re-indexed prompt 0, evicting one more; still bounded.
	if n := c.Stats().LocalVectors; n != maxLocal {
		t.Fatalf("after re-learn LocalVectors=%d want %d", n, maxLocal)
	}
}

// ---------------------------------------------------------------------------
// Embedder
// ---------------------------------------------------------------------------

func TestNGramEmbedderDeterministicAndUnit(t *testing.T) {
	e := NewNGramEmbedder(0)
	if e.Dim() != DefaultDim {
		t.Fatalf("Dim=%d", e.Dim())
	}
	v1, _ := e.Embed(bg, promptA)
	v2, _ := e.Embed(bg, promptA)
	if len(v1) != e.Dim() {
		t.Fatalf("len=%d", len(v1))
	}
	for i := range v1 {
		if v1[i] != v2[i] {
			t.Fatalf("non-deterministic at %d: %v vs %v", i, v1[i], v2[i])
		}
	}
	var sum float64
	for _, x := range v1 {
		sum += float64(x) * float64(x)
	}
	if norm := math.Sqrt(sum); math.Abs(norm-1) > 1e-5 {
		t.Fatalf("L2 norm = %v, want 1", norm)
	}
	if got := Cosine(v1, v2); math.Abs(float64(got)-1) > 1e-5 {
		t.Fatalf("self cosine = %v", got)
	}
	// Normalisation is part of the embedding: case/whitespace variants are identical.
	v3, _ := e.Embed(bg, "  SUMMARIZE the   following article about climate policy.")
	if got := Cosine(v1, v3); math.Abs(float64(got)-1) > 1e-5 {
		t.Fatalf("normalised variant cosine = %v", got)
	}
	// Empty text is the zero vector, not NaN.
	z, _ := e.Embed(bg, "")
	for _, x := range z {
		if x != 0 {
			t.Fatal("empty text is not the zero vector")
		}
	}
}

// ---------------------------------------------------------------------------
// Concurrency (meaningful under -race)
// ---------------------------------------------------------------------------

func TestConcurrentStoreLookup(t *testing.T) {
	kv := newMemKV()
	a, _ := newTestCache(kv, Options{MaxLocal: 64})
	b, _ := newTestCache(kv, Options{MaxLocal: 64})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p := fmt.Sprintf("worker %d asks question %d", g, i%40)
				if i%3 == 0 {
					if err := a.Store(bg, p, "ans"); err != nil {
						t.Error(err)
					}
				}
				if _, _, err := b.Lookup(bg, p); err != nil {
					t.Error(err)
				}
				if _, _, err := a.Lookup(bg, p+"?"); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	for _, c := range []*Cache{a, b} {
		s := c.Stats()
		if s.Lookups != s.ExactHits+s.NearHits+s.Misses {
			t.Fatalf("counters do not add up: %+v", s)
		}
		if s.LocalVectors > 64 {
			t.Fatalf("index unbounded: %+v", s)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmark: the brute-force scan cost at 5000 indexed vectors
// ---------------------------------------------------------------------------

func BenchmarkLookupNear(b *testing.B) {
	const n = 5000
	c, _ := newTestCache(newMemKV(), Options{MaxLocal: n})
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("Explain concept %d of distributed systems in simple terms, please, with an example about topic %d.", i, i*31%997)
		if err := c.Store(bg, p, "answer"); err != nil {
			b.Fatal(err)
		}
	}
	// A spelling variant of one stored prompt: misses the exact path, so
	// every iteration embeds and scans all n vectors, then fetches the hit.
	query := "Explain concept 2500 of distributed systems in simple terms please, with an example about topic " + fmt.Sprint(2500*31%997)
	if _, hit, _ := c.Lookup(bg, query); !hit {
		b.Fatal("benchmark query should be a near hit")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, hit, err := c.Lookup(bg, query); err != nil || !hit {
			b.Fatalf("hit=%v err=%v", hit, err)
		}
	}
}
