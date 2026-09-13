// Package mock is an inference worker that behaves like a real one without
// a GPU: it models prefill cost, a prefix (KV) cache, per-token decode
// latency, occasional stalls, and cancellation. Everything in Phase 5 that
// is worth measuring (prefix-cache hit rate, time to first token, the tail
// that hedging exists to cut) is visible with this worker, and the numbers
// mean the same thing they would with a model behind them.
//
// The latency model, kept deliberately simple and matched exactly by
// py/infer_worker.py so a benchmark can mix Go and Python workers:
//
//	prompt is split into 64-char blocks
//	the worker remembers the hash of every prefix-of-blocks it has prefilled
//	cachedChars = 64 * (longest leading run of blocks it already knows)
//	prefillMs   = BasePrefillMs + PrefillPerChar * (len(prompt) - cachedChars)
//	then one token every TokenMs
//
// That is the shape real prefix caching has: prefill is roughly linear in
// the number of NEW tokens, and a shared system prompt makes it nearly
// free. Decode is unaffected, which is also true in practice.
package mock

import (
	"container/list"
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	inferv1 "dsys/gen/infer/v1"
)

// BlockChars is the prefix-cache block size, in characters.
const BlockChars = 64

// Options configures a mock worker.
type Options struct {
	ID    string
	Model string
	// BasePrefillMs is the fixed cost of a forward pass.
	BasePrefillMs float64 // default 20
	// PrefillPerChar is the marginal cost per uncached prompt character.
	PrefillPerChar float64 // default 0.25
	// TokenMs is the inter-token decode delay.
	TokenMs float64 // default 12
	// CacheBlocks bounds the prefix cache (LRU over block-prefix hashes).
	CacheBlocks int // default 512
	// StallProb / StallMs inject a tail: with probability StallProb, add
	// StallMs to prefill. This is what hedging is measured against.
	StallProb float64
	StallMs   float64 // default 400
	// Seed makes stalls reproducible. 0 uses a fixed seed.
	Seed int64
	// Sleep is the delay function; tests inject a no-op or a fake.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Stats are cumulative counters.
type Stats struct {
	Requests       uint64
	Cancelled      uint64
	PrefixHits     uint64
	TokensEmitted  uint64
	Stalls         uint64
	CachedBlockLen int
}

// Worker implements inferv1.InferenceServer.
type Worker struct {
	inferv1.UnimplementedInferenceServer
	opts Options

	mu    sync.Mutex
	lru   *list.List               // front = most recently used; values are uint64 hashes
	index map[uint64]*list.Element // hash -> element
	rng   *rand.Rand

	inflight      atomic.Int32
	requests      atomic.Uint64
	cancelled     atomic.Uint64
	prefixHits    atomic.Uint64
	tokensEmitted atomic.Uint64
	stalls        atomic.Uint64
}

// New returns a mock worker.
func New(opts Options) *Worker {
	if opts.ID == "" {
		opts.ID = "mock"
	}
	if opts.Model == "" {
		opts.Model = "mock-1b"
	}
	if opts.BasePrefillMs == 0 {
		opts.BasePrefillMs = 20
	}
	if opts.PrefillPerChar == 0 {
		opts.PrefillPerChar = 0.25
	}
	if opts.TokenMs == 0 {
		opts.TokenMs = 12
	}
	if opts.CacheBlocks == 0 {
		opts.CacheBlocks = 512
	}
	if opts.StallMs == 0 {
		opts.StallMs = 400
	}
	if opts.Seed == 0 {
		opts.Seed = 1
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepCtx
	}
	return &Worker{
		opts:  opts,
		lru:   list.New(),
		index: map[uint64]*list.Element{},
		rng:   rand.New(rand.NewSource(opts.Seed)),
	}
}

// sleepCtx sleeps for d, returning early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ID returns the worker's id.
func (w *Worker) ID() string { return w.opts.ID }

// Inflight returns the number of in-progress generations.
func (w *Worker) Inflight() int32 { return w.inflight.Load() }

// Stats returns counters.
func (w *Worker) Stats() Stats {
	w.mu.Lock()
	n := w.lru.Len()
	w.mu.Unlock()
	return Stats{
		Requests:       w.requests.Load(),
		Cancelled:      w.cancelled.Load(),
		PrefixHits:     w.prefixHits.Load(),
		TokensEmitted:  w.tokensEmitted.Load(),
		Stalls:         w.stalls.Load(),
		CachedBlockLen: n,
	}
}

// blockHashes returns the cumulative hash of blocks[0..i] for each FULL
// block i. A trailing partial block is deliberately not cached: real
// paged-attention caches work in whole blocks, and counting a partial block
// would let a one-character difference at the end of a prompt claim a full
// block of reuse. py/infer_worker.py makes the same choice, so the two
// workers report identical cached_prefix_chars for the same prompt and a
// benchmark can mix them.
func blockHashes(prompt string) []uint64 {
	nBlocks := len(prompt) / BlockChars
	out := make([]uint64, 0, nBlocks)
	h := fnv.New64a()
	for i := 0; i < nBlocks; i++ {
		_, _ = h.Write([]byte(prompt[i*BlockChars : (i+1)*BlockChars]))
		out = append(out, h.Sum64())
	}
	return out
}

// lookupPrefix returns how many leading blocks are already cached.
func (w *Worker) lookupPrefix(hashes []uint64) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, hsh := range hashes {
		el, ok := w.index[hsh]
		if !ok {
			break
		}
		w.lru.MoveToFront(el)
		n++
	}
	return n
}

// insertPrefix records every cumulative block hash of this prompt.
func (w *Worker) insertPrefix(hashes []uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, hsh := range hashes {
		if el, ok := w.index[hsh]; ok {
			w.lru.MoveToFront(el)
			continue
		}
		el := w.lru.PushFront(hsh)
		w.index[hsh] = el
		for w.lru.Len() > w.opts.CacheBlocks {
			back := w.lru.Back()
			if back == nil {
				break
			}
			w.lru.Remove(back)
			delete(w.index, back.Value.(uint64))
		}
	}
}

var words = []string{
	"the", "model", "returns", "a", "sequence", "of", "plausible", "tokens",
	"because", "attention", "over", "the", "prompt", "produces", "context",
	"and", "sampling", "selects", "each", "next", "word", "in", "turn",
}

// token returns a deterministic pseudo-word for (prompt, index).
func token(promptHash uint64, i int) string {
	x := promptHash ^ (uint64(i+1) * 0x9E3779B97F4A7C15)
	x ^= x >> 29
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 32
	return words[x%uint64(len(words))] + " "
}

// Generate streams tokens, honouring cancellation on every token.
func (w *Worker) Generate(req *inferv1.GenerateRequest, stream inferv1.Inference_GenerateServer) error {
	ctx := stream.Context()
	w.requests.Add(1)
	w.inflight.Add(1)
	defer w.inflight.Add(-1)

	maxTokens := int(req.MaxTokens)
	if maxTokens <= 0 {
		maxTokens = 64
	}

	hashes := blockHashes(req.Prompt)
	cachedBlocks := w.lookupPrefix(hashes)
	cachedChars := min(cachedBlocks*BlockChars, len(req.Prompt))
	if cachedChars > 0 {
		w.prefixHits.Add(1)
	}

	prefill := w.opts.BasePrefillMs + w.opts.PrefillPerChar*float64(len(req.Prompt)-cachedChars)
	w.mu.Lock()
	stalled := w.opts.StallProb > 0 && w.rng.Float64() < w.opts.StallProb
	w.mu.Unlock()
	if stalled {
		w.stalls.Add(1)
		prefill += w.opts.StallMs
	}

	// Prefill. Cancellation here is the common case for a hedge loser.
	if err := w.opts.Sleep(ctx, time.Duration(prefill*float64(time.Millisecond))); err != nil {
		w.cancelled.Add(1)
		return ctx.Err()
	}
	w.insertPrefix(hashes)

	ph := fnv.New64a()
	_, _ = ph.Write([]byte(req.Prompt))
	promptHash := ph.Sum64()

	for i := 0; i < maxTokens; i++ {
		// Check before every token: tokens after a cancel are waste.
		if ctx.Err() != nil {
			w.cancelled.Add(1)
			return ctx.Err()
		}
		tok := &inferv1.Token{Text: token(promptHash, i), Index: int32(i)}
		if i == 0 {
			tok.PrefillMs = int64(math.Round(prefill))
			tok.PrefixCacheHit = cachedChars > 0
			tok.CachedPrefixChars = int32(cachedChars)
		}
		if i == maxTokens-1 {
			tok.Done = true
			tok.FinishReason = "length"
		}
		if err := stream.Send(tok); err != nil {
			return err
		}
		w.tokensEmitted.Add(1)
		if i < maxTokens-1 {
			if err := w.opts.Sleep(ctx, time.Duration(w.opts.TokenMs*float64(time.Millisecond))); err != nil {
				w.cancelled.Add(1)
				return ctx.Err()
			}
		}
	}
	return nil
}

// Health reports load.
func (w *Worker) Health(context.Context, *inferv1.HealthRequest) (*inferv1.HealthResponse, error) {
	return &inferv1.HealthResponse{
		WorkerId: w.opts.ID,
		Inflight: w.inflight.Load(),
		Model:    w.opts.Model,
	}, nil
}

// String is handy in test failure messages.
func (w *Worker) String() string { return fmt.Sprintf("mock(%s)", w.opts.ID) }
