// Package semcache is a semantic cache for LLM completions: given a prompt
// that is identical, or close enough, to one answered before, return the
// earlier answer and skip the GPU. It sits behind the gateway's rate limiter
// (a tenant over quota should not even get a lookup) and in front of routing
// (a hit costs one KV read and no worker).
//
// # The split: shared metadata in the KV, vectors local
//
// The cache has two halves and they are deliberately in different places.
//
//	exact path   <prefix>/exact/<sha256(normalised prompt)> -> entry   in the KV, shared
//	near path    vector -> that same exact key                          in memory, per replica
//
// The exact path is what makes the cache a single cache rather than N
// caches: the moment any gateway replica Stores an answer, every replica's
// next Lookup of the same prompt is a hit, because they all read the same
// linearizable KV. A hit here needs no embedding at all, just one hash and
// one Get.
//
// The near path exists to catch "Summarise" versus "Summarize". It needs a
// vector per cached prompt and a similarity search over all of them, and
// neither belongs in the KV: vectors are large relative to the rest of the
// record, similarity search is not a key lookup, and pushing a brute-force
// scan through a Raft-replicated store would cost a linearizable read per
// candidate. So the vector index is local. The price is stated plainly:
// a replica can only find near-duplicates of prompts it has itself seen,
// either because it Stored them or because it served an exact hit for them
// (both add the vector to the local index, which is how a replica learns
// over time). Right after a new replica starts, its near path finds nothing;
// its exact path is already fully shared. TestTwoGatewaysShareExactNotNear
// pins both halves of that behaviour.
//
// # Why Store uses Put, not CAS
//
// Every other KV-backed component in this project (the scheduler's queue,
// the rate limiter) uses read-then-CAS because two writers racing on one key
// can corrupt a counter or double-admit a job. A cache entry has no such
// invariant: if two replicas answer the same prompt at the same time and
// both Store, either answer is a valid answer for that prompt, and the loser
// simply overwrote a cached completion with another cached completion. Last
// writer wins is the correct semantics, so a plain Put is not a shortcut, it
// is the honest choice; a CAS loop would only add round trips to protect
// nothing.
//
// # Why brute force
//
// Nearest is a linear scan over at most MaxLocal unit vectors, one dot
// product each. BenchmarkLookupNear measures a full near-path Lookup (embed,
// scan 5000 vectors of Dim=512, confirming KV read) at ~1.4ms on one laptop
// core; MaxLocal=10000 is ~3ms. An LLM generation is hundreds to thousands
// of milliseconds, so the scan is noise on the path it protects. An
// approximate index (HNSW, IVF) would be the change to make once MaxLocal is
// in the millions; Index is an interface precisely so that swap does not
// touch the cache logic.
//
// # Store is on the critical path only after the last token
//
// Store runs after generation is complete, so its embedding call adds
// latency to the tail of the response, not the head. That is acceptable for
// the default embedder (microseconds). A production embedder is a model
// call, and the right shape then is: Put the exact entry synchronously
// (cheap, and it is what other replicas need) and embed plus index in the
// background. The split into two independent halves is what makes that
// future change local to Store.
//
// # TTL
//
// Every entry carries the wall-clock millisecond it was stored. With a TTL
// set, an entry older than that is a miss on either path; the near path also
// drops the stale key from the local index so it stops being found. Stale
// entries are not deleted from the KV, they are overwritten by the next Store
// of that prompt, which keeps Lookup free of writes. A GC of the exact
// namespace is the natural exercise.
package semcache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"dsys/kvapi"
)

// Options configures a Cache.
type Options struct {
	// Prefix namespaces every key. Empty means "semcache".
	Prefix string
	// Threshold is the minimum cosine similarity for a near hit. 0 means
	// 0.92, which with NGramEmbedder catches spelling variants and small
	// edits but not a one-word change of subject; see the near-hit test for
	// the numbers behind the choice.
	Threshold float32
	// MaxLocal bounds the local vector index. Once full, the oldest vector
	// (by insertion) is evicted. 0 means 10000.
	MaxLocal int
	// TTL expires entries stored longer ago than this. 0 means never.
	TTL time.Duration
	// Clock supplies "now" for stamping and expiring entries. nil means
	// time.Now. Expiry is judged by whichever replica looks, so clock skew
	// between replicas shifts expiry by that skew; for a cache that is a
	// hit-rate concern, never a correctness one.
	Clock func() time.Time
	// Index is the local vector index. nil means brute-force cosine bounded
	// by MaxLocal. Anything else must implement the Index contract (unit
	// vectors in, cosine out).
	Index Index
}

func (o Options) withDefaults() Options {
	if o.Prefix == "" {
		o.Prefix = "semcache"
	}
	if o.Threshold <= 0 {
		o.Threshold = 0.92
	}
	if o.MaxLocal <= 0 {
		o.MaxLocal = 10000
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.Index == nil {
		o.Index = NewBruteIndex(o.MaxLocal)
	}
	return o
}

// Stats are cumulative counters plus the current local index size.
// Lookups == ExactHits + NearHits + Misses once all in-flight calls return.
type Stats struct {
	Lookups      uint64
	ExactHits    uint64
	NearHits     uint64
	Misses       uint64
	Stores       uint64
	LocalVectors int
}

// Cache implements gateway.Cache. It is safe for concurrent use: the KV is
// the shared state, the index guards itself, and counters are atomic.
type Cache struct {
	kv   kvapi.KV
	emb  Embedder
	opts Options

	lookups, exactHits, nearHits, misses, stores atomic.Uint64
}

// New returns a Cache over kv using emb for the near path. Several Caches
// (in several processes) over the same kv and prefix share the exact path
// and nothing else.
func New(kv kvapi.KV, emb Embedder, opts Options) *Cache {
	return &Cache{kv: kv, emb: emb, opts: opts.withDefaults()}
}

// Options returns the effective options.
func (c *Cache) Options() Options { return c.opts }

// ---------------------------------------------------------------------------
// keys and encodings
// ---------------------------------------------------------------------------

// keyExact is the shared key for a normalised prompt. Hashing keeps keys a
// fixed size regardless of prompt length and keeps prompt text out of the
// key space (values only), which matters once keys appear in logs.
func (c *Cache) keyExact(norm string) string {
	sum := sha256.Sum256([]byte(norm))
	return c.opts.Prefix + "/exact/" + hex.EncodeToString(sum[:])
}

func (c *Cache) nowMs() int64 { return c.opts.Clock().UnixMilli() }

// entry is the stored record: the completion and when it was stored.
type entry struct {
	text     string
	storedMs int64
}

// The wire format is 8 bytes of big-endian stored_ms followed by the text.
// A fixed header and a trailing variable field need no length prefix and no
// codec; gob would work but drags in reflection and type descriptors for a
// record with two fields.
const entryHeader = 8

func encodeEntry(e entry) []byte {
	b := make([]byte, entryHeader+len(e.text))
	binary.BigEndian.PutUint64(b, uint64(e.storedMs))
	copy(b[entryHeader:], e.text)
	return b
}

func decodeEntry(b []byte) (entry, bool) {
	if len(b) < entryHeader {
		return entry{}, false
	}
	return entry{
		storedMs: int64(binary.BigEndian.Uint64(b)),
		text:     string(b[entryHeader:]),
	}, true
}

// expired reports whether e is past TTL as of now. With TTL unset nothing
// expires.
func (c *Cache) expired(e entry) bool {
	if c.opts.TTL <= 0 {
		return false
	}
	return e.storedMs+c.opts.TTL.Milliseconds() < c.nowMs()
}

// fetch reads and validates the entry at key. ok is false when the key is
// absent, undecodable, or expired; all three are a miss to the caller.
func (c *Cache) fetch(ctx context.Context, key string) (entry, bool, error) {
	raw, found, err := c.kv.Get(ctx, key)
	if err != nil {
		return entry{}, false, err
	}
	if !found {
		return entry{}, false, nil
	}
	e, ok := decodeEntry(raw)
	if !ok || c.expired(e) {
		return entry{}, false, nil
	}
	return e, true, nil
}

// embedUnit embeds norm and scales the result to unit length so that the
// index's dot product is a cosine whatever the Embedder returned.
func (c *Cache) embedUnit(ctx context.Context, norm string) ([]float32, error) {
	vec, err := c.emb.Embed(ctx, norm)
	if err != nil {
		return nil, err
	}
	if len(vec) != c.emb.Dim() {
		return nil, errors.New("semcache: embedder returned a vector of the wrong dimension")
	}
	unit(vec)
	return vec, nil
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

// Lookup returns the cached completion for prompt if one exists, trying the
// shared exact path first and the local near path second. A near hit is
// only possible for a prompt this replica has indexed; see the package doc.
func (c *Cache) Lookup(ctx context.Context, prompt string) (string, bool, error) {
	c.lookups.Add(1)
	norm := normalise(prompt)
	key := c.keyExact(norm)

	// Exact path: one Get, no embedding, shared across replicas.
	e, ok, err := c.fetch(ctx, key)
	if err != nil {
		return "", false, err
	}
	if ok {
		c.exactHits.Add(1)
		// Learn: index this prompt if we have not already, so the near
		// path can find its variants later. Embedding only on the first
		// exact hit keeps the steady-state exact path embedding-free.
		if !c.opts.Index.Has(key) {
			if vec, err := c.embedUnit(ctx, norm); err == nil {
				c.opts.Index.Add(vec, key)
			}
		}
		return e.text, true, nil
	}
	// Absent or stale. If stale, the index may still point at it; drop that
	// so the near path below does not immediately re-find the same key.
	c.opts.Index.Remove(key)

	// Near path: embed, scan the local index, then confirm in the KV. The
	// KV read is not optional: the index only knows keys, and the entry
	// behind a key may have expired or been evicted since it was indexed.
	vec, err := c.embedUnit(ctx, norm)
	if err != nil {
		return "", false, err
	}
	nkey, sim, found := c.opts.Index.Nearest(vec)
	if !found || sim < c.opts.Threshold {
		c.misses.Add(1)
		return "", false, nil
	}
	ne, ok, err := c.fetch(ctx, nkey)
	if err != nil {
		return "", false, err
	}
	if !ok {
		c.opts.Index.Remove(nkey)
		c.misses.Add(1)
		return "", false, nil
	}
	c.nearHits.Add(1)
	return ne.text, true, nil
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

// Store records text as the completion for prompt: Put the shared exact
// entry, then embed and add to the local index. The Put comes first because
// it is what other replicas can see; if embedding then fails, the exact path
// still works everywhere and the error tells the caller only the near path
// was not updated.
func (c *Cache) Store(ctx context.Context, prompt, text string) error {
	norm := normalise(prompt)
	key := c.keyExact(norm)
	if err := c.kv.Put(ctx, key, encodeEntry(entry{text: text, storedMs: c.nowMs()})); err != nil {
		return err
	}
	c.stores.Add(1)
	vec, err := c.embedUnit(ctx, norm)
	if err != nil {
		return err
	}
	c.opts.Index.Add(vec, key)
	return nil
}

// Stats returns a snapshot of the counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Lookups:      c.lookups.Load(),
		ExactHits:    c.exactHits.Load(),
		NearHits:     c.nearHits.Load(),
		Misses:       c.misses.Load(),
		Stores:       c.stores.Load(),
		LocalVectors: c.opts.Index.Len(),
	}
}
