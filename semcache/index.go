package semcache

import "sync"

// Index is the local vector index: unit vectors in, the most similar stored
// key out. It maps vectors to exact-path KV keys, never to text, so the
// index can never serve an answer the KV does not currently hold.
//
// The contract is small on purpose so an approximate structure (HNSW, IVF)
// can replace the brute-force default without touching Cache. Vectors
// passed to Add and Nearest are unit length; Nearest returns a dot product,
// which for unit vectors is the cosine. Implementations must be safe for
// concurrent use.
type Index interface {
	// Add indexes vec under key, replacing any vector already under key.
	// Evicting to stay within a bound is the implementation's business.
	Add(vec []float32, key string)
	// Remove drops key if present.
	Remove(key string)
	// Has reports whether key is indexed.
	Has(key string) bool
	// Nearest returns the indexed key most similar to vec and that
	// similarity. ok is false when the index is empty.
	Nearest(vec []float32) (key string, sim float32, ok bool)
	// Len is the number of indexed vectors.
	Len() int
}

// BruteIndex is an exact nearest-neighbour index by linear scan, bounded by
// evicting the oldest insertion once full (FIFO; a hit does not refresh an
// entry's age, since the exact path, not this index, is what a repeated
// prompt hits).
//
// Storage is a ring of slots: the first max Adds fill it, after which each
// Add overwrites the slot after the previous one, so eviction is O(1) and
// the map from key to slot stays valid. Remove leaves a tombstone (empty
// key) that Nearest skips and a later wrap-around reuses.
//
// A RWMutex, not a Mutex: Nearest is the hot path and read-only, so many
// concurrent lookups scan in parallel; only Add and Remove take the write
// lock.
type BruteIndex struct {
	mu    sync.RWMutex
	max   int
	slots []slot
	next  int // slot to overwrite once len(slots) == max
	pos   map[string]int
}

type slot struct {
	key string
	vec []float32
}

// NewBruteIndex returns an index holding at most max vectors (1 if max <= 0).
func NewBruteIndex(max int) *BruteIndex {
	if max <= 0 {
		max = 1
	}
	return &BruteIndex{max: max, pos: make(map[string]int)}
}

func (ix *BruteIndex) Add(vec []float32, key string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if i, ok := ix.pos[key]; ok {
		ix.slots[i].vec = vec
		return
	}
	if len(ix.slots) < ix.max {
		ix.slots = append(ix.slots, slot{key: key, vec: vec})
		ix.pos[key] = len(ix.slots) - 1
		return
	}
	// Full: evict whatever is in the next slot (the oldest live insertion,
	// or a tombstone) and take its place.
	if old := ix.slots[ix.next].key; old != "" {
		delete(ix.pos, old)
	}
	ix.slots[ix.next] = slot{key: key, vec: vec}
	ix.pos[key] = ix.next
	ix.next = (ix.next + 1) % ix.max
}

func (ix *BruteIndex) Remove(key string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if i, ok := ix.pos[key]; ok {
		ix.slots[i] = slot{}
		delete(ix.pos, key)
	}
}

func (ix *BruteIndex) Has(key string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	_, ok := ix.pos[key]
	return ok
}

func (ix *BruteIndex) Nearest(vec []float32) (string, float32, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var (
		bestKey string
		bestSim float32
		found   bool
	)
	for i := range ix.slots {
		s := &ix.slots[i]
		if s.key == "" || len(s.vec) != len(vec) {
			continue
		}
		sim := dot(s.vec, vec)
		if !found || sim > bestSim {
			bestKey, bestSim, found = s.key, sim, true
		}
	}
	return bestKey, bestSim, found
}

func (ix *BruteIndex) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.pos)
}
