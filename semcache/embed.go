package semcache

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
	"unicode"
)

// Embedder turns a prompt into a fixed-size vector. Pluggable: the default
// (NGramEmbedder) needs no model and no network; production would call an
// embedding model. Implementations must be safe for concurrent use. Vectors
// need not be unit length; the cache normalises them.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	Dim() int
}

// NGramEmbedder is a deterministic, dependency-free embedder built on
// feature hashing: normalise the text, hash its character 2-grams and
// 3-grams and its word unigrams into a Dim-sized bucket vector with a
// hash-derived sign, and L2-normalise.
//
// It is NOT a good semantic embedding. It knows nothing about meaning:
// "cheap flights to Paris" and "inexpensive airfare to France" share almost
// no n-grams and score near zero, while "do X" and "do not X" score near
// one. What it does have is exactly what a cache's tests and benchmark need
// without downloading a model: it is stable across processes and runs
// (identical prompts give identical vectors, cosine 1.0), it is fast
// (microseconds), it scores spelling variants and small edits high (the
// near-hit test logs ~0.95 for "Summarize" versus "Summarise") and unrelated
// prompts low (~0.3). Swap in a real model behind the Embedder interface
// for anything that has to understand paraphrase.
//
// Design notes. Character n-grams give tolerance to typos and inflection
// (one changed character disturbs two bigrams and three trigrams out of the
// whole string); word unigrams add a little weight to whole-word identity.
// The sign bit makes hash collisions cancel in expectation instead of
// inflating similarity, the standard trick from Weinberger et al. (2009).
// Dim trades collision noise against dot-product cost: 512 is comfortable
// for prompts of a few hundred characters.
type NGramEmbedder struct {
	dim int
}

// DefaultDim is the dimension NewNGramEmbedder uses for dim <= 0.
const DefaultDim = 512

// NewNGramEmbedder returns an embedder producing vectors of length dim
// (DefaultDim if dim <= 0).
func NewNGramEmbedder(dim int) *NGramEmbedder {
	if dim <= 0 {
		dim = DefaultDim
	}
	return &NGramEmbedder{dim: dim}
}

// Dim returns the vector length.
func (e *NGramEmbedder) Dim() int { return e.dim }

// Embed returns the unit-length feature-hashed vector for text. It never
// fails; the error is for the interface. The zero string embeds to the zero
// vector, which has cosine 0 with everything.
func (e *NGramEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	vec := make([]float32, e.dim)
	norm := normalise(text)

	// Character n-grams over runes, so multi-byte text is not split inside
	// a code point. Spaces are kept: " th" and "he " carry word-boundary
	// information that "the" alone does not.
	runes := []rune(norm)
	for n := 2; n <= 3; n++ {
		for i := 0; i+n <= len(runes); i++ {
			e.bump(vec, "c", string(runes[i:i+n]))
		}
	}
	// Word unigrams, with surrounding punctuation trimmed so "policy." and
	// "policy" are the same word. The punctuation is still in the n-grams
	// above, so it is not lost, merely not doubled.
	for _, w := range strings.Fields(norm) {
		w = strings.TrimFunc(w, unicode.IsPunct)
		if w != "" {
			e.bump(vec, "w", w)
		}
	}
	unit(vec)
	return vec, nil
}

// bump adds one feature: kind prefixes the token so a word and an n-gram
// with the same bytes hash differently; the low bits pick a bucket and a
// high bit picks the sign.
func (e *NGramEmbedder) bump(vec []float32, kind, tok string) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(kind))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(tok))
	sum := h.Sum64()
	bucket := int(sum % uint64(e.dim))
	if sum>>63 == 1 {
		vec[bucket] -= 1
	} else {
		vec[bucket] += 1
	}
}

// normalise is the canonical form used for both the exact hash and the
// embedding: lowercase, whitespace collapsed to single spaces, trimmed. It
// means the exact path treats "Hello world" and "hello   world" as one
// prompt, which for a cache of completions is a feature, not a bug: the
// model's answer does not hinge on that difference.
func normalise(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// unit scales vec to L2 length 1 in place. The zero vector is left alone.
func unit(vec []float32) {
	var sum float64
	for _, x := range vec {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range vec {
		vec[i] *= inv
	}
}

// Cosine returns the cosine similarity of a and b. It normalises internally
// so callers can pass raw Embedder output; the index avoids that work by
// requiring unit vectors up front.
func Cosine(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / math.Sqrt(na*nb))
}

// dot is the similarity used by the index: for unit vectors it equals the
// cosine and costs one pass.
func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}
