package simnet

import (
	"testing"
)

// TestSeededFaultsAreReproducible proves the property NewSeeded's doc
// comment claims: the same seed, driving calls in the same order, makes
// the same sequence of intn() decisions. This is the whole mechanism the
// Phase 6 chaos harness leans on.
func TestSeededFaultsAreReproducible(t *testing.T) {
	const seed = 12345
	draw := func() []int {
		sn := NewSeeded(1, seed)
		out := make([]int, 500)
		for i := range out {
			out[i] = sn.intn(1000)
		}
		return out
	}
	a, b := draw(), draw()
	if len(a) != len(b) {
		t.Fatalf("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("draw %d differs: %d vs %d — same seed did not reproduce the same decisions", i, a[i], b[i])
		}
	}
}

// A different seed must (overwhelmingly likely) produce a different sequence.
func TestDifferentSeedsDiffer(t *testing.T) {
	a := NewSeeded(1, 1)
	b := NewSeeded(1, 2)
	same := 0
	for i := 0; i < 50; i++ {
		if a.intn(1_000_000) == b.intn(1_000_000) {
			same++
		}
	}
	if same > 2 {
		t.Fatalf("seeds 1 and 2 agreed %d/50 times; they are not independent", same)
	}
}

// New (unseeded) must not panic and must still be usable; it is exercised
// by every pre-existing test in this repo, which is the real regression
// check, but this pins the "still works with no seed" contract directly.
func TestUnseededNetStillWorks(t *testing.T) {
	sn := New(3)
	if got := sn.intn(10); got < 0 || got >= 10 {
		t.Fatalf("intn out of range: %d", got)
	}
}
