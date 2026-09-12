// These tests describe the Phase 1 exercises: compare-and-swap and
// idempotent retries. They were gated behind a build tag until the
// exercises were implemented; now they run with everything else.
package store

import (
	"testing"

	kvv1 "dsys/gen/kv/v1"
)

// ---- Exercise 1: Compare-and-swap ------------------------------------------

func TestCASBasic(t *testing.T) {
	s := openT(t, t.TempDir())
	defer s.Close()

	// Absent key, expectAbsent: swap.
	swapped, cur, err := s.CompareAndSwap("lock", nil, true, []byte("A"), nil)
	if err != nil || !swapped || cur != nil {
		t.Fatalf("1: swapped=%v cur=%q err=%v", swapped, cur, err)
	}
	// Present key, expectAbsent: no swap, report current.
	swapped, cur, _ = s.CompareAndSwap("lock", nil, true, []byte("B"), nil)
	if swapped || string(cur) != "A" {
		t.Fatalf("2: swapped=%v cur=%q", swapped, cur)
	}
	// Wrong expected: no swap.
	swapped, cur, _ = s.CompareAndSwap("lock", []byte("Z"), false, []byte("B"), nil)
	if swapped || string(cur) != "A" {
		t.Fatalf("3: swapped=%v cur=%q", swapped, cur)
	}
	// Right expected: swap.
	swapped, cur, _ = s.CompareAndSwap("lock", []byte("A"), false, []byte("B"), nil)
	if !swapped || string(cur) != "A" {
		t.Fatalf("4: swapped=%v cur=%q", swapped, cur)
	}
	if v, _ := s.Get("lock"); string(v) != "B" {
		t.Fatalf("final=%q", v)
	}
}

// A failed CAS must still be correct after replay: the log may or may not
// contain failed attempts, but the resulting state must be identical.
func TestCASReplay(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	_, _, _ = s.CompareAndSwap("k", nil, true, []byte("1"), nil)
	_, _, _ = s.CompareAndSwap("k", []byte("wrong"), false, []byte("2"), nil)
	_, _, _ = s.CompareAndSwap("k", []byte("1"), false, []byte("3"), nil)
	live, _ := s.Get("k")
	s.Close()

	s = openT(t, dir)
	defer s.Close()
	after, _ := s.Get("k")
	if string(live) != "3" || string(after) != "3" {
		t.Fatalf("live=%q after=%q", live, after)
	}
}

// ---- Exercise 2: idempotent retries -----------------------------------------

func meta(client string, id uint64) *kvv1.RequestMeta {
	return &kvv1.RequestMeta{ClientId: client, RequestId: id}
}

// The same (client_id, request_id) delivered twice must have the effect of
// being applied once, and the retry must receive the ORIGINAL result.
func TestDuplicateDeleteReturnsOriginalResult(t *testing.T) {
	s := openT(t, t.TempDir())
	defer s.Close()
	_ = s.Put("a", []byte("1"), meta("c1", 1))

	existed, _ := s.Delete("a", meta("c1", 2))
	if !existed {
		t.Fatal("first delete should see the key")
	}
	// Network dropped the reply; client retries the same request.
	existed, _ = s.Delete("a", meta("c1", 2))
	if !existed {
		t.Fatal("retried delete must return the original result (existed=true), not be re-applied")
	}
}

func TestDuplicateCASIsNotReapplied(t *testing.T) {
	s := openT(t, t.TempDir())
	defer s.Close()
	_ = s.Put("ctr", []byte("0"), meta("c1", 1))

	swapped, _, _ := s.CompareAndSwap("ctr", []byte("0"), false, []byte("1"), meta("c1", 2))
	if !swapped {
		t.Fatal("first CAS should swap")
	}
	// Another client moves it on.
	swapped, _, _ = s.CompareAndSwap("ctr", []byte("1"), false, []byte("2"), meta("c2", 1))
	if !swapped {
		t.Fatal("c2 CAS should swap")
	}
	// c1 retries its old request. It must get swapped=true (the original
	// answer) and must NOT change the value.
	swapped, _, _ = s.CompareAndSwap("ctr", []byte("0"), false, []byte("1"), meta("c1", 2))
	if !swapped {
		t.Fatal("retry must return original swapped=true")
	}
	if v, _ := s.Get("ctr"); string(v) != "2" {
		t.Fatalf("retry mutated state: ctr=%q want 2", v)
	}
}

// Dedup state must itself survive a crash: if the server restarts between the
// original request and the retry, the retry must still be recognised.
func TestDedupSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	existed, _ := s.Delete("nothing", meta("c1", 7))
	if existed {
		t.Fatal("setup")
	}
	_ = s.Put("nothing", []byte("now-present"), meta("c2", 1))
	s.Close()

	s = openT(t, dir)
	defer s.Close()
	existed, _ = s.Delete("nothing", meta("c1", 7))
	if existed {
		t.Fatal("retry after restart was re-applied: dedup table was not rebuilt from the log")
	}
	if v, ok := s.Get("nothing"); !ok || string(v) != "now-present" {
		t.Fatalf("retry after restart deleted a newer value: %q %v", v, ok)
	}
}
