package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	kvv1 "dsys/gen/kv/v1"
)

// With real fsyncs and many concurrent writers, the committer must be
// coalescing writes: far fewer fsyncs than entries.
func TestGroupCommitCoalesces(t *testing.T) {
	s, err := Open(t.TempDir(), Options{}) // NoSync=false: real fsync
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const writers, perWriter = 32, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := s.Put(fmt.Sprintf("w%d-%d", w, i), []byte("x"), nil); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	st := s.Stats()
	if st.Entries != writers*perWriter {
		t.Fatalf("entries=%d want %d", st.Entries, writers*perWriter)
	}
	if st.Batches >= st.Entries/2 {
		t.Fatalf("batches=%d entries=%d: group commit is not coalescing", st.Batches, st.Entries)
	}
	t.Logf("%d entries in %d fsyncs (avg batch %.1f)", st.Entries, st.Batches, float64(st.Entries)/float64(st.Batches))
	if s.Len() != writers*perWriter {
		t.Fatalf("len=%d", s.Len())
	}
}

// Two copies of the same request racing into the same batch must still
// apply once. This is the case the commit-time fast path cannot catch.
func TestDuplicateInSameBatchAppliesOnce(t *testing.T) {
	s, err := Open(t.TempDir(), Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.Put("ctr", []byte("0"), nil)

	for round := 0; round < 200; round++ {
		m := &kvv1.RequestMeta{ClientId: "c", RequestId: uint64(round + 1)}
		exp := []byte(fmt.Sprint(round))
		next := []byte(fmt.Sprint(round + 1))
		var wg sync.WaitGroup
		results := make([]bool, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], _, _ = s.CompareAndSwap("ctr", exp, false, next, m)
			}(i)
		}
		wg.Wait()
		if !results[0] || !results[1] {
			t.Fatalf("round %d: both copies must report swapped=true, got %v", round, results)
		}
	}
	if v, _ := s.Get("ctr"); string(v) != "200" {
		t.Fatalf("ctr=%q want 200", v)
	}
}

// Close must complete writes that were accepted before it and reject ones after.
func TestCloseFlushesAndRejects(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var accepted, rejected int
	var mu sync.Mutex
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Put(fmt.Sprintf("k%d", i), []byte("v"), nil)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrClosed):
				rejected++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if accepted+rejected != 64 {
		t.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}

	// Everything that was acknowledged is on disk.
	s2, err := Open(dir, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Len() != accepted {
		t.Fatalf("reopened len=%d, acknowledged=%d", s2.Len(), accepted)
	}
	if err := s.Put("late", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestStaleRequestRejected(t *testing.T) {
	s, err := Open(t.TempDir(), Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.Put("a", []byte("1"), &kvv1.RequestMeta{ClientId: "c", RequestId: 5})
	err = s.Put("a", []byte("2"), &kvv1.RequestMeta{ClientId: "c", RequestId: 3})
	if !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("got %v want ErrStaleRequest", err)
	}
}
