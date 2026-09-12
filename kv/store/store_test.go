package store

import (
	"fmt"
	"sync"
	"testing"
)

func openT(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{NoSync: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := openT(t, t.TempDir())
	defer s.Close()

	if _, ok := s.Get("a"); ok {
		t.Fatal("expected missing")
	}
	if err := s.Put("a", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	v, ok := s.Get("a")
	if !ok || string(v) != "1" {
		t.Fatalf("got %q,%v", v, ok)
	}
	existed, err := s.Delete("a", nil)
	if err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	existed, _ = s.Delete("a", nil)
	if existed {
		t.Fatal("second delete should report not existed")
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := s.Put(k, []byte(fmt.Sprintf("v%d", i)), nil); err != nil {
			t.Fatal(err)
		}
	}
	// Overwrite and delete some, so replay order matters.
	_ = s.Put("k1", []byte("new"), nil)
	_, _ = s.Delete("k2", nil)
	s.Close()

	s = openT(t, dir)
	defer s.Close()
	if s.Len() != 49 {
		t.Fatalf("len=%d want 49", s.Len())
	}
	if v, _ := s.Get("k1"); string(v) != "new" {
		t.Fatalf("k1=%q", v)
	}
	if _, ok := s.Get("k2"); ok {
		t.Fatal("k2 should be deleted")
	}
	if v, _ := s.Get("k49"); string(v) != "v49" {
		t.Fatalf("k49=%q", v)
	}
}

// Run with -race. Concurrent writers to the same key must leave the store
// and the log agreeing about the final value.
func TestConcurrentWritersReplayConsistently(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = s.Put("shared", []byte(fmt.Sprintf("%d-%d", g, i)), nil)
				_ = s.Put(fmt.Sprintf("own-%d", g), []byte(fmt.Sprint(i)), nil)
			}
		}(g)
	}
	wg.Wait()
	live, _ := s.Get("shared")
	s.Close()

	s = openT(t, dir)
	defer s.Close()
	replayed, _ := s.Get("shared")
	if string(live) != string(replayed) {
		t.Fatalf("live=%q replayed=%q: log order != apply order", live, replayed)
	}
	for g := 0; g < 8; g++ {
		if v, _ := s.Get(fmt.Sprintf("own-%d", g)); string(v) != "199" {
			t.Fatalf("own-%d=%q", g, v)
		}
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := openT(t, t.TempDir())
	defer s.Close()
	_ = s.Put("a", []byte("abc"), nil)
	v, _ := s.Get("a")
	v[0] = 'X'
	if v2, _ := s.Get("a"); string(v2) != "abc" {
		t.Fatalf("internal state mutated: %q", v2)
	}
}
