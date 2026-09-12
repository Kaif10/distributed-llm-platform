package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.StateSize() != 0 || p.ReadState() != nil || p.ReadSnapshot() != nil {
		t.Fatal("fresh persister should be empty")
	}
	state := []byte("state-bytes")
	snap := bytes.Repeat([]byte{7}, 100_000)
	if err := p.Save(state, snap); err != nil {
		t.Fatal(err)
	}

	p2, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p2.ReadState(), state) || !bytes.Equal(p2.ReadSnapshot(), snap) {
		t.Fatal("reload mismatch")
	}
	// Save with nil snapshot must read back as nil, not empty slice.
	if err := p2.Save([]byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	p3, _ := OpenFilePersister(dir)
	if p3.ReadSnapshot() != nil || string(p3.ReadState()) != "x" {
		t.Fatalf("got snapshot=%v state=%q", p3.ReadSnapshot(), p3.ReadState())
	}
	if _, err := os.Stat(filepath.Join(dir, "raft.state.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temp file left behind")
	}
}

func TestFilePersisterDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenFilePersister(dir)
	_ = p.Save([]byte("hello world"), []byte("snap"))
	path := filepath.Join(dir, "raft.state")
	b, _ := os.ReadFile(path)
	b[14] ^= 0xff // flip a byte inside the state
	_ = os.WriteFile(path, b, 0o644)
	if _, err := OpenFilePersister(dir); !errors.Is(err, ErrCorruptPersistedState) {
		t.Fatalf("got %v, want ErrCorruptPersistedState", err)
	}
}
