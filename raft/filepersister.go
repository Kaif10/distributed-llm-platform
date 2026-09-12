package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister stores Raft state and the snapshot in a single file,
// replaced atomically on every Save.
//
// # Why one file, and why write-temp-then-rename
//
// Persister's contract is that state and snapshot are saved together or not
// at all. Two files cannot give that: a crash between the two writes leaves
// a snapshot from one moment and a log from another, and the log's
// lastIncludedIndex no longer describes the snapshot. One file with both
// halves, written to a temporary name and then renamed over the old one,
// gives atomicity from the filesystem: rename replaces the directory entry
// in one step, so a reader sees either the old file or the new one.
//
// # Cost
//
// Every Save rewrites the whole log plus the snapshot. That is O(log size)
// per append, which is fine while snapshots keep the log short (the
// service snapshots every few hundred entries) and is the same trade-off
// the MIT labs make. The production fix is obvious given Phase 1: append
// log entries to a WAL and only rewrite the snapshot file when it changes.
// That is left as the stated exercise in docs/phase2.md.
//
// File layout: magic(4) | stateLen(u32) | snapLen(u32) | state | snapshot | crc32c(4)
type FilePersister struct {
	mu       sync.Mutex
	path     string
	state    []byte
	snapshot []byte
}

var fileMagic = [4]byte{'R', 'A', 'F', '1'}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorruptPersistedState is returned by OpenFilePersister when the file
// fails its checksum.
var ErrCorruptPersistedState = errors.New("raft: persisted state is corrupt")

// OpenFilePersister loads existing state from dir (if any) and returns a
// persister writing to dir/raft.state.
func OpenFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &FilePersister{path: filepath.Join(dir, "raft.state")}
	b, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) < 16 || [4]byte(b[:4]) != fileMagic {
		return nil, fmt.Errorf("%w: bad header", ErrCorruptPersistedState)
	}
	stateLen := binary.LittleEndian.Uint32(b[4:8])
	snapLen := binary.LittleEndian.Uint32(b[8:12])
	if uint64(len(b)) != 12+uint64(stateLen)+uint64(snapLen)+4 {
		return nil, fmt.Errorf("%w: bad length", ErrCorruptPersistedState)
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptPersistedState)
	}
	p.state = clone(b[12 : 12+stateLen])
	p.snapshot = clone(b[12+stateLen : 12+stateLen+snapLen])
	if snapLen == 0 {
		p.snapshot = nil
	}
	return p, nil
}

// Save writes both values atomically.
func (p *FilePersister) Save(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf := make([]byte, 0, 16+len(state)+len(snapshot))
	buf = append(buf, fileMagic[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(state)))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(snapshot)))
	buf = append(buf, state...)
	buf = append(buf, snapshot...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	// fsync the temp file BEFORE the rename. Otherwise the rename can become
	// durable while the file's contents are still only in the page cache,
	// and a power loss leaves a correctly named, empty or torn file.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}
	// Strictly, POSIX also wants an fsync of the directory so the rename
	// itself is durable. Go cannot fsync a directory on Windows; on Linux
	// you would open the dir and call Sync. Noted here so you know the gap.

	p.state = clone(state)
	p.snapshot = clone(snapshot)
	return nil
}

// ReadState returns a copy of the last saved state.
func (p *FilePersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.state)
}

// ReadSnapshot returns a copy of the last saved snapshot.
func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

// StateSize returns the size of the saved state in bytes.
func (p *FilePersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}
