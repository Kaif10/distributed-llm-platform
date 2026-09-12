// Package wal implements an append-only write-ahead log.
//
// # Why a WAL exists
//
// A store keeps its data in memory for speed. Memory is lost on a crash, so
// before acknowledging any write we first append a description of it to a
// file and force that file to disk (fsync). On restart we replay the file to
// rebuild memory. The invariant the rest of the system relies on:
//
//	if Append returned nil, the record survives any crash after that point.
//
// # On-disk format
//
// The file is a sequence of records, nothing else, no index, no header:
//
//	+-----------------+-----------------+------------------+
//	| length (uint32) | crc32c (uint32) | payload (length) |
//	+-----------------+-----------------+------------------+
//
// Both integers are little-endian. The CRC covers only the payload.
//
// # Crash semantics
//
// A crash can happen in the middle of a write. Because we only ever append,
// the damage is confined to the tail of the file: the last record may be
// partially written ("torn"). Open detects this and truncates the file back
// to the last complete, checksum-valid record. Anything before that point was
// fully written before an earlier Append returned, so it is trusted.
//
// Corruption that is *not* at the tail (a bad checksum followed by more data)
// cannot be explained by a crash and is reported as ErrCorrupt rather than
// silently dropped.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	headerSize     = 8
	maxPayloadSize = 64 << 20 // 64 MiB; anything larger is treated as garbage
)

var (
	// ErrCorrupt is returned when a record fails its checksum and is not at
	// the tail of the log, or when the log is otherwise unreadable.
	ErrCorrupt = errors.New("wal: log is corrupt")
	// ErrClosed is returned by Append after Close.
	ErrClosed = errors.New("wal: closed")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options controls WAL behaviour.
type Options struct {
	// NoSync disables fsync after every append. Only ever set this in tests
	// or benchmarks: with NoSync a crash can lose acknowledged writes.
	NoSync bool
}

// WAL is a single append-only log file. It is safe for concurrent use, but
// note that Append holds a mutex across the disk write, so appends are
// serialized. That is intentional: the order of records in the file is the
// order of writes, and the caller relies on that (see store.Store).
type WAL struct {
	mu     sync.Mutex
	f      *os.File
	off    int64 // byte offset of the end of the last valid record
	opts   Options
	closed bool
}

// Open opens or creates the log at path and repairs any torn tail.
// Records that survived are then available through Replay.
func Open(path string, opts Options) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{f: f, opts: opts}

	validEnd, err := w.scan(nil)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// Repair: throw away the torn tail, if any. After this the file contains
	// only complete records, and new appends go at validEnd.
	if err := f.Truncate(validEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	if !opts.NoSync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	w.off = validEnd
	return w, nil
}

// Replay calls fn for every record in the log, in append order.
// If fn returns an error, replay stops and that error is returned.
func (w *WAL) Replay(fn func(payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.scan(fn)
	return err
}

// Append writes one record and, unless NoSync is set, fsyncs it. When Append
// returns nil the record is durable.
func (w *WAL) Append(payload []byte) error {
	if len(payload) > maxPayloadSize {
		return fmt.Errorf("wal: payload too large: %d bytes", len(payload))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	// Build the whole record in one buffer and issue a single write. A single
	// write is not atomic on most filesystems, but it minimizes the window
	// for a torn record and avoids interleaving.
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(payload, castagnoli))
	copy(buf[headerSize:], payload)

	if _, err := w.f.WriteAt(buf, w.off); err != nil {
		return err
	}
	if !w.opts.NoSync {
		// This is the expensive call: it forces the OS to push the data (and
		// the file's new length) to the physical device. Everything about
		// durability hinges on it. Without it, "written" only means "in the
		// OS page cache", which a power loss erases.
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	w.off += int64(len(buf))
	return nil
}

// Size returns the number of bytes of valid records in the log.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.off
}

// Close closes the underlying file. Further Appends fail with ErrClosed.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}

// scan reads the log from the start, calling fn (if non-nil) for each valid
// record, and returns the offset just past the last valid record.
//
// The rules for what counts as "the end of valid data" are the heart of
// crash recovery; read them together with the package comment.
func (w *WAL) scan(fn func([]byte) error) (int64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReaderSize(w.f, 1<<20)

	var (
		off    int64 // offset of the record we are about to read
		header [headerSize]byte
	)
	for {
		n, err := io.ReadFull(r, header[:])
		if err == io.EOF && n == 0 {
			return off, nil // clean end of log
		}
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n > 0) {
			return off, nil // torn header at the tail: drop it
		}
		if err != nil {
			return 0, err
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		want := binary.LittleEndian.Uint32(header[4:8])
		if length > maxPayloadSize {
			// An absurd length means the header itself is garbage. We can only
			// tolerate that if it is the last thing in the file.
			if isAtEOF(r) {
				return off, nil
			}
			return 0, fmt.Errorf("%w: bad length %d at offset %d", ErrCorrupt, length, off)
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return off, nil // torn payload at the tail: drop it
			}
			return 0, err
		}

		if crc32.Checksum(payload, castagnoli) != want {
			// Bad checksum. If nothing follows, this is a torn write on a
			// filesystem that extended the file before the data landed.
			// If more data follows, a complete record was later written
			// after it, meaning this record was once acknowledged, so its
			// corruption is real data loss and must not be hidden.
			if isAtEOF(r) {
				return off, nil
			}
			return 0, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorrupt, off)
		}

		if fn != nil {
			if err := fn(payload); err != nil {
				return 0, err
			}
		}
		off += headerSize + int64(length)
	}
}

func isAtEOF(r *bufio.Reader) bool {
	_, err := r.Peek(1)
	return err == io.EOF
}
