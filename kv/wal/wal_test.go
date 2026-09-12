package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var crashIterations = flag.Int("crash-iterations", 5, "iterations for TestCrashRecovery")

func openT(t *testing.T, path string) *WAL {
	t.Helper()
	w, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func replayAll(t *testing.T, w *WAL) [][]byte {
	t.Helper()
	var out [][]byte
	if err := w.Replay(func(p []byte) error {
		out = append(out, append([]byte(nil), p...))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out
}

func TestAppendThenReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w := openT(t, path)
	for i := 0; i < 100; i++ {
		if err := w.Append([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w = openT(t, path)
	defer w.Close()
	got := replayAll(t, w)
	if len(got) != 100 {
		t.Fatalf("got %d records, want 100", len(got))
	}
	for i, p := range got {
		if string(p) != fmt.Sprintf("record-%d", i) {
			t.Fatalf("record %d = %q", i, p)
		}
	}
}

func TestEmptyPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w := openT(t, path)
	if err := w.Append(nil); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w = openT(t, path)
	defer w.Close()
	if got := replayAll(t, w); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("got %v", got)
	}
}

// TestTornTail simulates a crash at every possible byte boundary inside the
// last record and checks that recovery keeps exactly the earlier records and
// that the log is usable afterwards.
func TestTornTail(t *testing.T) {
	const n = 10
	base := filepath.Join(t.TempDir(), "base.wal")
	w := openT(t, base)
	for i := 0; i < n; i++ {
		if err := w.Append([]byte(fmt.Sprintf("payload-number-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	full := w.Size()
	w.Close()
	data, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	recLen := int64(headerSize + len("payload-number-00"))
	lastStart := full - recLen

	for cut := lastStart + 1; cut < full; cut++ {
		path := filepath.Join(t.TempDir(), "torn.wal")
		if err := os.WriteFile(path, data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		w := openT(t, path)
		got := replayAll(t, w)
		if len(got) != n-1 {
			t.Fatalf("cut at %d: got %d records, want %d", cut, len(got), n-1)
		}
		if w.Size() != lastStart {
			t.Fatalf("cut at %d: size %d, want %d", cut, w.Size(), lastStart)
		}
		// The log must be writable again and the new record must land
		// immediately after the last good one.
		if err := w.Append([]byte("after-crash")); err != nil {
			t.Fatal(err)
		}
		w.Close()
		w = openT(t, path)
		got = replayAll(t, w)
		w.Close()
		if len(got) != n || string(got[n-1]) != "after-crash" {
			t.Fatalf("cut at %d: after reopen got %d records, last=%q", cut, len(got), got[len(got)-1])
		}
	}
}

// TestMidFileCorruptionIsAnError: a bad checksum with valid data after it is
// silent data loss if ignored, so Open must refuse.
func TestMidFileCorruptionIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	w := openT(t, path)
	for i := 0; i < 3; i++ {
		if err := w.Append([]byte("abcdefgh")); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the payload of the second record.
	rec := int64(headerSize + 8)
	if _, err := f.WriteAt([]byte{'X'}, rec+headerSize); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(path, Options{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got err %v, want ErrCorrupt", err)
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	w := openT(t, filepath.Join(t.TempDir(), "x.wal"))
	w.Close()
	if err := w.Append([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

// ---------------------------------------------------------------------------
// The real test: a separate process appends records and reports each one as
// acknowledged only after Append returned. We kill it at a random moment,
// reopen the log, and check that every acknowledged record is present and
// the log is a contiguous prefix. This is the Phase 1 exit criterion; run it
// with a high -crash-iterations via `make test-crash`.
// ---------------------------------------------------------------------------

const helperEnv = "DSYS_WAL_HELPER_PATH"

func TestMain(m *testing.M) {
	if path := os.Getenv(helperEnv); path != "" {
		runHelper(path)
		return
	}
	flag.Parse()
	os.Exit(m.Run())
}

// runHelper appends sequence numbers forever, printing each after it is durable.
func runHelper(path string) {
	w, err := Open(path, Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// Continue from where a previous incarnation left off.
	var next uint64
	_ = w.Replay(func(p []byte) error { next = binary.LittleEndian.Uint64(p) + 1; return nil })

	out := bufio.NewWriter(os.Stdout)
	var buf [8]byte
	for {
		binary.LittleEndian.PutUint64(buf[:], next)
		// Vary the size so torn writes land at different places.
		payload := append(buf[:], make([]byte, rand.Intn(200))...)
		if err := w.Append(payload); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(out, next)
		out.Flush()
		next++
	}
}

func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "crash.wal")
	var expectNext uint64

	for iter := 0; iter < *crashIterations; iter++ {
		cmd := exec.Command(exe)
		cmd.Env = append(os.Environ(), helperEnv+"="+path)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		acked := make(chan uint64, 1)
		go func() {
			var last uint64
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				v, _ := strconv.ParseUint(sc.Text(), 10, 64)
				last = v
			}
			acked <- last
		}()

		time.Sleep(time.Duration(5+rand.Intn(60)) * time.Millisecond)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		lastAcked := <-acked

		w := openT(t, path)
		var seqs []uint64
		if err := w.Replay(func(p []byte) error {
			seqs = append(seqs, binary.LittleEndian.Uint64(p))
			return nil
		}); err != nil {
			t.Fatalf("iter %d: replay: %v", iter, err)
		}
		w.Close()

		// 1. Contiguous prefix: 0,1,2,...
		for i, s := range seqs {
			if s != uint64(i) {
				t.Fatalf("iter %d: seqs[%d]=%d, log is not a contiguous prefix", iter, i, s)
			}
		}
		// 2. Everything acknowledged survived.
		if len(seqs) > 0 && uint64(len(seqs)-1) < lastAcked {
			t.Fatalf("iter %d: last acked %d but log ends at %d: LOST ACKNOWLEDGED WRITES", iter, lastAcked, len(seqs)-1)
		}
		if uint64(len(seqs)) < expectNext {
			t.Fatalf("iter %d: log shrank from %d to %d records", iter, expectNext, len(seqs))
		}
		expectNext = uint64(len(seqs))
	}
	t.Logf("survived %d crashes, %d records durable", *crashIterations, expectNext)
}
