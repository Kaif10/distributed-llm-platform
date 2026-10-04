package store

import (
	"bytes"
	"encoding/gob"
	"fmt"

	kvv1 "dsys/gen/kv/v1"
)

// Machine is the pure state machine: the in-memory map plus the per-client
// deduplication table, and nothing else. No I/O, no locks, no goroutines.
//
// It is NOT safe for concurrent use. The caller serializes every call: in
// Phase 1 that is Store, which holds mu around Apply, Dedup, Get and Len; in
// Phase 2 it is the Raft apply loop, which feeds committed entries in one
// goroutine, in log order.
//
// Everything Raft needs from a state machine lives here: a deterministic
// Apply, and Snapshot/Restore so a lagging follower can be caught up with a
// copy of the state instead of the whole log.
type Machine struct {
	data map[string][]byte

	// sessions is the deduplication table: for each client, the highest
	// request id applied so far and the result it produced. A retry with the
	// same id gets that result back without being applied again.
	//
	// It is updated inside Apply, and Apply runs on replay, so a restart
	// rebuilds it from the log exactly like it rebuilds data. It is also part
	// of the snapshot, for the same reason: a follower restored from a
	// snapshot must recognise the same retries as one that replayed the log.
	//
	// Assumption: one outstanding request per client at a time, so ids
	// arrive in increasing order and remembering only the latest suffices.
	// This is the same assumption the MIT 6.5840 labs make. Relaxing it
	// means remembering a window of ids per client, and bounding it.
	sessions map[string]session
}

// session is what we remember per client.
type session struct {
	lastID     uint64
	lastResult Result
}

// Result is what applying an entry produces. It is returned to the caller
// and remembered per client so a retried request gets the same answer.
type Result struct {
	Existed bool   // Delete
	Swapped bool   // CAS
	Current []byte // CAS: value at decision time (nil if absent)
	Value   []byte // Get
	Found   bool   // Get
}

// SessionState is the on-the-wire form of a session inside a snapshot.
// Exported only because encoding/gob requires exported fields.
type SessionState struct {
	LastID     uint64
	LastResult Result
}

// snapshotVersion is bumped whenever the encoding changes shape, so an old
// binary refuses a snapshot it cannot interpret instead of misreading it.
const snapshotVersion uint32 = 1

// snapshot is the gob-encoded image of a Machine.
type snapshot struct {
	Version  uint32
	Data     map[string][]byte
	Sessions map[string]SessionState
}

// NewMachine returns an empty machine.
func NewMachine() *Machine {
	return &Machine{
		data:     make(map[string][]byte),
		sessions: make(map[string]session),
	}
}

// Get returns a copy of the value for key, so callers cannot mutate machine
// state through the returned slice.
func (m *Machine) Get(key string) ([]byte, bool) {
	v, ok := m.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Len returns the number of live keys.
func (m *Machine) Len() int { return len(m.data) }

// Dedup reports whether the request described by meta has already been
// applied (done=true, with its original result) or is stale (err != nil).
// Requests without meta are always applied; that is the caller opting out
// of exactly-once.
func (m *Machine) Dedup(meta *kvv1.RequestMeta) (r Result, done bool, err error) {
	if meta == nil || meta.ClientId == "" {
		return Result{}, false, nil
	}
	sess, ok := m.sessions[meta.ClientId]
	if !ok {
		return Result{}, false, nil
	}
	switch {
	case meta.RequestId == sess.lastID:
		return sess.lastResult, true, nil
	case meta.RequestId < sess.lastID:
		return Result{}, false, ErrStaleRequest
	}
	return Result{}, false, nil
}

// Apply mutates in-memory state. Must be deterministic: the same entry
// applied to the same state must always produce the same result. No
// wall-clock reads, no randomness, no map iteration order in here.
//
// Apply is itself idempotent with respect to RequestMeta: applying the same
// (client, request) twice mutates once. That makes the log tolerant of
// duplicate entries, which matters because a duplicate can land in the same
// batch as its original (both passed the fast path before either was
// applied), and again in Raft, where a leader change can cause a client's
// retry to be logged a second time.
func (m *Machine) Apply(e *kvv1.LogEntry) Result {
	if r, done, err := m.Dedup(e.Meta); done || err != nil {
		// Stale ids are also skipped here: on replay there is nobody to
		// return an error to, and skipping is the only deterministic choice.
		return r
	}
	r := m.applyOp(e)
	if e.Meta != nil && e.Meta.ClientId != "" {
		m.sessions[e.Meta.ClientId] = session{lastID: e.Meta.RequestId, lastResult: r}
	}
	return r
}

// ApplyChecked is Apply for callers with someone waiting on the result. It
// applies exactly as Apply does (stale entries are skipped, deterministically),
// but reports a skipped stale entry as ErrStaleRequest instead of an empty
// Result. Without that, a replicated server whose log held a stale request
// (two in-flight requests from one identity, applied newest first) told the
// waiting caller its write succeeded when it was never applied.
func (m *Machine) ApplyChecked(e *kvv1.LogEntry) (Result, error) {
	if _, _, err := m.Dedup(e.Meta); err != nil {
		return Result{}, err
	}
	return m.Apply(e), nil
}

// applyOp performs the state change for one entry.
func (m *Machine) applyOp(e *kvv1.LogEntry) Result {
	switch e.Op {
	case kvv1.Op_OP_PUT:
		m.data[e.Key] = e.Value
		return Result{}
	case kvv1.Op_OP_DELETE:
		_, existed := m.data[e.Key]
		delete(m.data, e.Key)
		return Result{Existed: existed}
	case kvv1.Op_OP_CAS:
		// This runs both live and on replay, and must reach the same decision
		// both times. It does, because the decision depends only on
		// (current state, entry), and replay reproduces both in order.
		cur, present := m.data[e.Key]
		var matches bool
		if e.ExpectAbsent {
			matches = !present
		} else {
			matches = present && bytes.Equal(cur, e.Expected)
		}
		// Report what was there at decision time, copied so the caller
		// cannot alias our map. On a failed CAS this lets a client retry
		// with the right expectation instead of doing a separate Get.
		res := Result{}
		if present {
			res.Current = append([]byte(nil), cur...)
		}
		if matches {
			m.data[e.Key] = e.Value
			res.Swapped = true
		}
		return res
	case kvv1.Op_OP_GET:
		// A logged read. It changes no data, but it takes a place in the log
		// so its answer reflects every write ordered before it, which is what
		// makes it linearizable on a replica. Like every other op it records
		// its session entry (in Apply, after we return), so a retried read
		// gets the same answer the original did rather than a newer one.
		v, found := m.Get(e.Key)
		return Result{Value: v, Found: found}
	default:
		// An unknown op in the log means a newer binary wrote it. Crashing
		// loudly is better than silently skipping a write.
		panic(fmt.Sprintf("store: unknown op %v in log", e.Op))
	}
}

// Snapshot encodes the whole machine, data and sessions, into a byte slice
// that Restore accepts. The encoding is not byte-for-byte deterministic
// (gob walks maps in Go's randomized iteration order), so compare machines
// by their observable state, not by snapshot bytes.
func (m *Machine) Snapshot() ([]byte, error) {
	img := snapshot{
		Version:  snapshotVersion,
		Data:     m.data,
		Sessions: make(map[string]SessionState, len(m.sessions)),
	}
	for id, s := range m.sessions {
		img.Sessions[id] = SessionState{LastID: s.lastID, LastResult: s.lastResult}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&img); err != nil {
		return nil, fmt.Errorf("store: encode snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces all machine state with the contents of a snapshot
// produced by Snapshot. On error the machine is left unchanged.
func (m *Machine) Restore(b []byte) error {
	var img snapshot
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&img); err != nil {
		return fmt.Errorf("store: decode snapshot: %w", err)
	}
	if img.Version != snapshotVersion {
		return fmt.Errorf("store: snapshot version %d, want %d", img.Version, snapshotVersion)
	}
	// gob leaves nil maps nil when the encoded map was empty; normalise so
	// the rest of the machine never has to check.
	data := img.Data
	if data == nil {
		data = make(map[string][]byte)
	}
	sessions := make(map[string]session, len(img.Sessions))
	for id, s := range img.Sessions {
		sessions[id] = session{lastID: s.LastID, lastResult: s.LastResult}
	}
	m.data = data
	m.sessions = sessions
	return nil
}
