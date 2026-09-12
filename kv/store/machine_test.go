package store

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	kvv1 "dsys/gen/kv/v1"
)

func put(k, v string, m *kvv1.RequestMeta) *kvv1.LogEntry {
	return &kvv1.LogEntry{Op: kvv1.Op_OP_PUT, Key: k, Value: []byte(v), Meta: m}
}
func del(k string, m *kvv1.RequestMeta) *kvv1.LogEntry {
	return &kvv1.LogEntry{Op: kvv1.Op_OP_DELETE, Key: k, Meta: m}
}
func cas(k, exp string, absent bool, v string, m *kvv1.RequestMeta) *kvv1.LogEntry {
	e := &kvv1.LogEntry{Op: kvv1.Op_OP_CAS, Key: k, ExpectAbsent: absent, Value: []byte(v), Meta: m}
	if !absent {
		e.Expected = []byte(exp)
	}
	return e
}
func get(k string, m *kvv1.RequestMeta) *kvv1.LogEntry {
	return &kvv1.LogEntry{Op: kvv1.Op_OP_GET, Key: k, Meta: m}
}

func TestMachineApplyPut(t *testing.T) {
	m := NewMachine()
	if r := m.Apply(put("a", "1", nil)); !resultEqual(r, Result{}) {
		t.Fatalf("put result = %+v, want zero", r)
	}
	if v, ok := m.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("get a = %q,%v", v, ok)
	}
	m.Apply(put("a", "2", nil))
	if v, _ := m.Get("a"); string(v) != "2" {
		t.Fatalf("overwrite: a=%q", v)
	}
	if m.Len() != 1 {
		t.Fatalf("len=%d", m.Len())
	}
}

func TestMachineApplyDelete(t *testing.T) {
	m := NewMachine()
	if r := m.Apply(del("a", nil)); r.Existed {
		t.Fatal("delete of absent key reported existed")
	}
	m.Apply(put("a", "1", nil))
	if r := m.Apply(del("a", nil)); !r.Existed {
		t.Fatal("delete of present key reported not existed")
	}
	if _, ok := m.Get("a"); ok || m.Len() != 0 {
		t.Fatal("key still present after delete")
	}
}

func TestMachineApplyCAS(t *testing.T) {
	m := NewMachine()
	// Absent, expectAbsent: swap, no current.
	r := m.Apply(cas("lock", "", true, "A", nil))
	if !r.Swapped || r.Current != nil {
		t.Fatalf("1: %+v", r)
	}
	// Present, expectAbsent: no swap, current=A.
	r = m.Apply(cas("lock", "", true, "B", nil))
	if r.Swapped || string(r.Current) != "A" {
		t.Fatalf("2: %+v", r)
	}
	// Wrong expected: no swap.
	r = m.Apply(cas("lock", "Z", false, "B", nil))
	if r.Swapped || string(r.Current) != "A" {
		t.Fatalf("3: %+v", r)
	}
	// Right expected: swap, current is the pre-swap value.
	r = m.Apply(cas("lock", "A", false, "B", nil))
	if !r.Swapped || string(r.Current) != "A" {
		t.Fatalf("4: %+v", r)
	}
	if v, _ := m.Get("lock"); string(v) != "B" {
		t.Fatalf("final=%q", v)
	}
	// Current must be a copy, not an alias of the map's slice.
	r.Current[0] = 'X'
	if v, _ := m.Get("lock"); string(v) != "B" {
		t.Fatalf("CAS result aliased machine state: %q", v)
	}
}

func TestMachineApplyGet(t *testing.T) {
	m := NewMachine()
	r := m.Apply(get("a", nil))
	if r.Found || r.Value != nil {
		t.Fatalf("get of absent key: %+v", r)
	}
	m.Apply(put("a", "1", nil))
	r = m.Apply(get("a", nil))
	if !r.Found || string(r.Value) != "1" {
		t.Fatalf("get of present key: %+v", r)
	}
	// A logged read does not mutate data...
	if m.Len() != 1 {
		t.Fatalf("len=%d after get", m.Len())
	}
	r.Value[0] = 'X'
	if v, _ := m.Get("a"); string(v) != "1" {
		t.Fatalf("get result aliased machine state: %q", v)
	}
	// ...but it does record a session like any other op: a retried read
	// gets the original answer even if the key changed in between.
	first := m.Apply(get("a", meta("c", 1)))
	m.Apply(put("a", "2", meta("other", 1)))
	retry := m.Apply(get("a", meta("c", 1)))
	if !retry.Found || string(retry.Value) != string(first.Value) || string(retry.Value) != "1" {
		t.Fatalf("retried get: first=%+v retry=%+v", first, retry)
	}
	if r, done, err := m.Dedup(meta("c", 1)); !done || err != nil || string(r.Value) != "1" {
		t.Fatalf("dedup after get: %+v done=%v err=%v", r, done, err)
	}
}

func TestMachineDedupAndIdempotentApply(t *testing.T) {
	m := NewMachine()
	if _, done, err := m.Dedup(nil); done || err != nil {
		t.Fatal("nil meta must not dedup")
	}
	if _, done, err := m.Dedup(meta("c", 1)); done || err != nil {
		t.Fatal("unknown client must not dedup")
	}
	m.Apply(put("a", "1", meta("c", 1)))
	r1 := m.Apply(del("a", meta("c", 2)))
	if !r1.Existed {
		t.Fatal("setup")
	}
	// Retry: same (client, id) applied again returns the original result
	// and does not mutate.
	m.Apply(put("a", "again", meta("d", 1)))
	if r := m.Apply(del("a", meta("c", 2))); !r.Existed {
		t.Fatal("retry must return original Existed=true")
	}
	if v, ok := m.Get("a"); !ok || string(v) != "again" {
		t.Fatalf("retry was re-applied: a=%q,%v", v, ok)
	}
	// Dedup fast path agrees, and a stale id is an error there...
	if r, done, err := m.Dedup(meta("c", 2)); !done || err != nil || !r.Existed {
		t.Fatalf("dedup: %+v %v %v", r, done, err)
	}
	if _, _, err := m.Dedup(meta("c", 1)); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("stale dedup err=%v", err)
	}
	// ...while Apply silently skips it (on replay there is nobody to tell).
	m.Apply(put("a", "stale", meta("c", 1)))
	if v, _ := m.Get("a"); string(v) != "again" {
		t.Fatalf("stale entry was applied: a=%q", v)
	}
	// A newer id for the same client is applied normally.
	if r := m.Apply(del("a", meta("c", 3))); !r.Existed {
		t.Fatal("newer id not applied")
	}
}

func TestMachineApplyUnknownOpPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unknown op")
		}
	}()
	NewMachine().Apply(&kvv1.LogEntry{Op: kvv1.Op(99), Key: "a"})
}

// sequence is a mixed workload with overlapping keys, sessions, retries and
// stale ids, so determinism covers every branch of Apply.
func sequence() []*kvv1.LogEntry {
	var es []*kvv1.LogEntry
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i%10)
		c := fmt.Sprintf("c%d", i%3)
		es = append(es,
			put(k, fmt.Sprint(i), meta(c, uint64(i))),
			cas(k, fmt.Sprint(i), i%4 == 0, fmt.Sprint(i+1), meta(c, uint64(i+1))),
			get(k, meta(c, uint64(i+2))),
			del(fmt.Sprintf("k%d", (i+5)%10), meta(c, uint64(i+3))),
			// Retry of the CAS and a stale id, both no-ops.
			cas(k, fmt.Sprint(i), i%4 == 0, fmt.Sprint(i+1), meta(c, uint64(i+1))),
			put(k, "stale", meta(c, 0)),
			put(fmt.Sprintf("anon%d", i), "x", nil),
		)
	}
	return es
}

// assertSameState compares two machines by their observable state: every
// key of each is present in the other with the same value, and the dedup
// table answers identically for every session the sequence used. We do NOT
// compare Snapshot bytes: gob encodes maps in Go's randomized iteration
// order, so two identical machines can legitimately produce different bytes.
func assertSameState(t *testing.T, a, b *Machine, es []*kvv1.LogEntry) {
	t.Helper()
	if a.Len() != b.Len() {
		t.Fatalf("len %d != %d", a.Len(), b.Len())
	}
	for _, e := range es {
		va, oka := a.Get(e.Key)
		vb, okb := b.Get(e.Key)
		if oka != okb || !bytes.Equal(va, vb) {
			t.Fatalf("key %q: a=%q,%v b=%q,%v", e.Key, va, oka, vb, okb)
		}
		if e.Meta == nil {
			continue
		}
		ra, da, erra := a.Dedup(e.Meta)
		rb, db, errb := b.Dedup(e.Meta)
		if da != db || !errors.Is(erra, errb) || !resultEqual(ra, rb) {
			t.Fatalf("dedup %v: a=(%+v,%v,%v) b=(%+v,%v,%v)", e.Meta, ra, da, erra, rb, db, errb)
		}
	}
}

func resultEqual(a, b Result) bool {
	return a.Existed == b.Existed && a.Swapped == b.Swapped && a.Found == b.Found &&
		bytes.Equal(a.Current, b.Current) && bytes.Equal(a.Value, b.Value)
}

func TestMachineDeterministic(t *testing.T) {
	es := sequence()
	a, b := NewMachine(), NewMachine()
	for i, e := range es {
		ra, rb := a.Apply(e), b.Apply(e)
		if !resultEqual(ra, rb) {
			t.Fatalf("entry %d: results differ: %+v vs %+v", i, ra, rb)
		}
	}
	assertSameState(t, a, b, es)
}

func TestMachineSnapshotRestoreRoundTrip(t *testing.T) {
	es := sequence()
	src := NewMachine()
	for _, e := range es {
		src.Apply(e)
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Restore replaces state, so pre-populate the target with junk.
	dst := NewMachine()
	dst.Apply(put("junk", "junk", meta("junk", 1)))
	if err := dst.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, ok := dst.Get("junk"); ok {
		t.Fatal("Restore did not replace existing data")
	}
	if _, done, _ := dst.Dedup(meta("junk", 1)); done {
		t.Fatal("Restore did not replace existing sessions")
	}
	assertSameState(t, src, dst, es)

	// Sessions survive: a retry against the restored machine is deduped
	// (original result, no mutation), exactly as it would be on src.
	src.Apply(put("k1", "v", meta("c0", 5000)))
	orig := src.Apply(del("k1", meta("c0", 5001)))
	if !orig.Existed {
		t.Fatal("setup")
	}
	snap, err = src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	dst = NewMachine()
	if err := dst.Restore(snap); err != nil {
		t.Fatal(err)
	}
	dst.Apply(put("k1", "newer", meta("other", 1)))
	if r := dst.Apply(del("k1", meta("c0", 5001))); !resultEqual(r, orig) {
		t.Fatalf("retry after restore: got %+v want %+v", r, orig)
	}
	if v, ok := dst.Get("k1"); !ok || string(v) != "newer" {
		t.Fatalf("retry after restore was re-applied: k1=%q,%v", v, ok)
	}

	// Snapshot after restore round-trips again (Restore left usable maps).
	if _, err := dst.Snapshot(); err != nil {
		t.Fatal(err)
	}
}

func TestMachineSnapshotEmpty(t *testing.T) {
	snap, err := NewMachine().Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	m := NewMachine()
	m.Apply(put("a", "1", nil))
	if err := m.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if m.Len() != 0 {
		t.Fatalf("len=%d after restoring empty snapshot", m.Len())
	}
	// Machine must still be writable after restoring an empty image.
	m.Apply(put("b", "2", meta("c", 1)))
	if v, _ := m.Get("b"); string(v) != "2" {
		t.Fatalf("b=%q", v)
	}
}

func TestMachineRestoreGarbage(t *testing.T) {
	m := NewMachine()
	m.Apply(put("keep", "me", meta("c", 1)))
	for _, b := range [][]byte{nil, {}, []byte("not a gob stream"), {0xff, 0xfe, 0x01, 0x02, 0x03}} {
		if err := m.Restore(b); err == nil {
			t.Fatalf("Restore(%q) = nil, want error", b)
		}
	}
	// A failed Restore leaves the machine untouched.
	if v, ok := m.Get("keep"); !ok || string(v) != "me" {
		t.Fatalf("state lost after failed restore: %q,%v", v, ok)
	}
	if _, done, _ := m.Dedup(meta("c", 1)); !done {
		t.Fatal("sessions lost after failed restore")
	}
}
