package raft

import "testing"

// appendBatch must respect both caps, always send at least one entry when
// there is one (even an entry larger than the byte cap), and never skip.
func TestAppendBatchCaps(t *testing.T) {
	log := []Entry{{Term: 0}} // sentinel at index 0
	for i := 0; i < 20; i++ {
		log = append(log, Entry{Term: 1, Command: make([]byte, 100)})
	}
	rf := &Raft{log: log, cfg: Config{MaxAppendEntries: 8, MaxAppendBytes: 1 << 20}}
	if got := len(rf.appendBatch(1)); got != 8 {
		t.Fatalf("count cap: got %d entries, want 8", got)
	}
	if got := len(rf.appendBatch(17)); got != 4 {
		t.Fatalf("tail: got %d entries, want the 4 remaining", got)
	}
	if got := len(rf.appendBatch(21)); got != 0 {
		t.Fatalf("nothing to send: got %d entries, want 0 (heartbeat)", got)
	}

	rf.cfg = Config{MaxAppendEntries: 512, MaxAppendBytes: 250}
	if got := len(rf.appendBatch(1)); got != 2 {
		t.Fatalf("byte cap: got %d entries of 100 bytes under a 250-byte cap, want 2", got)
	}
	rf.log = append(rf.log, Entry{Term: 1, Command: make([]byte, 1000)})
	if got := len(rf.appendBatch(21)); got != 1 {
		t.Fatalf("oversized entry: got %d, want it sent alone", got)
	}
}
