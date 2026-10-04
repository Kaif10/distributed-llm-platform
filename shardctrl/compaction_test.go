package shardctrl

import (
	"testing"
	"time"

	shardctrlv1 "dsys/gen/shardctrl/v1"
)

// TestLogStaysBoundedUnderQueryLoad is the regression test for the
// controller's Raft log growing without bound. Every Query is a logged
// operation (it must be, to be linearizable), and every shardkv leader
// issues one per poll interval forever, so uptime alone grows the log;
// Raft re-encodes the whole log on every persist, so each later operation,
// Query included, gets slower until pollers' deadlines can no longer be met.
// The persisted Raft state must stay bounded by snapshotting, and history
// (every past Config, the dedup table) must survive compaction, a lagging
// replica catching up by InstallSnapshot, and a full restart.
func TestLogStaysBoundedUnderQueryLoad(t *testing.T) {
	const (
		maxState = 4 << 10
		queries  = 2000
	)
	c := newCluster(t, 3, func(cfg *Config) { cfg.MaxRaftState = maxState })
	cl := c.client("c1")
	ctx, cancel := ctxT(120 * time.Second)
	defer cancel()

	if err := cl.join(ctx, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := cl.join(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if err := cl.move(ctx, 0, 1); err != nil {
		t.Fatal(err)
	}
	latest, err := cl.query(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}

	// Replica 2 misses the whole query storm, so it can only catch up from
	// a snapshot once the others have compacted.
	c.crash(2)

	maxSeen := 0
	for q := 1; q <= queries; q++ {
		got, err := cl.query(ctx, -1)
		if err != nil {
			t.Fatalf("query %d: %v", q, err)
		}
		if got.Num != latest.Num {
			t.Fatalf("query %d: num %d, want %d", q, got.Num, latest.Num)
		}
		if q%100 == 0 {
			for i := 0; i < 2; i++ {
				if sz := c.persisters[i].StateSize(); sz > maxSeen {
					maxSeen = sz
				}
			}
		}
	}
	t.Logf("%d queries: max persisted Raft state %d bytes (MaxRaftState %d)", queries, maxSeen, maxState)
	if maxSeen > 4*maxState {
		t.Fatalf("persisted Raft state reached %d bytes after %d queries; the log is not being compacted (bound %d)",
			maxSeen, queries, 4*maxState)
	}

	// The lagging replica catches up (necessarily via InstallSnapshot).
	c.start(2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := c.server(2)
		s.mu.Lock()
		num := s.m.latest().Num
		s.mu.Unlock()
		if num == latest.Num {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagging replica stuck at config %d, want %d", num, latest.Num)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Full restart: history and dedup state come back from the snapshot.
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	for num := int64(0); num <= latest.Num; num++ {
		got, err := cl.query(ctx, num)
		if err != nil || got.Num != num {
			t.Fatalf("query(%d) after compaction + restart = %v, %v", num, got, err)
		}
	}
	if err := cl.join(ctx, 4); err != nil { // fresh request ids still accepted
		t.Fatal(err)
	}
	// ...and the dedup table survived: a stale request id is still refused.
	stale := &shardctrlv1.LeaveRequest{Meta: &shardctrlv1.RequestMeta{ClientId: "c1", RequestId: 1}, Gids: []int64{99}}
	if err := c.do(ctx, func(s *Server) error { _, err := s.Leave(ctx, stale); return err }); err == nil {
		t.Fatal("stale request id accepted after compaction + restart: dedup table lost")
	}
}
