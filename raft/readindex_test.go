package raft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"dsys/raft"
)

// A connected leader that has committed an entry in its term serves a
// ReadIndex at (at least) that entry's index, within a heartbeat round trip.
func TestReadIndexBasic(t *testing.T) {
	c := newCluster(t, 3, false, false)
	defer c.cleanup()
	c.begin("ReadIndex: basic")

	idx := c.one(cmd(101), 3, false)
	leader := c.checkOneLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ri, err := c.raft(leader).ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex on a healthy leader: %v", err)
	}
	if ri < idx {
		t.Fatalf("ReadIndex %d is behind a committed entry at %d", ri, idx)
	}
	for i := 0; i < 3; i++ {
		if i != leader {
			if _, err := c.raft(i).ReadIndex(ctx); !errors.Is(err, raft.ErrNotLeader) {
				t.Fatalf("follower %d: ReadIndex err = %v, want ErrNotLeader", i, err)
			}
		}
	}
}

// The safety property ReadIndex exists for: a leader cut off from the
// majority must NOT confirm a read, because the majority may already have
// elected a new leader and accepted writes the old one cannot see. Serving
// the read anyway is the classic stale-read bug.
func TestReadIndexDeposedLeaderRefuses(t *testing.T) {
	c := newCluster(t, 5, false, false)
	defer c.cleanup()
	c.begin("ReadIndex: an isolated leader refuses reads")

	c.one(cmd(201), 5, false)
	old := c.checkOneLeader()
	// Cut the leader off from everyone. It still believes it is leader
	// until it hears otherwise, which is exactly the dangerous moment.
	c.disconnect(old)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if ri, err := c.raft(old).ReadIndex(ctx); err == nil {
		t.Fatalf("isolated old leader confirmed a read at index %d; reads could be stale", ri)
	}

	// The majority elects a new leader and keeps writing.
	c.one(cmd(202), 4, true)
}

// A brand-new leader has not committed anything in its own term, so it
// cannot know its commit index is final: ReadIndex must say "not ready" (the
// caller then reads through the log) rather than return a possibly stale
// index. Once it commits an entry in its term, ReadIndex works.
func TestReadIndexNewLeaderNotReadyUntilCommitInTerm(t *testing.T) {
	c := newCluster(t, 3, false, false)
	defer c.cleanup()
	c.begin("ReadIndex: new leader waits for a commit in its term")

	c.one(cmd(301), 3, false)
	old := c.checkOneLeader()
	c.disconnect(old)
	leader := c.checkOneLeader() // new leader, nothing committed in its term yet

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.raft(leader).ReadIndex(ctx); !errors.Is(err, raft.ErrReadIndexNotReady) {
		t.Fatalf("new leader with no commit in its term: ReadIndex err = %v, want ErrReadIndexNotReady", err)
	}
	c.one(cmd(302), 2, true) // commits in the new term
	if _, err := c.raft(leader).ReadIndex(ctx); err != nil {
		t.Fatalf("after a commit in its term, ReadIndex failed: %v", err)
	}
	c.connect(old)
}
