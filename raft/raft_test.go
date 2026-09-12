package raft_test

// raft_test.go: tests adapted from MIT 6.5840 Lab 3 (3A election, 3B log
// replication, 3C persistence, 3D snapshots). They are timing sensitive and
// must not run in parallel. Each is bounded to well under a minute.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ===========================================================================
// 3A: leader election
// ===========================================================================

func TestInitialElection(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3A): initial election")

	// Is a leader elected?
	c.checkOneLeader()

	// Sleep a bit to avoid racing with followers learning of the election,
	// then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, but should be at least 1", term1)
	}

	// Does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * raftElectionTimeout)
	term2 := c.checkTerms()
	if term1 != term2 {
		t.Logf("warning: term changed from %d to %d even though there were no failures", term1, term2)
	}

	// There should still be a leader.
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3A): election after network failure")

	leader1 := c.checkOneLeader()

	// If the leader disconnects, a new one should be elected.
	c.disconnect(leader1)
	c.checkOneLeader()

	// If the old leader rejoins, that should not disturb the new leader,
	// and the old leader should switch to follower.
	c.connect(leader1)
	leader2 := c.checkOneLeader()
	if _, isLeader := c.raft(leader1).GetState(); isLeader && leader1 != leader2 {
		t.Fatalf("old leader %d still thinks it is leader after rejoining; current leader is %d", leader1, leader2)
	}

	// If there is no quorum, no new leader should be elected.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * raftElectionTimeout)
	c.checkNoLeader()

	// If a quorum arises, it should elect a leader.
	c.connect((leader2 + 1) % servers)
	c.checkOneLeader()

	// Re-join of last node should not prevent leader from existing.
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	servers := 7
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3A): multiple elections")

	c.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// Disconnect three nodes.
		i1 := rand.Intn(servers)
		i2 := rand.Intn(servers)
		i3 := rand.Intn(servers)
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)

		// Either the current leader should still be alive, or the remaining
		// four should elect a new one.
		c.checkOneLeader()

		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}

	c.checkOneLeader()
}

// ===========================================================================
// 3B: log replication
// ===========================================================================

func TestBasicAgree(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): basic agreement")

	iters := 3
	for index := uint64(1); index <= uint64(iters); index++ {
		nd, _ := c.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some servers have committed index %d before Start()", index)
		}
		xindex := c.one(cmd(int(index)*100), servers, false)
		if xindex != index {
			t.Fatalf("got index %d but expected %d", xindex, index)
		}
	}
}

// TestRPCBytes checks that each command is sent to each follower about
// once; a leader that re-sends its whole log on every AppendEntries fails.
func TestRPCBytes(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): RPC byte count")

	c.one(cmd(99), servers, false)
	bytes0 := c.bytesTotal()

	iters := 10
	var sent int64
	for index := uint64(2); index < uint64(iters+2); index++ {
		payload := make([]byte, 5000)
		for i := range payload {
			payload[i] = byte('a' + rand.Intn(26))
		}
		xindex := c.one(payload, servers, false)
		if xindex != index {
			t.Fatalf("got index %d but expected %d", xindex, index)
		}
		sent += int64(len(payload))
	}

	bytes1 := c.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %d, expected at most %d", got, expected+50000)
	}
}

func TestFollowerFailure(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): test progressive failure of followers")

	c.one(cmd(101), servers, false)

	// Disconnect one follower from the network.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 1) % servers)

	// The leader and remaining follower should be able to agree despite
	// the disconnected follower.
	c.one(cmd(102), servers-1, false)
	time.Sleep(raftElectionTimeout)
	c.one(cmd(103), servers-1, false)

	// Disconnect the remaining follower.
	leader2 := c.checkOneLeader()
	c.disconnect((leader2 + 1) % servers)
	c.disconnect((leader2 + 2) % servers)

	// Submit a command.
	index, _, ok := c.raft(leader2).Start(cmd(104))
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %d", index)
	}

	time.Sleep(2 * raftElectionTimeout)

	// Check that command 104 did not commit.
	n, _ := c.nCommitted(index)
	if n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}
}

func TestLeaderFailure(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): test failure of leaders")

	c.one(cmd(101), servers, false)

	// Disconnect the first leader.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)

	// The remaining followers should elect a new leader.
	c.one(cmd(102), servers-1, false)
	time.Sleep(raftElectionTimeout)
	c.one(cmd(103), servers-1, false)

	// Disconnect the new leader.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)

	// Submit a command to each server.
	for i := 0; i < servers; i++ {
		if rf := c.raft(i); rf != nil {
			rf.Start(cmd(104))
		}
	}

	time.Sleep(2 * raftElectionTimeout)

	// Check that command 104 did not commit.
	n, _ := c.nCommitted(4)
	if n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}
}

// TestFailAgree: a follower that disconnects and reconnects catches up.
func TestFailAgree(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): agreement after follower reconnects")

	c.one(cmd(101), servers, false)

	// Disconnect one follower from the network.
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % servers)

	// The leader and remaining follower should be able to agree despite
	// the disconnected follower.
	c.one(cmd(102), servers-1, false)
	c.one(cmd(103), servers-1, false)
	time.Sleep(raftElectionTimeout)
	c.one(cmd(104), servers-1, false)
	c.one(cmd(105), servers-1, false)

	// Re-connect.
	c.connect((leader + 1) % servers)

	// The full set of servers should preserve previous agreements, and be
	// able to agree on new commands.
	c.one(cmd(106), servers, true)
	time.Sleep(raftElectionTimeout)
	c.one(cmd(107), servers, true)
}

func TestFailNoAgree(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): no agreement if too many followers disconnect")

	c.one(cmd(10), servers, false)

	// 3 of 5 followers disconnect.
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % servers)
	c.disconnect((leader + 2) % servers)
	c.disconnect((leader + 3) % servers)

	index, _, ok := c.raft(leader).Start(cmd(20))
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %d", index)
	}

	time.Sleep(2 * raftElectionTimeout)

	n, _ := c.nCommitted(index)
	if n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}

	// Repair.
	c.connect((leader + 1) % servers)
	c.connect((leader + 2) % servers)
	c.connect((leader + 3) % servers)

	// The disconnected majority may have chosen a leader from among their
	// own ranks, forgetting index 2.
	leader2 := c.checkOneLeader()
	index2, _, ok2 := c.raft(leader2).Start(cmd(30))
	if !ok2 {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %d", index2)
	}

	c.one(cmd(1000), servers, true)
}

func TestConcurrentStarts(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): concurrent Start()s")

	success := false
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// Give solution some time to settle.
			time.Sleep(3 * time.Second)
		}

		leader := c.checkOneLeader()
		_, term, ok := c.raft(leader).Start(cmd(1))
		if !ok {
			// Leader moved on really quickly.
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan uint64, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := c.raft(leader).Start(cmd(100 + i))
				if term1 != term || !ok {
					return
				}
				is <- idx
			}(ii)
		}
		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t2, _ := c.raft(j).GetState(); t2 != term {
				// Term changed -- can't expect low RPC counts.
				continue loop
			}
		}

		failed := false
		var cmds [][]byte
		for index := range is {
			got, ok := c.wait(index, servers, term)
			if !ok {
				// Peers have moved on to later terms; retry.
				failed = true
				break
			}
			cmds = append(cmds, got)
		}
		if failed {
			continue loop
		}

		for ii := 0; ii < iters; ii++ {
			x := cmd(100 + ii)
			found := false
			for _, got := range cmds {
				if bytes.Equal(got, x) {
					found = true
				}
			}
			if !found {
				t.Fatalf("cmd %q missing in %q", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}
}

func TestRejoin(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): rejoin of partitioned leader")

	c.one(cmd(101), servers, true)

	// Leader network failure.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)

	// Make old leader try to agree on some entries.
	c.raft(leader1).Start(cmd(102))
	c.raft(leader1).Start(cmd(103))
	c.raft(leader1).Start(cmd(104))

	// New leader commits, also for index=2.
	c.one(cmd(103), 2, true)

	// New leader network failure.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)

	// Old leader connected again.
	c.connect(leader1)

	c.one(cmd(104), 2, true)

	// All together now.
	c.connect(leader2)

	c.one(cmd(105), servers, true)
}

// TestBackup: a leader must quickly bring a follower with a long tail of
// uncommitted entries back into sync (log backtracking).
func TestBackup(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): leader backs up quickly over incorrect follower logs")

	c.one(randCmd(), servers, true)

	// Put leader and one follower in a partition.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % servers)
	c.disconnect((leader1 + 3) % servers)
	c.disconnect((leader1 + 4) % servers)

	// Submit lots of commands that won't commit.
	for i := 0; i < 50; i++ {
		c.raft(leader1).Start(randCmd())
	}

	time.Sleep(raftElectionTimeout / 2)

	c.disconnect((leader1 + 0) % servers)
	c.disconnect((leader1 + 1) % servers)

	// Allow other partition to recover.
	c.connect((leader1 + 2) % servers)
	c.connect((leader1 + 3) % servers)
	c.connect((leader1 + 4) % servers)

	// Lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		c.one(randCmd(), 3, true)
	}

	// Now another partitioned leader and one follower.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	c.disconnect(other)

	// Lots more commands that won't commit.
	for i := 0; i < 50; i++ {
		c.raft(leader2).Start(randCmd())
	}

	time.Sleep(raftElectionTimeout / 2)

	// Bring original leader back to life.
	for i := 0; i < servers; i++ {
		c.disconnect(i)
	}
	c.connect((leader1 + 0) % servers)
	c.connect((leader1 + 1) % servers)
	c.connect(other)

	// Lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		c.one(randCmd(), 3, true)
	}

	// Now everyone.
	for i := 0; i < servers; i++ {
		c.connect(i)
	}
	c.one(randCmd(), servers, true)
}

// TestCount: RPC counts must be reasonable -- a handful for an election,
// roughly one per follower per command, and only heartbeats when idle.
func TestCount(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3B): RPC counts aren't too high")

	rpcs := func() int {
		n := 0
		for j := 0; j < servers; j++ {
			n += c.rpcCount(j)
		}
		return n
	}

	leader := c.checkOneLeader()

	total1 := rpcs()
	// checkOneLeader sleeps ~0.5s, during which a 50ms heartbeat to two
	// followers is ~20 RPCs on top of the election itself.
	if total1 > 60 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%d) to elect initial leader", total1)
	}

	var total2 int
	success := false
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// Give solution some time to settle.
			time.Sleep(3 * time.Second)
		}

		leader = c.checkOneLeader()
		total1 = rpcs()
		tStart := time.Now()

		iters := 10
		starti, term, ok := c.raft(leader).Start(cmd(1))
		if !ok {
			// Leader moved on really quickly.
			continue
		}
		var cmds [][]byte
		for i := 1; i < iters+2; i++ {
			x := randCmd()
			cmds = append(cmds, x)
			index1, term1, ok := c.raft(leader).Start(x)
			if term1 != term {
				// Term changed while starting.
				continue loop
			}
			if !ok {
				// No longer the leader, and hence term changed.
				continue loop
			}
			if starti+uint64(i) != index1 {
				t.Fatalf("Start() failed: expected index %d, got %d", starti+uint64(i), index1)
			}
		}

		for i := 1; i < iters+1; i++ {
			got, ok := c.wait(starti+uint64(i), servers, term)
			if !ok {
				// Term changed -- try again.
				continue loop
			}
			if !bytes.Equal(got, cmds[i-1]) {
				t.Fatalf("wrong value %q committed for index %d; expected %q", got, starti+uint64(i), cmds[i-1])
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t2, _ := c.raft(j).GetState(); t2 != term {
				// Term changed -- can't expect low RPC counts.
				// Need to keep going to update total2.
				failed = true
			}
			total2 += c.rpcCount(j)
		}
		if failed {
			continue loop
		}

		// One AppendEntries per follower per entry, plus heartbeats for the
		// time it took, plus a little slack.
		elapsed := time.Since(tStart)
		limit := (iters+1+3)*3 + (servers-1)*int(elapsed/heartbeatInterval+1)
		if total2-total1 > limit {
			t.Fatalf("too many RPCs (%d) for %d entries (limit %d)", total2-total1, iters, limit)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(raftElectionTimeout)

	total3 := rpcs()
	// Idle: heartbeats only. Two followers at 50ms is 40/s; allow 1.5x.
	idleLimit := (servers-1)*int(raftElectionTimeout/heartbeatInterval)*3/2 + 10
	if total3-total2 > idleLimit {
		t.Fatalf("too many RPCs (%d) for 1 second of idleness (limit %d)", total3-total2, idleLimit)
	}
}

// ===========================================================================
// 3C: persistence
// ===========================================================================

func TestPersist1(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3C): basic persistence")

	c.one(cmd(11), servers, true)

	// Crash and re-start all.
	for i := 0; i < servers; i++ {
		c.start1(i, (*cluster).applier)
	}
	for i := 0; i < servers; i++ {
		c.disconnect(i)
		c.connect(i)
	}

	c.one(cmd(12), servers, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.start1(leader1, (*cluster).applier)
	c.connect(leader1)

	c.one(cmd(13), servers, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.one(cmd(14), servers-1, true)
	c.start1(leader2, (*cluster).applier)
	c.connect(leader2)

	c.wait(4, servers, 0) // wait for leader2 to join before killing i3

	i3 := (c.checkOneLeader() + 1) % servers
	c.disconnect(i3)
	c.one(cmd(15), servers-1, true)
	c.start1(i3, (*cluster).applier)
	c.connect(i3)

	c.one(cmd(16), servers, true)
}

func TestPersist2(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		c.one(cmd(10+index), servers, true)
		index++

		leader1 := c.checkOneLeader()

		c.disconnect((leader1 + 1) % servers)
		c.disconnect((leader1 + 2) % servers)

		c.one(cmd(10+index), servers-2, true)
		index++

		c.disconnect((leader1 + 0) % servers)
		c.disconnect((leader1 + 3) % servers)
		c.disconnect((leader1 + 4) % servers)

		c.start1((leader1+1)%servers, (*cluster).applier)
		c.start1((leader1+2)%servers, (*cluster).applier)
		c.connect((leader1 + 1) % servers)
		c.connect((leader1 + 2) % servers)

		time.Sleep(raftElectionTimeout)

		c.start1((leader1+3)%servers, (*cluster).applier)
		c.connect((leader1 + 3) % servers)

		c.one(cmd(10+index), servers-2, true)
		index++

		c.connect((leader1 + 4) % servers)
		c.connect((leader1 + 0) % servers)
	}

	c.one(cmd(1000), servers, true)
}

func TestPersist3(t *testing.T) {
	servers := 3
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3C): partitioned leader and one follower crash, leader restarts")

	c.one(cmd(101), 3, true)

	leader := c.checkOneLeader()
	c.disconnect((leader + 2) % servers)

	c.one(cmd(102), 2, true)

	c.crash1((leader + 0) % servers)
	c.crash1((leader + 1) % servers)
	c.connect((leader + 2) % servers)
	c.start1((leader+0)%servers, (*cluster).applier)
	c.connect((leader + 0) % servers)

	c.one(cmd(103), 2, true)

	c.start1((leader+1)%servers, (*cluster).applier)
	c.connect((leader + 1) % servers)

	c.one(cmd(104), servers, true)
}

// TestFigure8: the scenario of Figure 8 in the paper. A leader must not
// commit an entry from a previous term by counting replicas; it may only
// commit it indirectly, by committing a later entry from its own term.
// Leaders crash repeatedly; at the end everyone must agree.
func TestFigure8(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, false, false)
	defer c.cleanup()
	c.begin("Test (3C): Figure 8")

	c.one(randCmd(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start(randCmd()); ok {
					leader = i
				}
			}
		}

		if rand.Intn(1000) < 100 {
			ms := rand.Int63() % (int64(raftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			c.crash1(leader)
			nup--
		}

		if nup < 3 {
			s := rand.Intn(servers)
			if c.raft(s) == nil {
				c.start1(s, (*cluster).applier)
				c.connect(s)
				nup++
			}
		}
	}

	for i := 0; i < servers; i++ {
		if c.raft(i) == nil {
			c.start1(i, (*cluster).applier)
			c.connect(i)
		}
	}

	c.one(randCmd(), servers, true)
}

func TestUnreliableAgree(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, true, false)
	defer c.cleanup()
	c.begin("Test (3C): unreliable agreement")

	var wg sync.WaitGroup
	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				c.one(cmd(100*iters+j), 1, true)
			}(iters, j)
		}
		c.one(cmd(iters), 1, true)
	}

	c.setUnreliable(false)

	wg.Wait()

	c.one(cmd(100), servers, true)
}

func TestFigure8Unreliable(t *testing.T) {
	servers := 5
	c := newCluster(t, servers, true, false)
	defer c.cleanup()
	c.begin("Test (3C): Figure 8 (unreliable)")

	c.one(cmd(rand.Intn(10000)), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			c.setLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			if _, _, ok := c.raft(i).Start(cmd(rand.Intn(10000))); ok && c.isConnected(i) {
				leader = i
			}
		}

		if rand.Intn(1000) < 100 {
			ms := rand.Int63() % (int64(raftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && rand.Intn(1000) < int(raftElectionTimeout/time.Millisecond)/2 {
			c.disconnect(leader)
			nup--
		}

		if nup < 3 {
			s := rand.Intn(servers)
			if !c.isConnected(s) {
				c.connect(s)
				nup++
			}
		}
	}

	for i := 0; i < servers; i++ {
		if !c.isConnected(i) {
			c.connect(i)
		}
	}

	c.one(cmd(rand.Intn(10000)), servers, true)
}

// internalChurn: concurrent clients submit commands while nodes are
// randomly disconnected, crashed and restarted for ~15s. Afterwards every
// value a client saw committed must be in the final log.
func internalChurn(t *testing.T, unreliable bool) {
	servers := 5
	c := newCluster(t, servers, unreliable, false)
	defer c.cleanup()
	if unreliable {
		c.begin("Test (3C): unreliable churn")
	} else {
		c.begin("Test (3C): churn")
	}

	c.one(randCmd(), 1, true)

	var stop int32

	// Each client submits values and remembers the ones it saw committed.
	cfn := func(me int, ch chan []int) {
		var ret []int
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Intn(1 << 30)
			var index uint64
			ok := false
			for i := 0; i < servers; i++ {
				// Try them all, maybe one of them is a leader.
				if rf := c.raft(i); rf != nil {
					if index1, _, ok1 := rf.Start(cmd(x)); ok1 {
						ok = true
						index = index1
					}
				}
			}
			if ok {
				// Maybe leader will commit our value, maybe not. But don't
				// wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, got := c.nCommitted(index)
					if nd > 0 {
						if xx, isInt := cmdInt(got); isInt {
							if xx == x {
								values = append(values, x)
							}
						} else {
							c.fatalf("wrong command type: %q", got)
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int, 1))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if rand.Intn(1000) < 200 {
			i := rand.Intn(servers)
			c.disconnect(i)
		}

		if rand.Intn(1000) < 500 {
			i := rand.Intn(servers)
			if c.raft(i) == nil {
				c.start1(i, (*cluster).applier)
			}
			c.connect(i)
		}

		if rand.Intn(1000) < 200 {
			i := rand.Intn(servers)
			if c.raft(i) != nil {
				c.crash1(i)
			}
		}

		// Making crash/restart infrequent enough that the peers can keep
		// up, but frequent enough that it is a test.
		time.Sleep((raftElectionTimeout * 7) / 10)
	}

	time.Sleep(raftElectionTimeout)
	c.setUnreliable(false)
	for i := 0; i < servers; i++ {
		if c.raft(i) == nil {
			c.start1(i, (*cluster).applier)
		}
		c.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatalf("client %d failed", i)
		}
		values = append(values, vv...)
	}

	time.Sleep(raftElectionTimeout)

	lastIndex := c.one(randCmd(), servers, true)

	really := map[int]bool{}
	for index := uint64(1); index <= lastIndex; index++ {
		v, _ := c.wait(index, servers, 0)
		if vi, ok := cmdInt(v); ok {
			really[vi] = true
		} else {
			t.Fatalf("not an int: %q", v)
		}
	}

	for _, v1 := range values {
		if !really[v1] {
			t.Fatalf("didn't find a value %d that a client saw committed", v1)
		}
	}
	t.Logf("churn: %d client-observed commits, final log length %d", len(values), lastIndex)
}

func TestReliableChurn(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn(t *testing.T) {
	internalChurn(t, true)
}

// ===========================================================================
// 3D: snapshots
// ===========================================================================

// snapCommon: the applier snapshots every snapshotInterval entries. With
// disconnect, a victim is partitioned while the others commit enough to
// snapshot, so it needs InstallSnapshot on return. With crash, the victim
// is killed and restarted instead (it must recover from its own persisted
// snapshot and then be caught up).
func snapCommon(t *testing.T, name string, iters int, disconnect bool, reliable bool, crash bool) {
	servers := 3
	c := newCluster(t, servers, !reliable, true)
	defer c.cleanup()
	c.begin(name)

	c.one(randCmd(), servers, true)
	leader1 := c.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			c.disconnect(victim)
			c.one(randCmd(), servers-1, true)
		}
		if crash {
			c.crash1(victim)
			c.one(randCmd(), servers-1, true)
		}

		// Perhaps send enough to get a snapshot.
		nn := (snapshotInterval / 2) + rand.Intn(snapshotInterval)
		for j := 0; j < nn; j++ {
			if rf := c.raft(sender); rf != nil {
				rf.Start(randCmd())
			}
		}

		// Let applier threads catch up with the Start()'s.
		if !disconnect && !crash {
			// Make sure all followers have caught up, so that an
			// InstallSnapshot RPC isn't required for TestSnapshotBasic.
			c.one(randCmd(), servers, true)
		} else {
			c.one(randCmd(), servers-1, true)
		}

		if size := c.logSize(); size >= maxLogSize {
			t.Fatalf("log size too large: %d >= %d (snapshots are not trimming the log)", size, maxLogSize)
		}

		if disconnect {
			// Reconnect a follower, who maybe behind and needs to receive a
			// snapshot to catch up.
			c.connect(victim)
			c.one(randCmd(), servers, true)
			leader1 = c.checkOneLeader()
		}
		if crash {
			c.start1(victim, (*cluster).applier)
			c.connect(victim)
			c.one(randCmd(), servers, true)
			leader1 = c.checkOneLeader()
		}
	}
}

func TestSnapshotBasic(t *testing.T) {
	snapCommon(t, "Test (3D): snapshots basic", 30, false, true, false)
}

func TestSnapshotInstall(t *testing.T) {
	snapCommon(t, "Test (3D): install snapshots (disconnect)", 30, true, true, false)
}

func TestSnapshotInstallUnreliable(t *testing.T) {
	snapCommon(t, "Test (3D): install snapshots (disconnect+unreliable)", 25, true, false, false)
}

func TestSnapshotInstallCrash(t *testing.T) {
	snapCommon(t, "Test (3D): install snapshots (crash)", 20, false, true, true)
}

func TestSnapshotInstallUnCrash(t *testing.T) {
	snapCommon(t, "Test (3D): install snapshots (unreliable+crash)", 20, false, false, true)
}
