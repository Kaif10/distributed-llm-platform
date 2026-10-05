package raft_test

// harness_test.go: a cluster harness for the Raft tests, modelled on MIT
// 6.5840's config.go. It runs n Raft instances over a simnet.Net, records
// what each one applies, and checks State Machine Safety (no two nodes
// apply different commands at the same index) plus in-order, gap-free
// application, in the background. Tests live in package raft_test because
// dsys/raft/simnet imports dsys/raft, so an in-package test would be an
// import cycle.

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"dsys/raft"
	"dsys/raft/simnet"
)

// raftElectionTimeout is the budget tests give the cluster to elect a
// leader; it is comfortably above ElectionTimeoutMax.
const raftElectionTimeout = 1 * time.Second

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond

	// snapshotInterval: in snapshot mode the applier snapshots every this
	// many applied entries.
	snapshotInterval = 10
	// maxLogSize: in snapshot mode, the largest persisted Raft state (bytes)
	// any node may hold. Snapshots must actually discard the log prefix.
	maxLogSize = 2000
)

func testConfig() raft.Config {
	cfg := raft.Config{
		HeartbeatInterval:  heartbeatInterval,
		ElectionTimeoutMin: electionTimeoutMin,
		ElectionTimeoutMax: electionTimeoutMax,
		// Deliberately tiny, so every catch-up in the suite (Figure 8,
		// unreliable churn, snapshots, backtracking) has to go through
		// several capped batches instead of one RPC carrying the whole tail.
		MaxAppendEntries: 8,
	}
	if os.Getenv("RAFT_DEBUG") != "" {
		cfg.Logf = raft.StdLogger
	}
	return cfg
}

// Commands in tests are ints encoded as decimal strings.
func cmd(x int) []byte { return []byte(strconv.Itoa(x)) }

func cmdInt(b []byte) (int, bool) {
	x, err := strconv.Atoi(string(b))
	return x, err == nil
}

func randCmd() []byte { return cmd(rand.Intn(1 << 30)) }

// applierFunc consumes one node's applyCh until stop is closed.
type applierFunc func(c *cluster, i int, rf *raft.Raft, applyCh chan raft.ApplyMsg, stop <-chan struct{})

// harnessSnapshot is what the applier stores when it snapshots: the applied
// prefix of the log.
type harnessSnapshot struct {
	LastIndex uint64
	Log       map[uint64][]byte
}

type cluster struct {
	t  *testing.T
	mu sync.Mutex

	n           int
	net         *simnet.Net
	rafts       []*raft.Raft
	saved       []*raft.MemPersister
	connected   []bool
	stops       []chan struct{}
	logs        []map[uint64][]byte // per node: index -> applied command
	lastApplied []uint64
	applyErr    []string // first invariant violation seen per node
	failure     string   // first fatalf message from any goroutine
	snapshot    bool

	maxIndex  uint64 // highest index applied anywhere
	maxIndex0 uint64 // maxIndex at begin()

	done        chan struct{} // closed by cleanup
	testGoid    uint64
	goroutines0 int
	t0          time.Time
	rpcs0       int
	bytes0      int64
	name        string
}

// newCluster starts n nodes, all connected, over a fresh network.
func newCluster(t *testing.T, n int, unreliable bool, snapshot bool) *cluster {
	c := &cluster{
		t:           t,
		n:           n,
		net:         simnet.New(n),
		rafts:       make([]*raft.Raft, n),
		saved:       make([]*raft.MemPersister, n),
		connected:   make([]bool, n),
		stops:       make([]chan struct{}, n),
		logs:        make([]map[uint64][]byte, n),
		lastApplied: make([]uint64, n),
		applyErr:    make([]string, n),
		snapshot:    snapshot,
		done:        make(chan struct{}),
		testGoid:    goid(),
		goroutines0: runtime.NumGoroutine(),
		t0:          time.Now(),
	}
	c.net.SetUnreliable(unreliable)
	for i := 0; i < n; i++ {
		c.logs[i] = map[uint64][]byte{}
		c.saved[i] = raft.NewMemPersister()
	}
	for i := 0; i < n; i++ {
		c.start1(i, (*cluster).applier)
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	return c
}

// begin names the test in the output and resets the per-test counters.
func (c *cluster) begin(name string) {
	fmt.Printf("%s ...\n", name)
	c.mu.Lock()
	c.name = name
	c.t0 = time.Now()
	c.rpcs0 = c.net.TotalRPCs()
	c.bytes0 = c.net.BytesSent()
	c.maxIndex0 = c.maxIndex
	c.mu.Unlock()
}

// cleanup kills every node, fails the test if a background invariant was
// violated, prints the one-line summary, and loosely checks for leaked
// goroutines.
func (c *cluster) cleanup() {
	c.mu.Lock()
	for i := range c.rafts {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
			c.rafts[i] = nil
		}
		c.net.Bind(i, nil)
		if c.stops[i] != nil {
			close(c.stops[i])
			c.stops[i] = nil
		}
	}
	close(c.done)
	applyErr := append([]string(nil), c.applyErr...)
	failure := c.failure
	name := c.name
	elapsed := time.Since(c.t0)
	rpcs := c.net.TotalRPCs() - c.rpcs0
	nbytes := c.net.BytesSent() - c.bytes0
	cmds := c.maxIndex - c.maxIndex0
	c.mu.Unlock()

	for i, e := range applyErr {
		if e != "" {
			c.t.Errorf("server %d: apply invariant violated: %s", i, e)
		}
	}
	if failure != "" && !c.t.Failed() {
		c.t.Errorf("%s", failure)
	}

	if !c.t.Failed() {
		fmt.Printf("  ... Passed -- %5.1fs  peers=%d  rpcs=%d  bytes=%d  cmds=%d\n",
			elapsed.Seconds(), c.n, rpcs, nbytes, cmds)
	} else {
		fmt.Printf("  ... FAILED -- %5.1fs  %s\n", elapsed.Seconds(), name)
	}

	// Loose leak check: killed nodes and the network's delayed goroutines
	// should wind down within a few seconds. Only warn; delayed replies
	// (long reordering / long delays) can legitimately outlive the test.
	deadline := time.Now().Add(3 * time.Second)
	slack := 4*c.n + 8
	for runtime.NumGoroutine() > c.goroutines0+slack && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if ng := runtime.NumGoroutine(); ng > c.goroutines0+slack {
		c.t.Logf("warning: %d goroutines still running after cleanup (baseline %d); possible leak",
			ng, c.goroutines0)
	}
}

// fatalf fails the test with a message. From the test goroutine it is
// t.Fatalf; from a helper goroutine it marks the test failed and exits that
// goroutine (deferred calls still run), since FailNow must not be called
// off the test goroutine.
func (c *cluster) fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	c.mu.Lock()
	if c.failure == "" {
		c.failure = msg
	}
	c.mu.Unlock()
	if goid() == c.testGoid {
		c.t.Fatalf("%s", msg)
	}
	c.t.Errorf("%s", msg)
	runtime.Goexit()
}

func goid() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}

// ---------------------------------------------------------------------------
// Node lifecycle
// ---------------------------------------------------------------------------

// crash1 kills node i and disconnects it. Its persister is replaced by a
// copy so that nothing the dying instance does afterwards is visible to a
// restart. Its applied log is kept (it still counts toward nCommitted).
func (c *cluster) crash1(i int) {
	c.disconnect(i)
	c.net.Bind(i, nil)

	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.saved[i] != nil {
		c.saved[i] = c.saved[i].Copy()
	}
	if c.stops[i] != nil {
		close(c.stops[i])
		c.stops[i] = nil
	}
	c.mu.Unlock()

	if rf != nil {
		rf.Kill()
	}
}

// start1 (re)starts node i from a copy of its persisted state and binds it
// on the network. It does not reconnect the node; callers do that (as in
// 6.5840, restarted nodes come back partitioned until connect(i)).
func (c *cluster) start1(i int, applier applierFunc) {
	c.crash1(i)

	c.mu.Lock()
	c.saved[i] = c.saved[i].Copy()
	c.logs[i] = map[uint64][]byte{}
	c.lastApplied[i] = 0
	if snap := c.saved[i].ReadSnapshot(); len(snap) > 0 {
		if err := c.ingestSnap(i, snap, 0); err != "" {
			c.mu.Unlock()
			c.fatalf("server %d: restart: %s", i, err)
			return
		}
	}
	peers := make([]raft.Peer, c.n)
	for j := 0; j < c.n; j++ {
		if j != i {
			peers[j] = c.net.Peer(i, j)
		}
	}
	applyCh := make(chan raft.ApplyMsg)
	stop := make(chan struct{})
	c.stops[i] = stop
	rf := raft.New(peers, i, c.saved[i], applyCh, testConfig())
	c.rafts[i] = rf
	c.mu.Unlock()

	c.net.Bind(i, rf)
	go applier(c, i, rf, applyCh, stop)
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Connect(i)
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Disconnect(i)
}

// raft returns node i's live instance, or nil if it is crashed.
func (c *cluster) raft(i int) *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i]
}

func (c *cluster) rpcCount(i int) int   { return c.net.RPCCount(i) }
func (c *cluster) rpcTotal() int        { return c.net.TotalRPCs() }
func (c *cluster) bytesTotal() int64    { return c.net.BytesSent() }
func (c *cluster) setUnreliable(v bool) { c.net.SetUnreliable(v) }
func (c *cluster) setLongReordering(v bool) {
	c.net.SetLongReordering(v)
}

// logSize is the largest persisted Raft state across all nodes.
func (c *cluster) logSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	max := 0
	for _, p := range c.saved {
		if s := p.StateSize(); s > max {
			max = s
		}
	}
	return max
}

// ---------------------------------------------------------------------------
// Applier: records applied commands and checks invariants
// ---------------------------------------------------------------------------

func (c *cluster) recordApplyErr(i int, msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr[i] == "" {
		c.applyErr[i] = msg
		c.t.Logf("server %d: %s", i, msg)
	}
}

// applier is the per-node service. It records every applied command,
// checks State Machine Safety and ordering, and in snapshot mode tells Raft
// to snapshot every snapshotInterval entries and restores itself from
// incoming snapshots. After stop is closed it keeps draining applyCh (so a
// dying instance blocked on a send is released) until cleanup.
func (c *cluster) applier(i int, rf *raft.Raft, applyCh chan raft.ApplyMsg, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			for {
				select {
				case <-applyCh:
				case <-c.done:
					return
				}
			}
		case m := <-applyCh:
			switch {
			case m.SnapshotValid && m.CommandValid:
				c.recordApplyErr(i, "apply message with both CommandValid and SnapshotValid")
			case m.SnapshotValid:
				if !c.snapshot {
					c.recordApplyErr(i, fmt.Sprintf("unexpected snapshot at index %d (service never called Snapshot)", m.SnapshotIndex))
					continue
				}
				c.mu.Lock()
				if stale(stop) {
					c.mu.Unlock()
					continue
				}
				err := c.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
				c.mu.Unlock()
				if err != "" {
					c.recordApplyErr(i, err)
				}
			case m.CommandValid:
				c.mu.Lock()
				if stale(stop) {
					c.mu.Unlock()
					c.t.Logf("server %d: dropped apply of index %d from a crashed instance", i, m.CommandIndex)
					continue
				}
				err := c.checkLogs(i, m)
				var snap []byte
				if c.snapshot && err == "" && m.CommandIndex%snapshotInterval == 0 {
					snap = c.encodeSnapshot(i, m.CommandIndex)
				}
				c.mu.Unlock()
				if err != "" {
					c.recordApplyErr(i, err)
				}
				if snap != nil {
					// Not under c.mu: Snapshot takes Raft's lock and Raft's
					// applier may be blocked sending to us.
					rf.Snapshot(m.CommandIndex, snap)
				}
			default:
				c.recordApplyErr(i, "apply message with neither CommandValid nor SnapshotValid")
			}
		}
	}
}

// stale reports whether this applier's instance has been crashed. Callers
// hold c.mu, and crash1 closes stop under c.mu before start1 resets the
// node's expected apply index, so checking here is race-free. Without it, a
// message the old instance delivered just before crashing could be picked
// by this goroutine's select (Go chooses randomly between ready cases) and
// recorded against the NEW instance's fresh state, reporting a duplicate
// apply that the restarted instance never made.
func stale(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// checkLogs records m for node i and returns a description of any
// invariant it violates. Caller holds c.mu.
func (c *cluster) checkLogs(i int, m raft.ApplyMsg) string {
	err := ""
	if m.CommandIndex != c.lastApplied[i]+1 {
		err = fmt.Sprintf("apply out of order: expected index %d, got %d", c.lastApplied[i]+1, m.CommandIndex)
	}
	for j := 0; j < c.n; j++ {
		if old, ok := c.logs[j][m.CommandIndex]; ok && !bytes.Equal(old, m.Command) {
			// State Machine Safety violated.
			err = fmt.Sprintf("inconsistent apply at index %d: server %d applied %q, server %d applied %q",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	c.logs[i][m.CommandIndex] = append([]byte(nil), m.Command...)
	c.lastApplied[i] = m.CommandIndex
	if m.CommandIndex > c.maxIndex {
		c.maxIndex = m.CommandIndex
	}
	return err
}

// encodeSnapshot serialises node i's applied log through index. Caller
// holds c.mu.
func (c *cluster) encodeSnapshot(i int, index uint64) []byte {
	s := harnessSnapshot{LastIndex: index, Log: map[uint64][]byte{}}
	for idx, v := range c.logs[i] {
		if idx <= index {
			s.Log[idx] = v
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&s); err != nil {
		panic("harness: cannot encode snapshot: " + err.Error())
	}
	return buf.Bytes()
}

// ingestSnap replaces node i's view of the log with the snapshot. index is
// the SnapshotIndex from the apply message, or 0 on restart (unknown).
// Returns "" or an error description. Caller holds c.mu.
func (c *cluster) ingestSnap(i int, data []byte, index uint64) string {
	var s harnessSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return fmt.Sprintf("cannot decode snapshot: %v", err)
	}
	if index != 0 && s.LastIndex != index {
		return fmt.Sprintf("snapshot index %d does not match SnapshotIndex %d", s.LastIndex, index)
	}
	if s.LastIndex < c.lastApplied[i] {
		return fmt.Sprintf("snapshot at index %d delivered after index %d was applied", s.LastIndex, c.lastApplied[i])
	}
	for idx, v := range s.Log {
		for j := 0; j < c.n; j++ {
			if old, ok := c.logs[j][idx]; ok && !bytes.Equal(old, v) {
				return fmt.Sprintf("inconsistent apply (via snapshot) at index %d: server %d has %q, server %d has %q",
					idx, i, v, j, old)
			}
		}
	}
	c.logs[i] = map[uint64][]byte{}
	for idx, v := range s.Log {
		c.logs[i][idx] = v
	}
	c.lastApplied[i] = s.LastIndex
	if s.LastIndex > c.maxIndex {
		c.maxIndex = s.LastIndex
	}
	return ""
}

// ---------------------------------------------------------------------------
// Leader / term / commit checks
// ---------------------------------------------------------------------------

// checkOneLeader polls (up to ~5s) until some connected node claims
// leadership. Two leaders in the same term is fatal. Returns the leader of
// the highest term seen.
func (c *cluster) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)

		leaders := map[uint64][]int{}
		for i := 0; i < c.n; i++ {
			if rf := c.raft(i); rf != nil && c.isConnected(i) {
				if term, isLeader := rf.GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		var lastTerm uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.fatalf("term %d has %d (>1) leaders: %v", term, len(ls), ls)
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if len(leaders) != 0 {
			return leaders[lastTerm][0]
		}
	}
	c.fatalf("expected one leader, got none")
	return -1
}

// checkNoLeader fails if any connected node claims leadership.
func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				c.fatalf("expected no leader among connected servers, but %d claims to be leader", i)
			}
		}
	}
}

// checkTerms returns the term all connected nodes agree on, failing if
// they disagree.
func (c *cluster) checkTerms() uint64 {
	var term uint64
	seen := false
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			t, _ := rf.GetState()
			if !seen {
				term, seen = t, true
			} else if t != term {
				c.fatalf("servers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many nodes have applied index and the command
// there. Crashed nodes count (their applied log is kept until restart).
// Differing commands at the same index is fatal.
func (c *cluster) nCommitted(index uint64) (int, []byte) {
	c.mu.Lock()
	count := 0
	var cmd []byte
	var err string
	for i := 0; i < c.n; i++ {
		if c.applyErr[i] != "" && err == "" {
			err = c.applyErr[i]
		}
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && !bytes.Equal(cmd, v) {
				err = fmt.Sprintf("committed values do not match at index %d: %q vs %q", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	c.mu.Unlock()
	if err != "" {
		c.fatalf("%s", err)
	}
	return count, cmd
}

// wait blocks (bounded, ~30 rounds of growing sleeps) until at least n
// nodes have applied index, and returns the command there. If startTerm is
// nonzero and any node's term moves past it, wait gives up and returns
// ok=false (the caller should retry from scratch).
func (c *cluster) wait(index uint64, n int, startTerm uint64) ([]byte, bool) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := c.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > 0 {
			for i := 0; i < c.n; i++ {
				if rf := c.raft(i); rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return nil, false
					}
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.fatalf("only %d decided for index %d; wanted %d", nd, index, n)
	}
	return cmd, true
}

// one submits cmd to whichever connected node accepts it as leader and
// waits (up to 2s per attempt, 10s overall) for expectedServers to apply it
// at the returned index. With retry=false a single attempt that does not
// commit (e.g. the leader changed) is fatal; with retry=true the command is
// resubmitted, possibly landing at a different index.
func (c *cluster) one(cmd []byte, expectedServers int, retry bool) uint64 {
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second && !c.t.Failed() {
		var index uint64
		for si := 0; si < c.n; si++ {
			starts = (starts + 1) % c.n
			c.mu.Lock()
			var rf *raft.Raft
			if c.connected[starts] {
				rf = c.rafts[starts]
			}
			c.mu.Unlock()
			if rf != nil {
				if idx, _, ok := rf.Start(cmd); ok {
					index = idx
					break
				}
			}
		}
		if index == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		t1 := time.Now()
		for time.Since(t1) < 2*time.Second {
			nd, got := c.nCommitted(index)
			if nd > 0 && nd >= expectedServers && bytes.Equal(got, cmd) {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			c.fatalf("one(%q) failed to reach agreement at index %d", cmd, index)
		}
	}
	if !c.t.Failed() {
		c.fatalf("one(%q) failed to reach agreement", cmd)
	}
	return 0
}
