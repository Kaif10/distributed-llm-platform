// Package simnet is an in-memory simulated network for testing Raft and the
// services built on it, modelled on MIT 6.5840's labrpc.
//
// Every RPC is gob-encoded on the way in and gob-encoded on the way back, so
// a handler never shares memory with its caller (this catches aliasing bugs
// such as a leader mutating an Entries slice it already handed to a
// follower). The network can partition nodes, drop and delay messages, and
// reorder replies. All methods are safe for concurrent use.
package simnet

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dsys/raft"
)

// Net connects n nodes numbered 0..n-1. Node i's inbound handler is
// registered with Bind; node i's outbound Peer for node j comes from
// Peer(i, j). All nodes start connected and unbound.
type Net struct {
	mu             sync.Mutex
	n              int
	handlers       []raft.Handler // nil = unbound (crashed / not started)
	connected      []bool
	unreliable     bool
	longReordering bool
	longDelays     bool

	rngMu sync.Mutex
	rng   *rand.Rand // every fault-injection decision goes through this

	counts []atomic.Int64 // RPCs sent, per source node
	total  atomic.Int64   // RPCs sent, all nodes
	nbytes atomic.Int64   // gob-encoded bytes of args sent and replies delivered
}

// New returns a network for n nodes, with fault injection seeded from the
// current time (the historical behaviour: every run picks different faults).
// Use NewSeeded for a reproducible sequence of faults.
func New(n int) *Net {
	return NewSeeded(n, time.Now().UnixNano())
}

// NewSeeded returns a network for n nodes whose fault-injection decisions
// (which RPCs are delayed, dropped, or reordered, and by how long) are a
// deterministic function of seed: the same seed, replayed against the same
// sequence of RPCs, makes the same decisions in the same order.
//
// This is NOT full determinism in the FoundationDB/TigerBeetle sense. Those
// simulators also virtualize time and single-thread the whole system, so a
// seed reproduces the exact interleaving, byte for byte, forever. Here the
// underlying goroutines still run on the real scheduler with real
// wall-clock sleeps: two runs of the same seed make the same fault
// DECISIONS, but a slow CI machine can still deliver them in a different
// order relative to, say, a Raft election timer that fires on real time.
// What you get is "the same seed almost always reproduces the same bug",
// which is enormously more useful than "reproduces never", and honest about
// where it falls short of the state of the art (see docs/phase6.md).
func NewSeeded(n int, seed int64) *Net {
	sn := &Net{
		n:         n,
		handlers:  make([]raft.Handler, n),
		connected: make([]bool, n),
		counts:    make([]atomic.Int64, n),
		rng:       rand.New(rand.NewSource(seed)),
	}
	for i := range sn.connected {
		sn.connected[i] = true
	}
	return sn
}

// intn is the one place this package reads randomness, so seeding it in one
// place (NewSeeded) governs every fault-injection decision.
func (sn *Net) intn(n int) int {
	sn.rngMu.Lock()
	defer sn.rngMu.Unlock()
	return sn.rng.Intn(n)
}

// Bind registers h as node id's inbound handler. Binding nil detaches the
// node (calls to it get no reply). A node may be re-bound after a restart;
// any in-flight call to the previous handler then fails with ok=false.
func (sn *Net) Bind(id int, h raft.Handler) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.handlers[id] = h
}

// Connect re-attaches node i to the network in both directions.
func (sn *Net) Connect(i int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.connected[i] = true
}

// Disconnect cuts node i off from everyone, in both directions. Calls to
// and from it fail with ok=false after a delay (see SetLongDelays).
func (sn *Net) Disconnect(i int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.connected[i] = false
}

// IsConnected reports whether node i is attached.
func (sn *Net) IsConnected(i int) bool {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return sn.connected[i]
}

// SetUnreliable: drop ~10% of requests, delay delivered requests by
// 0-27ms, drop ~10% of replies.
func (sn *Net) SetUnreliable(v bool) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.unreliable = v
}

// SetLongReordering: with probability 2/3, delay a reply by 200-2200ms so
// replies arrive out of order with respect to later requests.
func (sn *Net) SetLongReordering(v bool) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.longReordering = v
}

// SetLongDelays: calls that cannot be delivered (partition, unbound target)
// wait up to 7s before failing instead of up to 100ms, like labrpc.
func (sn *Net) SetLongDelays(v bool) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.longDelays = v
}

// RPCCount returns the number of RPCs node i has attempted to send
// (delivered or not).
func (sn *Net) RPCCount(i int) int { return int(sn.counts[i].Load()) }

// TotalRPCs returns the number of RPCs attempted by all nodes.
func (sn *Net) TotalRPCs() int { return int(sn.total.Load()) }

// BytesSent returns the total gob-encoded size of all RPC arguments sent
// plus all replies delivered.
func (sn *Net) BytesSent() int64 { return sn.nbytes.Load() }

// Peer returns node from's outbound endpoint for node to.
func (sn *Net) Peer(from, to int) raft.Peer {
	return &peer{net: sn, from: from, to: to}
}

// peer is one directed endpoint from -> to.
type peer struct {
	net      *Net
	from, to int
}

func (p *peer) RequestVote(args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	return call(p, args, func(h raft.Handler, a *raft.RequestVoteArgs) *raft.RequestVoteReply {
		return h.HandleRequestVote(a)
	})
}

func (p *peer) AppendEntries(args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	return call(p, args, func(h raft.Handler, a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
		return h.HandleAppendEntries(a)
	})
}

func (p *peer) InstallSnapshot(args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, bool) {
	return call(p, args, func(h raft.Handler, a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
		return h.HandleInstallSnapshot(a)
	})
}

// gobCopy deep-copies *src through a gob round trip and reports the encoded
// size. Structs with all-zero fields encode fine (gob emits an empty struct).
func gobCopy[T any](src *T) (*T, int, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return nil, 0, err
	}
	size := buf.Len()
	dst := new(T)
	if err := gob.NewDecoder(&buf).Decode(dst); err != nil {
		return nil, 0, err
	}
	return dst, size, nil
}

// alive reports whether the call from->to that was dispatched to handler h
// may still complete: both ends connected and to still bound to h.
func (sn *Net) alive(from, to int, h raft.Handler) bool {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return sn.connected[from] && sn.connected[to] && sn.handlers[to] == h
}

func sleepMs(ms int) {
	if ms <= 0 {
		return
	}
	<-time.After(time.Duration(ms) * time.Millisecond)
}

// call implements one RPC, following labrpc's processReq:
//
//  1. If either end is disconnected or the target is unbound, wait a random
//     time (<100ms, or <7s with long delays) and return ok=false.
//  2. If unreliable, delay 0-27ms and drop the request with p=0.1.
//  3. Run the handler in its own goroutine on a gob copy of args. While
//     waiting, poll every 100ms; if the target is unbound/rebound or either
//     end is disconnected, give up (the handler goroutine finishes on its
//     own; its reply is discarded).
//  4. If unreliable, drop the reply with p=0.1. If long reordering, delay
//     it 200-2200ms with p=2/3. Return a gob copy of the reply.
//
// The call never blocks longer than the handler plus the delays above.
func call[A, R any](p *peer, args *A, handle func(raft.Handler, *A) *R) (*R, bool) {
	sn := p.net
	sn.counts[p.from].Add(1)
	sn.total.Add(1)

	argCopy, size, err := gobCopy(args)
	if err != nil {
		panic(fmt.Sprintf("simnet: cannot gob-encode %T: %v", args, err))
	}
	sn.nbytes.Add(int64(size))

	sn.mu.Lock()
	enabled := sn.connected[p.from] && sn.connected[p.to]
	h := sn.handlers[p.to]
	unreliable, longReordering, longDelays := sn.unreliable, sn.longReordering, sn.longDelays
	sn.mu.Unlock()

	if !enabled || h == nil {
		// Simulate a request that vanishes and a caller that eventually
		// times out.
		if longDelays {
			sleepMs(sn.intn(7000))
		} else {
			sleepMs(sn.intn(100))
		}
		return nil, false
	}

	if unreliable {
		sleepMs(sn.intn(27))
		if sn.intn(1000) < 100 {
			return nil, false // request lost
		}
	}

	ch := make(chan *R, 1)
	go func() { ch <- handle(h, argCopy) }()

	var reply *R
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
waiting:
	for {
		select {
		case reply = <-ch:
			break waiting
		case <-ticker.C:
			if !sn.alive(p.from, p.to, h) {
				return nil, false
			}
		}
	}
	if !sn.alive(p.from, p.to, h) {
		return nil, false
	}
	if reply == nil {
		// Handler contract violated ("always return a reply"); treat as lost.
		return nil, false
	}
	if unreliable && sn.intn(1000) < 100 {
		return nil, false // reply lost
	}
	if longReordering && sn.intn(900) < 600 {
		sleepMs(200 + sn.intn(1+sn.intn(2000)))
	}
	out, size, err := gobCopy(reply)
	if err != nil {
		panic(fmt.Sprintf("simnet: cannot gob-encode %T: %v", reply, err))
	}
	sn.nbytes.Add(int64(size))
	return out, true
}
