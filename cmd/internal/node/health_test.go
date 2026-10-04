package node

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/raftkv"
	"dsys/raft"
)

// memNet is an in-process network of n nodes with a per-direction link switch,
// enough to partition a node without real sockets.
type memNet struct {
	mu    sync.Mutex
	nodes []*raft.Raft
	up    [][]atomic.Bool
}

type localPeer struct {
	n        *memNet
	from, to int
}

func (p localPeer) target() (raft.Handler, bool) {
	if !p.n.up[p.from][p.to].Load() || !p.n.up[p.to][p.from].Load() {
		return nil, false
	}
	p.n.mu.Lock()
	defer p.n.mu.Unlock()
	h := p.n.nodes[p.to]
	return h, h != nil
}

func (p localPeer) RequestVote(a *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	if h, ok := p.target(); ok {
		return h.HandleRequestVote(a), true
	}
	return nil, false
}

func (p localPeer) AppendEntries(a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	if h, ok := p.target(); ok {
		return h.HandleAppendEntries(a), true
	}
	return nil, false
}

func (p localPeer) InstallSnapshot(a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, bool) {
	if h, ok := p.target(); ok {
		return h.HandleInstallSnapshot(a), true
	}
	return nil, false
}

// isolate cuts every link to and from node i.
func (n *memNet) isolate(i int) {
	for j := range n.up {
		n.up[i][j].Store(false)
		n.up[j][i].Store(false)
	}
}

func TestHealthIsPerNode(t *testing.T) {
	const N = 3
	nw := &memNet{nodes: make([]*raft.Raft, N), up: make([][]atomic.Bool, N)}
	for i := range nw.up {
		nw.up[i] = make([]atomic.Bool, N)
		for j := range nw.up[i] {
			nw.up[i][j].Store(true)
		}
	}
	rcfg := raft.Config{HeartbeatInterval: 20 * time.Millisecond, ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond}
	hs := make([]*Health, N)
	for i := 0; i < N; i++ {
		peers := make([]raft.Peer, N)
		for j := range peers {
			if j != i {
				peers[j] = localPeer{n: nw, from: i, to: j}
			}
		}
		srv := raftkv.New(peers, i, raft.NewMemPersister(), raftkv.Config{Raft: rcfg})
		t.Cleanup(srv.Kill)
		nw.mu.Lock()
		nw.nodes[i] = srv.Raft()
		nw.mu.Unlock()
		hs[i] = &Health{Raft: srv.Raft(), Me: i, Probe: func(ctx context.Context) error {
			_, err := srv.Get(ctx, &kvv1.GetRequest{Key: "health-probe"})
			return err
		}}
	}

	check := func(i int) bool {
		r, err := hs[i].Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("node %d Check: %v", i, err)
		}
		return r.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}
	// eventually polls until want(i) holds for every listed node.
	eventually := func(what string, nodes []int, want bool) {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for {
			all := true
			for _, i := range nodes {
				if check(i) != want {
					all = false
				}
			}
			if all {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: nodes %v never all reported serving=%v", what, nodes, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	leader := func() int {
		for i, rf := range nw.nodes {
			if _, ok := rf.GetState(); ok {
				return i
			}
		}
		return -1
	}

	eventually("healthy cluster", []int{0, 1, 2}, true)

	// Isolate the leader. The other two elect a new one and stay healthy;
	// the old leader still BELIEVES it leads, but its probe cannot commit,
	// so it must report NOT_SERVING even though the cluster is fine.
	old := leader()
	if old < 0 {
		t.Fatal("no leader")
	}
	nw.isolate(old)
	var rest []int
	for i := 0; i < N; i++ {
		if i != old {
			rest = append(rest, i)
		}
	}
	eventually("isolated old leader", []int{old}, false)
	eventually("majority side", rest, true)

	// Now isolate a follower of the new majority (leaving one node alone,
	// so nobody has a quorum): every node must go NOT_SERVING.
	nw.isolate(rest[0])
	eventually("no quorum anywhere", []int{0, 1, 2}, false)
}
