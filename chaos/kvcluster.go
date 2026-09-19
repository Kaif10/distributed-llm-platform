package chaos

// kvCluster wraps a real, Raft-replicated raftkv cluster over a seeded
// simnet.Net so it can be crashed, partitioned, and restarted by the
// nemesis, and calls it directly in-process (no gRPC) the way every
// raftkv/shardkv integration test in this repo already does.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/simnet"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvCluster struct {
	n          int
	net        *simnet.Net
	cfg        raftkv.Config
	mu         sync.Mutex
	servers    []*raftkv.Server
	persisters []*raft.MemPersister
	lastLeader atomic.Int32
	ids        *identityGen

	partitioned []bool // nemesis bookkeeping, guarded by mu
	crashed     []bool
}

// newKVCluster starts n raftkv replicas over net. idSeed seeds the identity
// generator for client_ids this cluster mints on its own behalf (Put/CAS/
// Delete callers below); it is derived from the scenario's seed, so which
// identities are used is itself reproducible.
func newKVCluster(n int, net *simnet.Net, cfg raftkv.Config, idSeed int64) *kvCluster {
	c := &kvCluster{
		n: n, net: net, cfg: cfg,
		servers:     make([]*raftkv.Server, n),
		persisters:  make([]*raft.MemPersister, n),
		partitioned: make([]bool, n),
		crashed:     make([]bool, n),
		ids:         newIdentityGen(idSeed),
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.start(i)
	}
	return c
}

func (c *kvCluster) start(i int) {
	peers := make([]raft.Peer, c.n)
	for j := 0; j < c.n; j++ {
		if j != i {
			peers[j] = c.net.Peer(i, j)
		}
	}
	c.mu.Lock()
	s := raftkv.New(peers, i, c.persisters[i], c.cfg)
	c.servers[i] = s
	c.crashed[i] = false
	c.mu.Unlock()
	c.net.Bind(i, s.Raft())
	if !c.isPartitioned(i) {
		c.net.Connect(i)
	}
}

func (c *kvCluster) crash(i int) {
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.persisters[i] = c.persisters[i].Copy()
	c.crashed[i] = true
	c.mu.Unlock()
	c.net.Disconnect(i)
	if s != nil {
		s.Kill()
	}
}

func (c *kvCluster) partition(i int) {
	c.mu.Lock()
	c.partitioned[i] = true
	c.mu.Unlock()
	c.net.Disconnect(i)
}

func (c *kvCluster) heal(i int) {
	c.mu.Lock()
	c.partitioned[i] = false
	crashed := c.crashed[i]
	c.mu.Unlock()
	if !crashed {
		c.net.Connect(i)
	}
}

func (c *kvCluster) isPartitioned(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partitioned[i]
}

func (c *kvCluster) isCrashed(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed[i]
}

func (c *kvCluster) server(i int) *raftkv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

func (c *kvCluster) killAll() {
	for i := 0; i < c.n; i++ {
		if s := c.server(i); s != nil {
			s.Kill()
		}
	}
}

// do runs fn against the cluster, following leader hints and retrying
// transient failures, exactly like every other client in this project.
func (c *kvCluster) do(ctx context.Context, fn func(s *raftkv.Server) error) error {
	i := int(c.lastLeader.Load())
	for {
		if s := c.server(i); s != nil {
			err := fn(s)
			if err == nil {
				c.lastLeader.Store(int32(i))
				return nil
			}
			switch status.Code(err) {
			case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
			default:
				return err
			}
		}
		i = (i + 1) % c.n
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}
}

// The three methods below make *kvCluster satisfy both sched.KV/kvapi.KV
// (Get/Put/CAS) and give raw KV clients Get/Put/CAS/Delete, all through one
// adapter with one identity per logical caller (see idGen below).

func (c *kvCluster) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var resp *kvv1.GetResponse
	err := c.do(ctx, func(s *raftkv.Server) error {
		var err error
		resp, err = s.Get(ctx, &kvv1.GetRequest{Key: key})
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *kvCluster) Put(ctx context.Context, key string, value []byte) error {
	req := &kvv1.PutRequest{Meta: c.ids.next(), Key: key, Value: value}
	return c.do(ctx, func(s *raftkv.Server) error { _, err := s.Put(ctx, req); return err })
}

func (c *kvCluster) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	req := &kvv1.CASRequest{Meta: c.ids.next(), Key: key, Expected: expected, ExpectAbsent: expectAbsent, Value: value}
	var resp *kvv1.CASResponse
	err := c.do(ctx, func(s *raftkv.Server) error {
		var err error
		resp, err = s.CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return false, nil, err
	}
	return resp.Swapped, resp.Current, nil
}

func (c *kvCluster) Delete(ctx context.Context, key string) (bool, error) {
	req := &kvv1.DeleteRequest{Meta: c.ids.next(), Key: key}
	var resp *kvv1.DeleteResponse
	err := c.do(ctx, func(s *raftkv.Server) error {
		var err error
		resp, err = s.Delete(ctx, req)
		return err
	})
	if err != nil {
		return false, err
	}
	return resp.Existed, nil
}

// identityGen hands out fresh (client_id, request_id=1) identities, one per
// logical call. None of these calls retry themselves — kvCluster.do retries
// the SAME struct, so the meta is stable across retries of one call — and
// no two concurrent calls share an identity, satisfying the "one
// outstanding request per identity" rule Phase 1's dedup table requires
// (see kv/client.Client.Session for the production version of this idea).
// It is seeded, not global, so which identities a run mints is itself a
// function of the scenario's seed.
type identityGen struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newIdentityGen(seed int64) *identityGen {
	return &identityGen{rng: rand.New(rand.NewSource(seed))}
}

func (g *identityGen) next() *kvv1.RequestMeta {
	g.mu.Lock()
	defer g.mu.Unlock()
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], g.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], g.rng.Uint64())
	return &kvv1.RequestMeta{ClientId: fmt.Sprintf("%x", b), RequestId: 1}
}
