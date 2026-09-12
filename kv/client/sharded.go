package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	kvv1 "dsys/gen/kv/v1"
	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultConfigCacheTTL bounds how long a sharded-mode config answer is
// reused before the client asks the controller again. Short enough that a
// shard move is picked up quickly on its own (on top of the immediate
// invalidation a wrong-group error already triggers), long enough that a
// benchmark does not hammer the controller once per op. Override with
// WithConfigCacheTTL.
const defaultConfigCacheTTL = 200 * time.Millisecond

// runner is the one seam Put/Get/Delete/CAS go through, so they do not need
// to know whether the Client is flat (one group) or sharded (controller).
type runner interface {
	run(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error
}

func newKVClientFromConn(cc grpc.ClientConnInterface) kvv1.KVClient { return kvv1.NewKVClient(cc) }

// flatRunner is NewFlat: one cluster for the whole keyspace, no controller
// involved.
type flatRunner struct{ cl *cluster[kvv1.KVClient] }

func (r flatRunner) run(ctx context.Context, _ string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	return doOn(ctx, r.cl, fn)
}

// ---------------------------------------------------------------------------
// Sharded mode: resolve a key's owning group from the shard controller,
// then run the request against that group's cluster.
// ---------------------------------------------------------------------------

// shardedClient implements sharded-mode dispatch: which group owns a key's
// shard, cached briefly, with wrong-group invalidation on top.
type shardedClient struct {
	sh   *shared
	ctrl *cluster[shardctrlv1.ShardCtrlClient]

	mu      sync.Mutex
	cfg     *shardctrlv1.Config
	fetched time.Time

	// groupClusters caches one cluster per group id for the life of this
	// shardedClient. A cluster remembers which of its replicas last
	// answered as leader (see cluster.pick/markGood), so reusing it across
	// requests to the same group is what makes steady-state traffic avoid
	// a "wrong replica, here's the leader" round trip on every single op.
	// Rebuilding a fresh cluster per request (the original bug here) throws
	// that memory away every time, so a request would need on average one
	// retry per call even with nothing wrong in the cluster: with n
	// replicas, guessing cold hits the actual leader only 1/n of the time.
	groupClusters map[int64]*cluster[kvv1.KVClient]
}

func newShardedClient(ctrlAddrs []string, sh *shared) *shardedClient {
	return &shardedClient{
		sh: sh,
		ctrl: newCluster(ctrlAddrs, sh, func(cc grpc.ClientConnInterface) shardctrlv1.ShardCtrlClient {
			return shardctrlv1.NewShardCtrlClient(cc)
		}),
		groupClusters: make(map[int64]*cluster[kvv1.KVClient]),
	}
}

func (s *shardedClient) run(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	return s.do(ctx, key, fn)
}

// config returns the cached controller config, refreshing it if stale or
// if force is set (used right after a wrong-group error).
func (s *shardedClient) config(ctx context.Context, force bool) (*shardctrlv1.Config, error) {
	s.mu.Lock()
	if !force && s.cfg != nil && time.Since(s.fetched) < s.sh.cfg.configCacheTTL {
		cfg := s.cfg
		s.mu.Unlock()
		return cfg, nil
	}
	s.mu.Unlock()

	var resp *shardctrlv1.QueryResponse
	err := doOn(ctx, s.ctrl, func(ctx context.Context, c shardctrlv1.ShardCtrlClient) error {
		var err error
		resp, err = c.Query(ctx, &shardctrlv1.QueryRequest{Num: -1})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("query shard controller: %w", err)
	}
	s.mu.Lock()
	s.cfg = resp.GetConfig()
	s.fetched = time.Now()
	cfg := s.cfg
	s.mu.Unlock()
	return cfg, nil
}

func (s *shardedClient) invalidate() {
	s.mu.Lock()
	s.cfg = nil
	s.mu.Unlock()
}

// ownerCluster returns a cluster for whichever group currently owns key's
// shard, per the (possibly cached) config.
func (s *shardedClient) ownerCluster(ctx context.Context, key string, force bool) (*cluster[kvv1.KVClient], error) {
	cfg, err := s.config(ctx, force)
	if err != nil {
		return nil, err
	}
	shardID := shard.Key2Shard(key)
	shards := cfg.GetShards()
	if shardID >= len(shards) {
		return nil, fmt.Errorf("kv/client: controller config has no entry for shard %d", shardID)
	}
	gid := shards[shardID]
	if gid == 0 {
		return nil, fmt.Errorf("kv/client: shard %d is currently unassigned", shardID)
	}
	group := cfg.GetGroups()[gid]
	if group == nil || len(group.GetAddrs()) == 0 {
		return nil, fmt.Errorf("kv/client: no known addresses for group %d (shard %d)", gid, shardID)
	}
	return s.clusterForGroup(gid, group.GetAddrs()), nil
}

// clusterForGroup returns the cached cluster for gid, creating one (or
// replacing it, on the rare event the group's address list itself changed)
// the first time it is needed. See groupClusters' doc comment for why this
// caching matters.
func (s *shardedClient) clusterForGroup(gid int64, addrs []string) *cluster[kvv1.KVClient] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, ok := s.groupClusters[gid]; ok && cl.builtFrom(addrs) {
		return cl
	}
	cl := newCluster(addrs, s.sh, newKVClientFromConn)
	s.groupClusters[gid] = cl
	return cl
}

// isWrongGroup reports whether err is the FailedPrecondition status
// shardkv.Server.toStatus produces for ErrWrongGroup ("shardkv: wrong
// group for this key's shard"): the shard has moved to a different group
// since we last asked the controller.
func isWrongGroup(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), "wrong group")
}

// do runs one logical KV request in sharded mode: resolve the owning
// group, run it there via doOn (so within-group leader hints are followed
// exactly as in flat mode), and on a wrong-group failure invalidate the
// cached config and retry against whichever group owns the shard now.
//
// The same RequestMeta travels to the new group. That is still safe: the
// dedup table migrates with the shard, so a group that received the shard
// after the old owner applied our write recognises the meta and answers
// with the original result.
func (s *shardedClient) do(ctx context.Context, key string, fn func(ctx context.Context, kv kvv1.KVClient) error) error {
	force := false
	for {
		cl, err := s.ownerCluster(ctx, key, force)
		if err != nil {
			return err
		}
		err = doOn(ctx, cl, fn)
		if err == nil {
			return nil
		}
		if isWrongGroup(err) {
			s.sh.retries.Add(1)
			s.sh.cfg.logf("kv/client: shard for %q moved; re-resolving owner", key)
			s.invalidate()
			force = true
			if ctx.Err() != nil {
				return err
			}
			continue
		}
		return err
	}
}
