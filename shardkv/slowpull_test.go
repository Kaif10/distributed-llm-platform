package shardkv

import (
	"context"
	"testing"
	"time"

	"dsys/shard"
)

// slowFetcher answers like fakeFetcher but only after delay, and honours the
// caller's deadline the way a real RPC does: a pull whose context expires
// first fails with the context's error, exactly as a gRPC call would.
type slowFetcher struct {
	*fakeFetcher
	delay time.Duration
}

func (f *slowFetcher) PullShard(ctx context.Context, addrs []string, configNum int64, shardID int) ([]byte, bool, error) {
	select {
	case <-time.After(f.delay):
		return f.fakeFetcher.PullShard(ctx, addrs, configNum, shardID)
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// TestSlowShardPullStillMigrates is the regression test for the pull
// deadline being the poll interval itself. A shard whose transfer takes
// longer than PollInterval (a big shard, a slow link) could never arrive:
// every attempt was cancelled at the poll interval and retried from
// scratch, so the receiving group sat at ErrNotReady forever and its
// config never advanced. The pull deadline must be independent of how
// often we poll.
func TestSlowShardPullStillMigrates(t *testing.T) {
	ctrl := newFakeCtrl()
	fetcher := newFakeFetcher()
	// Every pull takes 10x the test PollInterval (15ms) but well inside the
	// default pull deadline.
	slow := func(o *Options) { o.Fetcher = &slowFetcher{fakeFetcher: fetcher, delay: 150 * time.Millisecond} }
	gA := newGroup(t, 100, 3, ctrl, fetcher, slow)
	gB := newGroup(t, 200, 3, ctrl, fetcher, slow)
	groups := map[int64][]string{100: gA.addrs, 200: gB.addrs}
	ctrl.push(allTo(100, groups))

	tc := &testCluster{ctrl: ctrl, groups: map[int64]*group{100: gA, 200: gB}}
	cl := tc.client("c1")
	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()

	key := "slow-pull"
	if err := cl.put(ctx, key, "v1"); err != nil {
		t.Fatal(err)
	}
	next := ctrl.latest()
	next.Shards[shard.Key2Shard(key)] = 200
	num := ctrl.push(next)

	// B must apply the config AND finish receiving the shard's data. The
	// test client retries ErrNotReady itself until its context expires.
	getCtx, getCancel := ctxT(10 * time.Second)
	defer getCancel()
	v, found, err := cl.get(getCtx, key)
	if err != nil || !found || v != "v1" {
		for i := 0; i < gB.n(); i++ {
			if s := gB.server(i); s != nil {
				s.mu.Lock()
				t.Logf("B replica %d: config %d (want %d), still needs %d shard(s)", i, s.cur.Num, num, len(s.needed))
				s.mu.Unlock()
			}
		}
		t.Fatalf("shard never migrated to group B through a fetcher answering in 150ms: v=%q found=%v err=%v", v, found, err)
	}
}
