package shardkv

import (
	"context"
	"testing"
	"time"
)

// slowCtrl answers like fakeCtrl but only after delay, and honours the
// caller's deadline the way a real RPC does: a Query whose context expires
// first fails with the context's error.
type slowCtrl struct {
	*fakeCtrl
	delay time.Duration
}

func (c *slowCtrl) Query(ctx context.Context, num int64) (Config, error) {
	select {
	case <-time.After(c.delay):
		return c.fakeCtrl.Query(ctx, num)
	case <-ctx.Done():
		return Config{}, ctx.Err()
	}
}

// TestSlowControllerStillDeliversConfigs is the regression test for the
// poll loop's Query deadline being the poll interval itself. A controller
// whose Query has become slower than that (its Raft log long, a busy
// leader, a slow link) was never heard from again: every poll timed out,
// so groups never saw a new configuration. The Query deadline must be
// independent of the poll interval and back off while Queries keep failing.
func TestSlowControllerStillDeliversConfigs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
		tweak func(*Options)
	}{
		// Slower than PollInterval (15ms), within the default deadline.
		{name: "default-deadline", delay: 150 * time.Millisecond},
		// Slower than the configured deadline: only the backoff (40ms,
		// 80ms, 160ms...) ever gets an answer through.
		{name: "backoff", delay: 150 * time.Millisecond, tweak: func(o *Options) { o.QueryTimeout = 40 * time.Millisecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := newFakeCtrl()
			slow := &slowCtrl{fakeCtrl: ctrl, delay: tc.delay}
			var tweaks []func(*Options)
			if tc.tweak != nil {
				tweaks = append(tweaks, tc.tweak)
			}
			g := newGroup(t, 100, 3, slow, newFakeFetcher(), tweaks...)
			num := ctrl.push(allTo(100, map[int64][]string{100: g.addrs}))

			deadline := time.Now().Add(10 * time.Second)
			for {
				for i := 0; i < g.n(); i++ {
					if s := g.server(i); s != nil && s.CurrentConfig().Num >= num {
						return
					}
				}
				if time.Now().After(deadline) {
					t.Fatalf("group never applied config %d from a controller answering in %v", num, tc.delay)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
