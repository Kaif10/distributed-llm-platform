package sched

// Integration: the queue over a REAL replicated KV (kv/raftkv, three
// replicas over raft/simnet), not the in-memory fake. This is what proves
// the CAS discipline composes with Phase 2: every counter and record round
// trips through consensus, a leader is killed mid-run, and exactly-once
// still holds. It is smaller than the chaos test because each KV operation
// is now a Raft round, not a mutex.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "dsys/gen/kv/v1"
	schedv1 "dsys/gen/sched/v1"
	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/simnet"
)

// raftKV adapts a raftkv cluster (called in-process, no gRPC) to sched.KV.
// Every logical operation gets its own client identity, so concurrent
// operations never share a request-id sequence (the one-outstanding-request
// -per-client rule from kv/store), and each operation's retries reuse the
// same identity so the store deduplicates them.
type raftKV struct {
	servers    []*raftkv.Server
	lastLeader atomic.Int32
	seq        atomic.Uint64
}

func (r *raftKV) meta() *kvv1.RequestMeta {
	return &kvv1.RequestMeta{ClientId: fmt.Sprintf("sched-op-%d", r.seq.Add(1)), RequestId: 1}
}

func (r *raftKV) do(ctx context.Context, fn func(s *raftkv.Server) error) error {
	n := len(r.servers)
	i := int(r.lastLeader.Load())
	for {
		err := fn(r.servers[i])
		if err == nil {
			r.lastLeader.Store(int32(i))
			return nil
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		default:
			return err
		}
		i = (i + 1) % n
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (r *raftKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var resp *kvv1.GetResponse
	err := r.do(ctx, func(s *raftkv.Server) error {
		var err error
		resp, err = s.Get(ctx, &kvv1.GetRequest{Key: key})
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (r *raftKV) Put(ctx context.Context, key string, value []byte) error {
	req := &kvv1.PutRequest{Meta: r.meta(), Key: key, Value: value}
	return r.do(ctx, func(s *raftkv.Server) error { _, err := s.Put(ctx, req); return err })
}

func (r *raftKV) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	req := &kvv1.CASRequest{Meta: r.meta(), Key: key, Expected: expected, ExpectAbsent: expectAbsent, Value: value}
	var resp *kvv1.CASResponse
	err := r.do(ctx, func(s *raftkv.Server) error {
		var err error
		resp, err = s.CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return false, nil, err
	}
	return resp.Swapped, resp.Current, nil
}

func newRaftKV(t *testing.T, n int) (*raftKV, *simnet.Net) {
	t.Helper()
	net := simnet.New(n)
	r := &raftKV{servers: make([]*raftkv.Server, n)}
	cfg := raftkv.Config{
		CommitTimeout: time.Second,
		Raft: raft.Config{
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 250 * time.Millisecond,
			ElectionTimeoutMax: 500 * time.Millisecond,
		},
	}
	for i := 0; i < n; i++ {
		peers := make([]raft.Peer, n)
		for j := 0; j < n; j++ {
			if j != i {
				peers[j] = net.Peer(i, j)
			}
		}
		r.servers[i] = raftkv.New(peers, i, raft.NewMemPersister(), cfg)
		net.Bind(i, r.servers[i].Raft())
		net.Connect(i)
	}
	t.Cleanup(func() {
		for _, s := range r.servers {
			s.Kill()
		}
	})
	return r, net
}

func TestOverRaftKVWithLeaderLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	kv, net := newRaftKV(t, 3)
	q := New(kv, Options{Prefix: "t", DefaultLease: 2 * time.Second, MaxAttempts: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const nJobs, workers = 30, 3
	for i := 0; i < nJobs; i++ {
		if _, err := q.Submit(ctx, []byte(fmt.Sprint(i)), ""); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	var completions sync.Map
	var done atomic.Int64
	var fenced atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			name := fmt.Sprintf("w%d", w)
			idle := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				job, err := q.Claim(ctx, name, 0)
				if errors.Is(err, ErrNoJob) {
					idle++
					if idle > 50 {
						return
					}
					time.Sleep(50 * time.Millisecond)
					continue
				}
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(50 * time.Millisecond)
					continue
				}
				idle = 0
				time.Sleep(20 * time.Millisecond)
				err = q.Complete(ctx, job.Id, job.Gen, []byte(name))
				switch {
				case err == nil:
					v, _ := completions.LoadOrStore(job.Id, new(atomic.Int32))
					v.(*atomic.Int32).Add(1)
					done.Add(1)
				case errors.Is(err, ErrFenced):
					fenced.Add(1)
				default:
					if ctx.Err() == nil {
						t.Errorf("complete %d: %v", job.Id, err)
					}
				}
			}
		}(w)
	}

	// Kill whichever replica is leader partway through, then bring it back.
	time.Sleep(400 * time.Millisecond)
	leader := -1
	for i, s := range kv.servers {
		if s.IsLeader() {
			leader = i
		}
	}
	if leader >= 0 {
		net.Disconnect(leader)
		t.Logf("disconnected leader %d mid-run", leader)
		time.Sleep(1500 * time.Millisecond)
		net.Connect(leader)
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && done.Load() < nJobs {
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if done.Load() != nJobs {
		t.Fatalf("done=%d want %d (fenced=%d)", done.Load(), nJobs, fenced.Load())
	}
	for id := uint64(1); id <= nJobs; id++ {
		st, err := q.Status(ctx, id)
		if err != nil || st.State != schedv1.State_STATE_DONE {
			t.Fatalf("job %d: %+v %v", id, st, err)
		}
		if v, ok := completions.Load(id); !ok || v.(*atomic.Int32).Load() != 1 {
			t.Fatalf("job %d completed %v times", id, v)
		}
	}
	t.Logf("%d jobs exactly-once over a 3-replica Raft KV with a leader outage; fenced=%d", nJobs, fenced.Load())
}
