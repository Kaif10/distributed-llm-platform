// Package kvadapter makes a kv/client.Client satisfy sched.KV, so the
// scheduler core never imports gRPC: it sees Get/Put/CAS and nothing else.
//
// # Why a pool of sessions, not one per operation
//
// The scheduler serves concurrent gRPC handlers, and kv/client's rule is at
// most one outstanding mutation per client identity at a time (the store's
// dedup table assumes request ids arrive in order per identity). Each
// concurrent Put/CAS therefore needs its own identity, i.e. its own
// client.Session().
//
// Minting a fresh Session per operation would satisfy the rule, but every
// identity costs the STORE a permanent dedup-table entry (one per client id,
// never evicted, replicated through Raft and carried in every snapshot). A
// busy scheduler would grow that table by one row per mutation forever.
// A bounded pool of N sessions, checked out for the duration of one
// operation, costs the store exactly N rows for the scheduler's lifetime and
// still lets N operations run in parallel; a caller that finds every session
// busy waits for one (or for its context) rather than inventing a new
// identity.
//
// Gets carry no RequestMeta, so they bypass the pool and go straight to the
// root Client, which is safe for concurrent reads.
package kvadapter

import (
	"context"

	"dsys/kv/client"
	"dsys/sched"
)

// DefaultSessions is the pool size New uses unless WithSessions says
// otherwise. It bounds how many mutations the scheduler can have in flight
// at once against the KV.
const DefaultSessions = 32

// Option configures the adapter.
type Option func(*adapter)

// WithSessions sets the number of client identities (and so the mutation
// concurrency) the adapter keeps. Values below 1 are ignored.
func WithSessions(n int) Option {
	return func(a *adapter) {
		if n >= 1 {
			a.n = n
		}
	}
}

type adapter struct {
	root *client.Client
	n    int
	// pool holds every idle session. A buffered channel is the whole
	// checkout mechanism: receive to borrow, send to return, and ctx.Done
	// bounds the wait.
	pool chan *client.Client
}

// New wraps c as a sched.KV. c stays owned by the caller (Close it after
// the scheduler stops); the adapter only derives Sessions from it.
func New(c *client.Client, opts ...Option) sched.KV {
	a := &adapter{root: c, n: DefaultSessions}
	for _, o := range opts {
		o(a)
	}
	a.pool = make(chan *client.Client, a.n)
	for i := 0; i < a.n; i++ {
		a.pool <- c.Session()
	}
	return a
}

// borrow blocks until a session is free or ctx is done.
func (a *adapter) borrow(ctx context.Context) (*client.Client, error) {
	select {
	case s := <-a.pool:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *adapter) release(s *client.Client) { a.pool <- s }

func (a *adapter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return a.root.Get(ctx, key)
}

func (a *adapter) Put(ctx context.Context, key string, value []byte) error {
	s, err := a.borrow(ctx)
	if err != nil {
		return err
	}
	defer a.release(s)
	return s.Put(ctx, key, value)
}

func (a *adapter) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	s, err := a.borrow(ctx)
	if err != nil {
		return false, nil, err
	}
	defer a.release(s)
	return s.CAS(ctx, key, expected, expectAbsent, value)
}
