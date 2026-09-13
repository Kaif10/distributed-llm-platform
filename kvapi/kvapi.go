// Package kvapi is the one small interface every higher layer programs
// against: a linearizable key-value store with compare-and-swap. The
// scheduler (Phase 4), the rate limiter, the worker registry and the
// semantic cache's shared index (Phase 5) all take a KV and nothing else, so
// each of them runs unchanged over the in-memory fake used in tests, the
// single-node store, the Raft-replicated store, or the sharded cluster.
package kvapi

import "context"

// KV is a linearizable key-value store. Implementations retry transient
// failures internally and attach a stable RequestMeta to each logical
// mutation so a retry is deduplicated rather than applied twice (see
// kv/store's package doc). A CAS returning swapped=false with err=nil is a
// normal outcome, not an error: someone else moved first, and current is
// what they wrote.
type KV interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
	CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (swapped bool, current []byte, err error)
}
