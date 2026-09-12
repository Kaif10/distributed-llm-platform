// Package shardkv is one replica group in a sharded key-value store: several
// independent Raft groups, each owning a subset of the keyspace, coordinated
// by a separate shard controller (package shardctrl) that decides which
// group owns which shard and moves that assignment over time.
//
// # Why this needs its own package instead of just running N raftkv's
//
// Phase 2's raftkv already gives you a replicated, linearizable KV store.
// Running several independent raftkv groups side by side would give you
// more throughput, but only if a key never needs to change group: nothing
// in raftkv models "this key used to live over there, and might still, for
// stragglers". Rebalancing (moving a shard from an overloaded group to an
// idle one) requires:
//
//  1. A place both groups agree on for "which group owns which shard, as of
//     when" — the controller's sequence of Configs.
//  2. A way to move a shard's actual data (its key-value pairs AND its
//     idempotency sessions, or a client's retry would forget it already
//     succeeded) from the old owner to the new one.
//  3. A rule for what a group does with requests for a shard it does not
//     currently own, and for a shard it owns but has not finished receiving.
//
// This package is items 2 and 3. Package shardctrl is item 1.
//
// # The three kinds of Raft entry
//
// Compare with raftkv, whose Raft log held exactly one kind of entry: a
// kvv1.LogEntry. A shardkv group's log holds three kinds, wrapped in
// Command (raftcommand.go), so that config changes and migrations are
// ordered against client operations by the SAME mechanism that orders
// client operations against each other: appearing in the same replicated
// log. That is what makes "did this migration happen before or after that
// write" a well-defined question with the same answer on every replica.
//
//   - opKV:        a client Put/Get/Delete/CAS, same as raftkv.
//   - opConfig:    "the configuration is now number N"; only takes effect
//     once every shard the group is due to gain under N has
//     actually arrived (see server.go, applyConfig).
//   - opMigration: "here is shard S's data as of the end of config N-1,
//     pulled from its previous owner"; installed idempotently,
//     so a duplicate delivery (possible after a leader change,
//     exactly like Phase 1/2) is a no-op the second time.
package shardkv

import (
	"context"

	"dsys/shard"
)

// Config is this package's own view of a shard assignment: which group owns
// each shard, and how to reach each group. It deliberately does not import
// anything from package shardctrl, so shardkv has no compile-time dependency
// on the controller's wire format; only the small Controller interface below
// crosses that boundary, implemented by an adapter that lives in cmd/shardkv.
type Config struct {
	Num    int64
	Shards [shard.NShards]int64 // shard index -> owning group id; 0 = unassigned
	Groups map[int64][]string   // group id -> replica addresses, leader unknown
}

// Owner returns the group id that owns shardID under this config.
func (c Config) Owner(shardID int) int64 { return c.Shards[shardID] }

// Controller is what a shardkv server needs from the shard controller: the
// ability to ask "what is configuration number N" (or, with num<0, "what is
// the latest configuration"). A shardkv server polls this periodically and
// proposes an opConfig entry when it sees a configuration newer than its own.
//
// Implemented by an adapter around the shardctrl gRPC client (cmd/shardkv),
// and by a trivial in-memory stand-in in tests.
type Controller interface {
	Query(ctx context.Context, num int64) (Config, error)
}

// ShardFetcher pulls one shard's state from whichever group currently
// answers for it, used when this group is about to start owning a shard it
// does not yet hold. addrs is every replica address of the PREVIOUS owner
// group (from the config being migrated away from); the fetcher tries them
// until one, presumably the leader, has the data.
//
// ok=false with a nil error means "asked, but nobody had it yet" (the old
// group has not applied the config change either, or its leader is
// unreachable) — retry later. A non-nil error means the request could not
// even be attempted and is also just cause to retry later.
//
// Implemented by a gRPC client around the ShardMigration service
// (proto/shardkv/v1) in cmd/shardkv, and by a direct in-process call in
// tests (see shardkv_test.go), exactly the way raftkv's tests called Server
// methods directly instead of going through grpctransport.
type ShardFetcher interface {
	PullShard(ctx context.Context, addrs []string, configNum int64, shardID int) (snapshot []byte, ok bool, err error)
}
