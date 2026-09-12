// Package shard defines how keys map to shards. It has no dependencies so
// both the shard controller and every shard-owning replica group agree on
// the mapping without needing to talk to each other about it.
//
// # Why sharding at all
//
// Phase 2 gave you a replicated log, but every write still goes through one
// leader and every replica holds the whole keyspace. That caps throughput at
// one leader's worth of consensus rounds, no matter how many machines you
// add. Sharding splits the keyspace into independent partitions, each
// replicated by its own Raft group. Groups do not coordinate with each other
// to serve a request, so throughput scales with the number of groups.
//
// The price is that operations spanning two shards are no longer atomic
// (there is no cross-shard transaction here, matching the scope of a
// standard sharded-KV lab). A single key's operations remain linearizable,
// because a key always lives in exactly one shard, served by exactly one
// Raft group at a time.
package shard

import "hash/fnv"

// NShards is fixed at compile time. A real system would let this grow, but
// a fixed shard count keeps the rebalancing algorithm and the migration
// protocol simple, which is the point of this phase: consistent hashing
// with virtual nodes is the natural follow-up exercise.
const NShards = 10

// Key2Shard deterministically maps a key to a shard index in [0, NShards).
// It must be a pure function of the key: every replica of every group, and
// the controller, must compute the same answer independently, with no
// coordination, for the same key.
func Key2Shard(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}
