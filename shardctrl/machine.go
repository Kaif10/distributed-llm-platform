// Package shardctrl is the shard controller: the small piece of state that
// says which replica group owns which shard, and how that changes over
// time. It is structurally the same idea as kv/store and kv/raftkv (a pure
// state machine driven, in order, by a replicated log) but the state is a
// history of Configs instead of a key-value map, and the interesting logic
// is not the map operations but the rebalancing rule that Join and Leave
// use to redistribute shards deterministically.
//
// # Why this needs to be a Raft-replicated service at all
//
// Every shardkv replica group and every client needs to agree on the
// mapping from shard to group, including during the window while a
// migration is in flight. If each group decided independently when it
// "owned" a shard, two groups could believe they own the same shard at
// once. Running the controller itself as a tiny Raft group gives it the
// same fault-tolerant, linearizable log discipline Phase 2 built for the
// data path, applied to the much smaller amount of state that describes
// the data path's own topology.
package shardctrl

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"
)

// Op identifies which mutation (or query) a Command represents.
type Op int

const (
	OpJoin Op = iota
	OpLeave
	OpMove
	OpQuery
)

// Command is the payload that goes into the replicated log; see
// kv/store.Machine for the analogous per-op struct in Phase 1/2. One
// Command carries the union of fields any op might need; only the fields
// relevant to c.Op are read.
type Command struct {
	Op   Op
	Meta *shardctrlv1.RequestMeta

	Groups map[int64]*shardctrlv1.Group // Join
	Gids   []int64                      // Leave
	Shard  int64                        // Move
	Gid    int64                        // Move

	QueryNum int64 // Query
}

var (
	// ErrStaleRequest mirrors store.ErrStaleRequest: a request id lower than
	// the last one this client is on record for. Safe to ignore on replay,
	// an error everywhere else.
	ErrStaleRequest = errors.New("shardctrl: request id is older than the last applied for this client")

	// ErrUnknownGroup is returned by Move when asked to hand a shard to a
	// group id the current config does not know about. Move is a deliberate
	// operator override, not a rebalance, so accepting an unknown gid would
	// silently create a config pointing at a group nobody can reach. We
	// choose to reject it instead: the request is a no-op (no new Config is
	// produced) and the caller sees the error and can retry once the group
	// has actually Join-ed.
	ErrUnknownGroup = errors.New("shardctrl: move to unknown group")

	// ErrInvalidShard is returned by Move for a shard index outside
	// [0, shard.NShards).
	ErrInvalidShard = errors.New("shardctrl: shard index out of range")
)

// session is the per-client dedup entry (see kv/store.Machine.session).
// Errors are remembered by kind, not by formatted string, so a retry gets
// back something errors.Is can still match against instead of an opaque
// reconstructed error.
type session struct {
	lastID    uint64
	errKind   errKind
	resultNum int64 // valid config index (== Config.Num) when errKind == errNone
}

type errKind int

const (
	errNone errKind = iota
	errKindUnknownGroup
	errKindInvalidShard
)

func (k errKind) err() error {
	switch k {
	case errKindUnknownGroup:
		return ErrUnknownGroup
	case errKindInvalidShard:
		return ErrInvalidShard
	default:
		return nil
	}
}

func classify(err error) errKind {
	switch {
	case err == nil:
		return errNone
	case errors.Is(err, ErrUnknownGroup):
		return errKindUnknownGroup
	case errors.Is(err, ErrInvalidShard):
		return errKindInvalidShard
	default:
		return errNone
	}
}

// Machine is the pure state machine: the whole history of Configs plus the
// dedup table. No I/O, no locks, no goroutines, not safe for concurrent
// use — exactly like kv/store.Machine, and for the same reason: the caller
// (the Raft apply loop) serialises every call by construction.
type Machine struct {
	// configs[i].Num == int64(i) always; index 0 is the initial empty
	// config every Machine starts with. Configs are never mutated once
	// appended, so a *Config handed out by an old Apply/Dedup call stays
	// valid forever, and configs may safely share Group pointers across
	// versions where a group did not change.
	configs  []*shardctrlv1.Config
	sessions map[string]session
}

// NewMachine returns a Machine holding only the initial config: Num 0, all
// shards unassigned (gid 0), no groups.
func NewMachine() *Machine {
	return &Machine{
		configs:  []*shardctrlv1.Config{initialConfig()},
		sessions: make(map[string]session),
	}
}

func initialConfig() *shardctrlv1.Config {
	return &shardctrlv1.Config{
		Num:    0,
		Shards: make([]int64, shard.NShards),
		Groups: make(map[int64]*shardctrlv1.Group),
	}
}

// Dedup reports whether the request described by meta has already been
// applied (done=true, with its original result) or is stale (err != nil).
// A nil meta or empty client id always means "not deduplicated": Query
// requests carry no RequestMeta (querying is a read with no side effect,
// exactly like store.Machine's Get), so they are simply re-executed every
// time, which is safe because they are naturally idempotent.
func (m *Machine) Dedup(meta *shardctrlv1.RequestMeta) (*shardctrlv1.Config, bool, error) {
	if meta == nil || meta.ClientId == "" {
		return nil, false, nil
	}
	sess, ok := m.sessions[meta.ClientId]
	if !ok {
		return nil, false, nil
	}
	switch {
	case meta.RequestId == sess.lastID:
		if err := sess.errKind.err(); err != nil {
			return nil, true, err
		}
		return cloneConfig(m.configs[sess.resultNum]), true, nil
	case meta.RequestId < sess.lastID:
		return nil, false, ErrStaleRequest
	}
	return nil, false, nil
}

// Apply mutates (or queries) the Machine and returns the result. Must be
// deterministic: same Command against the same state always produces the
// same Config, byte-for-byte in its shard assignment, on every replica.
// See rebalance for the part that requires care.
//
// Apply is idempotent with respect to RequestMeta for the same reason
// store.Machine.Apply is: a leader change can cause a client's retry to be
// logged a second time, and the log must tolerate that without creating a
// second Config for one logical request.
func (m *Machine) Apply(c *Command) (*shardctrlv1.Config, error) {
	if cfg, done, err := m.Dedup(c.Meta); done || err != nil {
		return cfg, err
	}
	cfg, err := m.applyOp(c)
	if c.Meta != nil && c.Meta.ClientId != "" {
		sess := session{lastID: c.Meta.RequestId, errKind: classify(err)}
		if err == nil {
			sess.resultNum = cfg.Num
		}
		m.sessions[c.Meta.ClientId] = sess
	}
	if err != nil {
		// Stale ids are handled in Dedup above; any error reaching here is a
		// functional one (Move's ErrUnknownGroup/ErrInvalidShard) and is
		// deterministic given the current config, so every replica that
		// applies this entry reaches the same conclusion and none of them
		// mutate state for it.
		return nil, err
	}
	return cloneConfig(cfg), nil
}

func (m *Machine) applyOp(c *Command) (*shardctrlv1.Config, error) {
	switch c.Op {
	case OpQuery:
		return m.queryConfig(c.QueryNum), nil
	case OpJoin:
		return m.applyJoin(c.Groups), nil
	case OpLeave:
		return m.applyLeave(c.Gids), nil
	case OpMove:
		return m.applyMove(c.Shard, c.Gid)
	default:
		// An unknown op in the log means a newer binary wrote it. Crashing
		// loudly is better than silently skipping a control-plane change.
		panic(fmt.Sprintf("shardctrl: unknown op %v in log", c.Op))
	}
}

func (m *Machine) latest() *shardctrlv1.Config { return m.configs[len(m.configs)-1] }

// queryConfig returns configs[num], or the latest config if num is negative
// or past the end (num < 0 means "latest" per the proto's QueryRequest doc;
// an out-of-range num degrades to the same behaviour rather than an error,
// since Query has no error return in its RPC and a stale/racing client
// asking for a not-yet-created config almost certainly just wants "the
// current one").
func (m *Machine) queryConfig(num int64) *shardctrlv1.Config {
	if num < 0 || int(num) >= len(m.configs) {
		return m.latest()
	}
	return m.configs[num]
}

// applyJoin merges groups into the current group set — an already-known gid
// simply gets new addrs, matching Join's idempotent-re-join contract — and
// rebalances. See rebalance for the algorithm.
func (m *Machine) applyJoin(groups map[int64]*shardctrlv1.Group) *shardctrlv1.Config {
	latest := m.latest()
	newGroups := make(map[int64]*shardctrlv1.Group, len(latest.Groups)+len(groups))
	for gid, g := range latest.Groups {
		newGroups[gid] = g
	}
	for gid, g := range groups {
		newGroups[gid] = &shardctrlv1.Group{Addrs: append([]string(nil), g.GetAddrs()...)}
	}
	cfg := &shardctrlv1.Config{
		Num:    latest.Num + 1,
		Shards: rebalance(latest.Shards, sortedGids(newGroups)),
		Groups: newGroups,
	}
	m.configs = append(m.configs, cfg)
	return cfg
}

// applyLeave removes gids from the group set and rebalances whatever
// remains (or maps every shard to gid 0 if nothing remains).
func (m *Machine) applyLeave(gids []int64) *shardctrlv1.Config {
	latest := m.latest()
	remove := make(map[int64]bool, len(gids))
	for _, g := range gids {
		remove[g] = true
	}
	newGroups := make(map[int64]*shardctrlv1.Group, len(latest.Groups))
	for gid, g := range latest.Groups {
		if !remove[gid] {
			newGroups[gid] = g
		}
	}
	cfg := &shardctrlv1.Config{
		Num:    latest.Num + 1,
		Shards: rebalance(latest.Shards, sortedGids(newGroups)),
		Groups: newGroups,
	}
	m.configs = append(m.configs, cfg)
	return cfg
}

// applyMove forces one shard to one group, overriding the rebalancer for
// that shard only. It does not touch any other shard's assignment — a
// later Join or Leave rebalances from whatever Move left behind, treating
// the moved shard like any other already-assigned shard.
//
// gid must already be a known group. Accepting an unknown gid would let an
// operator typo silently point a shard at a group nobody can reach, with no
// way for a client to notice; requiring it to exist and erroring otherwise
// (a no-op: no new Config is produced) is the documented, defensible
// choice here.
func (m *Machine) applyMove(shardID, gid int64) (*shardctrlv1.Config, error) {
	latest := m.latest()
	if shardID < 0 || int(shardID) >= len(latest.Shards) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidShard, shardID)
	}
	if _, ok := latest.Groups[gid]; !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownGroup, gid)
	}
	newShards := append([]int64(nil), latest.Shards...)
	newShards[shardID] = gid
	cfg := &shardctrlv1.Config{
		Num:    latest.Num + 1,
		Shards: newShards,
		Groups: latest.Groups, // unchanged; safe to share, configs are never mutated in place
	}
	m.configs = append(m.configs, cfg)
	return cfg, nil
}

// sortedGids returns groups' keys in ascending order. Go's map iteration
// order is randomized, so every caller that needs a decision to be
// deterministic across replicas must go through this instead of ranging
// over the map directly.
func sortedGids(groups map[int64]*shardctrlv1.Group) []int64 {
	gids := make([]int64, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool { return gids[i] < gids[j] })
	return gids
}

// rebalance computes the shard assignment for the group set gids (which the
// caller must already have sorted ascending — see sortedGids) given the
// CURRENT shard assignment. A shard currently owned by a gid not present in
// gids (a group that just left, or gid 0 for "unassigned") is treated as
// unassigned. rebalance is a pure function of (shards, gids): the same
// inputs always produce the same output, on every replica, independent of
// map iteration order.
//
// # The algorithm
//
//  1. Fix a target shard count per group ahead of any movement: divide
//     shard.NShards by len(gids); the first NShards%len(gids) groups IN
//     ASCENDING GID ORDER get one extra shard (target = base+1), the rest
//     get the plain quotient (target = base). Fixing targets this way,
//     from gid order alone, is what makes the result depend only on the
//     group ids involved and not on the current distribution or history —
//     two replicas that agree on (shards, gids) always agree on targets.
//  2. Shed: for each group in ascending gid order, if it holds more than
//     its target, move its excess — its own currently-owned shards, in
//     ascending shard-index order — into a pool of shards awaiting
//     reassignment. A group already at or below its target is never
//     touched, so shards that are "already fine" never move.
//  3. Fill: hand out every shard in the pool (unassigned shards first, in
//     ascending index order, then the shed shards appended in the order
//     step 2 produced them) to whichever group is currently furthest below
//     its target, breaking ties by ascending gid. Because this always
//     tops up the neediest group first, every group reaches exactly its
//     target and no group is ever handed more shards than its target
//     allows.
//
// This moves the minimum number of shards required to reach the fixed
// targets (a group at its target is source and destination for nothing),
// and is fully deterministic.
func rebalance(shards []int64, gids []int64) []int64 {
	out := append([]int64(nil), shards...)
	n := len(out)
	if len(gids) == 0 {
		for i := range out {
			out[i] = 0
		}
		return out
	}

	base, extra := n/len(gids), n%len(gids)
	target := make(map[int64]int, len(gids))
	for i, gid := range gids {
		if i < extra {
			target[gid] = base + 1
		} else {
			target[gid] = base
		}
	}

	valid := make(map[int64]bool, len(gids))
	for _, gid := range gids {
		valid[gid] = true
	}
	count := make(map[int64]int, len(gids))
	var pool []int // shard indices awaiting assignment, in deterministic order
	for i, gid := range out {
		if valid[gid] {
			count[gid]++
		} else {
			out[i] = 0
			pool = append(pool, i)
		}
	}

	for _, gid := range gids {
		excess := count[gid] - target[gid]
		if excess <= 0 {
			continue
		}
		for i := 0; i < n && excess > 0; i++ {
			if out[i] == gid {
				out[i] = 0
				pool = append(pool, i)
				count[gid]--
				excess--
			}
		}
	}

	for _, idx := range pool {
		best := int64(-1)
		for _, gid := range gids {
			if count[gid] >= target[gid] {
				continue
			}
			if best == -1 || count[gid] < count[best] {
				best = gid
			}
		}
		if best == -1 {
			// Total deficit across groups always equals len(pool): targets
			// sum to n and every shard is accounted for exactly once. If
			// this fires, the invariant broke and the bug is above.
			panic("shardctrl: rebalance: no group has room for a pending shard")
		}
		out[idx] = best
		count[best]++
	}
	return out
}

// cloneConfig deep-copies cfg so a caller can never mutate a Machine's
// stored history through a value it was handed, matching the defensive
// copying store.Machine.Get does for its byte slices.
func cloneConfig(cfg *shardctrlv1.Config) *shardctrlv1.Config {
	groups := make(map[int64]*shardctrlv1.Group, len(cfg.Groups))
	for gid, g := range cfg.Groups {
		groups[gid] = &shardctrlv1.Group{Addrs: append([]string(nil), g.GetAddrs()...)}
	}
	return &shardctrlv1.Config{
		Num:    cfg.Num,
		Shards: append([]int64(nil), cfg.Shards...),
		Groups: groups,
	}
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

// snapshotVersion is bumped whenever the encoding changes shape, so an old
// binary refuses a snapshot it cannot interpret instead of misreading it.
const snapshotVersion uint32 = 1

// configImage and sessionImage are the on-the-wire (gob) forms. We do not
// gob-encode the generated proto structs directly: encoding_gob only walks
// exported fields, which would happen to work here, but going through a
// plain mirror type keeps the snapshot format decoupled from protoc-gen-go's
// internal struct layout, the same reasoning kv/store.Machine's
// SessionState follows.
type configImage struct {
	Num    int64
	Shards []int64
	Groups map[int64][]string
}

type sessionImage struct {
	LastID    uint64
	ErrKind   errKind
	ResultNum int64
}

type snapshotImage struct {
	Version  uint32
	Configs  []configImage
	Sessions map[string]sessionImage
}

// Snapshot encodes the whole machine — every Config ever produced, and the
// dedup table — into a byte slice Restore accepts. Unlike kv/store, we keep
// full history (Query can ask for any past Num), so a snapshot grows with
// administrative activity (Join/Leave/Move). What it bounds is the Raft
// LOG, which otherwise grows with every logged Query; see server.go.
func (m *Machine) Snapshot() ([]byte, error) {
	img := snapshotImage{
		Version:  snapshotVersion,
		Configs:  make([]configImage, len(m.configs)),
		Sessions: make(map[string]sessionImage, len(m.sessions)),
	}
	for i, cfg := range m.configs {
		groups := make(map[int64][]string, len(cfg.Groups))
		for gid, g := range cfg.Groups {
			groups[gid] = append([]string(nil), g.GetAddrs()...)
		}
		img.Configs[i] = configImage{
			Num:    cfg.Num,
			Shards: append([]int64(nil), cfg.Shards...),
			Groups: groups,
		}
	}
	for id, s := range m.sessions {
		img.Sessions[id] = sessionImage{LastID: s.lastID, ErrKind: s.errKind, ResultNum: s.resultNum}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&img); err != nil {
		return nil, fmt.Errorf("shardctrl: encode snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces all machine state with the contents of a snapshot
// produced by Snapshot. On error the machine is left unchanged.
func (m *Machine) Restore(b []byte) error {
	var img snapshotImage
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&img); err != nil {
		return fmt.Errorf("shardctrl: decode snapshot: %w", err)
	}
	if img.Version != snapshotVersion {
		return fmt.Errorf("shardctrl: snapshot version %d, want %d", img.Version, snapshotVersion)
	}
	configs := make([]*shardctrlv1.Config, len(img.Configs))
	for i, ci := range img.Configs {
		groups := make(map[int64]*shardctrlv1.Group, len(ci.Groups))
		for gid, addrs := range ci.Groups {
			groups[gid] = &shardctrlv1.Group{Addrs: append([]string(nil), addrs...)}
		}
		shards := ci.Shards
		if shards == nil {
			shards = make([]int64, shard.NShards)
		}
		configs[i] = &shardctrlv1.Config{Num: ci.Num, Shards: append([]int64(nil), shards...), Groups: groups}
	}
	if len(configs) == 0 {
		configs = []*shardctrlv1.Config{initialConfig()}
	}
	sessions := make(map[string]session, len(img.Sessions))
	for id, s := range img.Sessions {
		sessions[id] = session{lastID: s.LastID, errKind: s.ErrKind, resultNum: s.ResultNum}
	}
	m.configs = configs
	m.sessions = sessions
	return nil
}
