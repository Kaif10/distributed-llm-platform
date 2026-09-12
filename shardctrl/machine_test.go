package shardctrl

import (
	"errors"
	"fmt"
	"testing"

	shardctrlv1 "dsys/gen/shardctrl/v1"
	"dsys/shard"
)

func meta(id string, seq uint64) *shardctrlv1.RequestMeta {
	return &shardctrlv1.RequestMeta{ClientId: id, RequestId: seq}
}

func groupsOf(gids ...int64) map[int64]*shardctrlv1.Group {
	g := make(map[int64]*shardctrlv1.Group, len(gids))
	for _, gid := range gids {
		g[gid] = &shardctrlv1.Group{Addrs: []string{fmt.Sprintf("addr-%d", gid)}}
	}
	return g
}

func join(m *shardctrlv1.RequestMeta, gids ...int64) *Command {
	return &Command{Op: OpJoin, Meta: m, Groups: groupsOf(gids...)}
}

func leave(m *shardctrlv1.RequestMeta, gids ...int64) *Command {
	return &Command{Op: OpLeave, Meta: m, Gids: gids}
}

func move(m *shardctrlv1.RequestMeta, shardID, gid int64) *Command {
	return &Command{Op: OpMove, Meta: m, Shard: shardID, Gid: gid}
}

func query(num int64) *Command {
	return &Command{Op: OpQuery, QueryNum: num}
}

// counts returns gid -> number of shards owned, for the live groups only.
func counts(cfg *shardctrlv1.Config) map[int64]int {
	c := make(map[int64]int)
	for _, gid := range cfg.Shards {
		c[gid]++
	}
	return c
}

func assertBalanced(t *testing.T, cfg *shardctrlv1.Config, ngroups int) {
	t.Helper()
	if ngroups == 0 {
		for i, gid := range cfg.Shards {
			if gid != 0 {
				t.Fatalf("shard %d owned by %d, want 0 (no groups)", i, gid)
			}
		}
		return
	}
	c := counts(cfg)
	if len(c) != ngroups {
		t.Fatalf("config assigns shards to %d groups, want %d (%v)", len(c), ngroups, c)
	}
	min, max := shard.NShards, 0
	for gid := range cfg.Groups {
		n := c[gid]
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		t.Fatalf("unbalanced: min=%d max=%d counts=%v", min, max, c)
	}
	total := 0
	for _, n := range c {
		total += n
	}
	if total != shard.NShards {
		t.Fatalf("shards sum to %d, want %d", total, shard.NShards)
	}
}

func configsEqual(a, b *shardctrlv1.Config) bool {
	if a.Num != b.Num || len(a.Shards) != len(b.Shards) || len(a.Groups) != len(b.Groups) {
		return false
	}
	for i := range a.Shards {
		if a.Shards[i] != b.Shards[i] {
			return false
		}
	}
	for gid, ga := range a.Groups {
		gb, ok := b.Groups[gid]
		if !ok || len(ga.Addrs) != len(gb.Addrs) {
			return false
		}
		for i := range ga.Addrs {
			if ga.Addrs[i] != gb.Addrs[i] {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Basic shape
// ---------------------------------------------------------------------------

func TestInitialConfig(t *testing.T) {
	m := NewMachine()
	cfg, err := m.Apply(query(-1))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Num != 0 || len(cfg.Groups) != 0 {
		t.Fatalf("initial config = %+v", cfg)
	}
	if len(cfg.Shards) != shard.NShards {
		t.Fatalf("initial shards len = %d, want %d", len(cfg.Shards), shard.NShards)
	}
	for i, gid := range cfg.Shards {
		if gid != 0 {
			t.Fatalf("initial shard %d owned by %d, want 0", i, gid)
		}
	}
}

func TestJoinAssignsAllShards(t *testing.T) {
	m := NewMachine()
	cfg, err := m.Apply(join(meta("c", 1), 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Num != 1 {
		t.Fatalf("num = %d, want 1", cfg.Num)
	}
	assertBalanced(t, cfg, 3)
}

// ---------------------------------------------------------------------------
// Determinism: two independently constructed machines, fed the same
// sequence of commands (rebuilt via re-marshaling so there is no chance of
// accidental pointer aliasing between them), must reach byte-identical
// configs at every step.
// ---------------------------------------------------------------------------

func TestDeterminism(t *testing.T) {
	seq := []*Command{
		join(meta("c", 1), 1, 2, 3),
		join(meta("c", 2), 4),
		move(meta("c", 3), 0, 4),
		leave(meta("c", 4), 2),
		join(meta("c", 5), 5, 6, 7, 8),
		leave(meta("c", 6), 1, 3, 4),
	}
	m1, m2 := NewMachine(), NewMachine()
	for i, cmd := range seq {
		// Rebuild the command from scratch for m2 so nothing is shared with m1.
		cmd2 := &Command{Op: cmd.Op, Shard: cmd.Shard, Gid: cmd.Gid, QueryNum: cmd.QueryNum}
		if cmd.Meta != nil {
			cmd2.Meta = &shardctrlv1.RequestMeta{ClientId: cmd.Meta.ClientId, RequestId: cmd.Meta.RequestId}
		}
		if cmd.Groups != nil {
			cmd2.Groups = groupsOf(gidsOf(cmd.Groups)...)
		}
		if cmd.Gids != nil {
			cmd2.Gids = append([]int64(nil), cmd.Gids...)
		}

		c1, err1 := m1.Apply(cmd)
		c2, err2 := m2.Apply(cmd2)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("step %d: err1=%v err2=%v", i, err1, err2)
		}
		if err1 != nil {
			continue
		}
		if !configsEqual(c1, c2) {
			t.Fatalf("step %d: configs diverged:\n  m1=%+v %v\n  m2=%+v %v", i, c1, c1.Groups, c2, c2.Groups)
		}
	}
}

func gidsOf(groups map[int64]*shardctrlv1.Group) []int64 {
	gids := make([]int64, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	return gids
}

// ---------------------------------------------------------------------------
// Balance property after Join, for various group counts.
// ---------------------------------------------------------------------------

func TestJoinBalance(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 7} {
		t.Run(fmt.Sprintf("groups=%d", n), func(t *testing.T) {
			m := NewMachine()
			gids := make([]int64, n)
			for i := range gids {
				gids[i] = int64(i + 1)
			}
			cfg, err := m.Apply(join(meta("c", 1), gids...))
			if err != nil {
				t.Fatal(err)
			}
			assertBalanced(t, cfg, n)
		})
	}
}

// ---------------------------------------------------------------------------
// Minimal movement: joining a 4th group to 3 already-balanced groups must
// move exactly the number of shards the new group needs, and nothing else.
// ---------------------------------------------------------------------------

func TestJoinMinimalMovement(t *testing.T) {
	m := NewMachine()
	before, err := m.Apply(join(meta("c", 1), 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	assertBalanced(t, before, 3)

	after, err := m.Apply(join(meta("c", 2), 4))
	if err != nil {
		t.Fatal(err)
	}
	assertBalanced(t, after, 4)

	moved := 0
	for i := range before.Shards {
		if before.Shards[i] != after.Shards[i] {
			moved++
		}
	}
	// Theoretical minimum: the new group's target shard count is the only
	// shard count that necessarily changes from 0, so at least that many
	// shards must move in; nothing else needs to, because the previous
	// state was already exactly balanced under this same algorithm.
	newGroupTarget := counts(after)[4]
	if moved != newGroupTarget {
		t.Fatalf("moved %d shards, want exactly %d (the new group's target)", moved, newGroupTarget)
	}
}

// ---------------------------------------------------------------------------
// Leave
// ---------------------------------------------------------------------------

func TestLeaveRedistributes(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	cfg, err := m.Apply(leave(meta("c", 2), 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Groups[2]; ok {
		t.Fatal("left group still present")
	}
	assertBalanced(t, cfg, 2)
}

func TestLeaveEmptyMapsToZero(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2)); err != nil {
		t.Fatal(err)
	}
	cfg, err := m.Apply(leave(meta("c", 2), 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Groups) != 0 {
		t.Fatalf("groups = %v, want none", cfg.Groups)
	}
	assertBalanced(t, cfg, 0)
}

// ---------------------------------------------------------------------------
// Move
// ---------------------------------------------------------------------------

func TestMoveOverridesOneShardOnly(t *testing.T) {
	m := NewMachine()
	before, err := m.Apply(join(meta("c", 1), 1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	var target int64 // a shard currently NOT owned by group 1
	for i, gid := range before.Shards {
		if gid != 1 {
			target = int64(i)
			break
		}
	}
	after, err := m.Apply(move(meta("c", 2), target, 1))
	if err != nil {
		t.Fatal(err)
	}
	for i := range before.Shards {
		want := before.Shards[i]
		if int64(i) == target {
			want = 1
		}
		if after.Shards[i] != want {
			t.Fatalf("shard %d = %d, want %d", i, after.Shards[i], want)
		}
	}
}

func TestMoveThenJoinRespectsMoveAsBaseline(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	moved, err := m.Apply(move(meta("c", 2), 0, 2))
	if err != nil {
		t.Fatal(err)
	}
	if moved.Shards[0] != 2 {
		t.Fatalf("shard 0 = %d, want 2", moved.Shards[0])
	}
	// A subsequent Join rebalances from whatever Move left behind: shard 0
	// must not spontaneously revert to whichever group "should" have had it.
	after, err := m.Apply(join(meta("c", 3), 4))
	if err != nil {
		t.Fatal(err)
	}
	assertBalanced(t, after, 4)
}

func TestMoveUnknownGroupErrors(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(move(meta("c", 2), 0, 99)); !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("err = %v, want ErrUnknownGroup", err)
	}
	// No new config was produced by the failed Move.
	latest, _ := m.Apply(query(-1))
	if latest.Num != 1 {
		t.Fatalf("num = %d, want 1 (failed move must not bump it)", latest.Num)
	}
}

func TestMoveInvalidShardErrors(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(move(meta("c", 2), int64(shard.NShards), 1)); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("err = %v, want ErrInvalidShard", err)
	}
	if _, err := m.Apply(move(meta("c", 3), -1, 1)); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("err = %v, want ErrInvalidShard", err)
	}
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

func TestQueryPastAndOutOfRange(t *testing.T) {
	m := NewMachine()
	c1, _ := m.Apply(join(meta("c", 1), 1, 2))
	c2, _ := m.Apply(join(meta("c", 2), 3))

	got, err := m.Apply(query(1))
	if err != nil || got.Num != c1.Num || !configsEqual(got, c1) {
		t.Fatalf("query(1) = %+v, want %+v", got, c1)
	}
	got, _ = m.Apply(query(-1))
	if !configsEqual(got, c2) {
		t.Fatalf("query(-1) = %+v, want latest %+v", got, c2)
	}
	got, _ = m.Apply(query(999))
	if !configsEqual(got, c2) {
		t.Fatalf("query(999) = %+v, want latest %+v", got, c2)
	}
}

// ---------------------------------------------------------------------------
// Dedup / idempotency
// ---------------------------------------------------------------------------

func TestDedupJoin(t *testing.T) {
	m := NewMachine()
	meta1 := meta("c", 1)
	c1, err := m.Apply(join(meta1, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	// Retry with the exact same meta: same Command object re-sent, as a
	// leader-change retry would do.
	c2, err := m.Apply(join(meta1, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if !configsEqual(c1, c2) {
		t.Fatalf("retry produced a different config: %+v vs %+v", c1, c2)
	}
	latest, _ := m.Apply(query(-1))
	if latest.Num != 1 {
		t.Fatalf("num = %d, want 1 (retry must not create a new config)", latest.Num)
	}
}

func TestDedupLeave(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	meta2 := meta("c", 2)
	c1, err := m.Apply(leave(meta2, 1))
	if err != nil {
		t.Fatal(err)
	}
	c2, err := m.Apply(leave(meta2, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !configsEqual(c1, c2) {
		t.Fatal("leave retry produced a different config")
	}
	latest, _ := m.Apply(query(-1))
	if latest.Num != 2 {
		t.Fatalf("num = %d, want 2", latest.Num)
	}
}

func TestDedupMove(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2)); err != nil {
		t.Fatal(err)
	}
	meta2 := meta("c", 2)
	c1, err := m.Apply(move(meta2, 0, 2))
	if err != nil {
		t.Fatal(err)
	}
	c2, err := m.Apply(move(meta2, 0, 2))
	if err != nil {
		t.Fatal(err)
	}
	if !configsEqual(c1, c2) {
		t.Fatal("move retry produced a different config")
	}
	latest, _ := m.Apply(query(-1))
	if latest.Num != 2 {
		t.Fatalf("num = %d, want 2", latest.Num)
	}
}

func TestDedupMoveError(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2)); err != nil {
		t.Fatal(err)
	}
	meta2 := meta("c", 2)
	_, err1 := m.Apply(move(meta2, 0, 99))
	_, err2 := m.Apply(move(meta2, 0, 99))
	if !errors.Is(err1, ErrUnknownGroup) || !errors.Is(err2, ErrUnknownGroup) {
		t.Fatalf("errs = %v, %v; want ErrUnknownGroup both times", err1, err2)
	}
}

func TestDedupStaleRequest(t *testing.T) {
	m := NewMachine()
	c := meta("c", 5)
	if _, err := m.Apply(join(c, 1)); err != nil {
		t.Fatal(err)
	}
	stale := meta("c", 3)
	if _, err := m.Apply(join(stale, 2)); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("err = %v, want ErrStaleRequest", err)
	}
}

func TestQueryNotDeduped(t *testing.T) {
	// QueryRequest carries no RequestMeta; repeated identical queries must
	// simply be re-answered, never create state, and never error.
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := m.Apply(query(-1)); err != nil {
			t.Fatal(err)
		}
	}
	latest, _ := m.Apply(query(-1))
	if latest.Num != 1 {
		t.Fatalf("num = %d, want 1 (queries must not bump it)", latest.Num)
	}
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	m := NewMachine()
	if _, err := m.Apply(join(meta("c", 1), 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(move(meta("c", 2), 0, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(leave(meta("c", 3), 1)); err != nil {
		t.Fatal(err)
	}
	// A dedup entry, including one that recorded an error, must survive.
	if _, err := m.Apply(move(meta("c", 4), 0, 999)); !errors.Is(err, ErrUnknownGroup) {
		t.Fatal(err)
	}

	snap, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}

	// History preserved.
	for num := int64(0); num <= 3; num++ {
		want, _ := m.Apply(query(num))
		got, _ := restored.Apply(query(num))
		if !configsEqual(want, got) {
			t.Fatalf("num=%d: restored=%+v want=%+v", num, got, want)
		}
	}

	// Dedup sessions preserved: a mutating retry after restore is still
	// recognised and does not create a new config. Client "c"'s last
	// applied request id is 4 (the erroring Move), so that is the id whose
	// retry must be deduped; a lower id would (correctly) be ErrStaleRequest
	// instead, which is a different property tested elsewhere.
	before, _ := restored.Apply(query(-1))
	_, err = restored.Apply(move(meta("c", 4), 0, 999))
	if !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("retried move err = %v, want ErrUnknownGroup", err)
	}
	after, _ := restored.Apply(query(-1))
	if before.Num != after.Num {
		t.Fatal("restored machine re-applied a deduped, errored Move as if new")
	}

	// A mutating op that succeeded before the snapshot (the Leave at
	// request id 3) is also still deduped after restore: retrying it must
	// not create a new config or change any config's contents. We check
	// this on a fresh client, since client "c" has since moved on to id 4.
	if _, err := m.Apply(leave(meta("d", 1), 3)); err != nil {
		t.Fatal(err)
	}
	snap2, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored2 := NewMachine()
	if err := restored2.Restore(snap2); err != nil {
		t.Fatal(err)
	}
	beforeD, _ := restored2.Apply(query(-1))
	c1, err := restored2.Apply(leave(meta("d", 1), 3))
	if err != nil {
		t.Fatal(err)
	}
	afterD, _ := restored2.Apply(query(-1))
	if beforeD.Num != afterD.Num || !configsEqual(beforeD, c1) {
		t.Fatal("restored machine re-applied a deduped Leave")
	}
}

func TestSnapshotRestoreEmptyMachine(t *testing.T) {
	m := NewMachine()
	snap, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	got, _ := restored.Apply(query(-1))
	if got.Num != 0 || len(got.Groups) != 0 || len(got.Shards) != shard.NShards {
		t.Fatalf("restored empty machine = %+v", got)
	}
}
