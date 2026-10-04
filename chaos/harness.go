package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"dsys/kv/client"
	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/simnet"
)

// panicLog collects panics recovered in ANY goroutine the harness starts.
// recover() only catches a panic on its own goroutine, so every harness
// goroutine defers guard itself (via goWG); each recovered panic becomes a
// violation. Panics inside library-owned goroutines (e.g. the gateway's
// per-attempt stream pumps) cannot be recovered from here: they crash the
// test binary, which fails the run just as loudly.
type panicLog struct {
	mu   sync.Mutex
	msgs []string
}

// guard must be deferred DIRECTLY (defer pl.guard(name)) so that its
// recover() is called by the deferred function itself.
func (p *panicLog) guard(name string) {
	if r := recover(); r != nil {
		p.mu.Lock()
		p.msgs = append(p.msgs, fmt.Sprintf("%s: panic: %v\n%s", name, r, debug.Stack()))
		p.mu.Unlock()
	}
}

// goWG runs fn on a new goroutine under guard, tracked by wg if non-nil.
func (p *panicLog) goWG(wg *sync.WaitGroup, name string, fn func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer p.guard(name)
		fn()
	}()
}

func (p *panicLog) violations() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.msgs...)
}

// retryableKVErr reports whether err is a failure a KV client may
// legitimately see under faults: its own deadline, or a transient gRPC
// status kv/client gave up retrying. Anything else (FailedPrecondition from
// a stale request id, InvalidArgument, Internal, Unknown) is a bug.
func retryableKVErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Canceled:
		return true
	}
	return false
}

// waitClusterAvailable polls cluster with a trivial Get (through the
// production client) until it succeeds or budget elapses, so the harness
// proceeds to checks exactly when the cluster is actually ready rather than
// after a guessed sleep. ctx is the run's parent context (runCtx, timed to
// sc.Duration, is already done by the time this is called), used for the
// poll's own deadline.
func waitClusterAvailable(ctx context.Context, cluster *kvCluster, budget time.Duration, logf func(string, ...any)) {
	deadline := time.Now().Add(budget)
	attempt := 0
	for time.Now().Before(deadline) {
		attempt++
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, _, err := cluster.client.Get(pctx, "__chaos_availability_probe__")
		cancel()
		if err == nil {
			if attempt > 1 {
				logf("cluster answered again after %d probe(s)", attempt)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	logf("cluster did not answer within %s after the run ended; checks proceed anyway and may report it", budget)
}

// Run wires the stack described by sc, drives it for sc.Duration, and
// checks every invariant this package knows how to check. See the package
// doc for exactly what "seed-driven" does and does not guarantee.
func Run(ctx context.Context, sc Scenario) (*Report, error) {
	sc = sc.withDefaults()
	rep := &Report{Seed: sc.Seed, Scenario: sc}
	wallStart := time.Now()
	logf := sc.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	net := simnet.NewSeeded(sc.NumKVNodes, sc.Seed)
	kvcfg := raftkv.Config{
		CommitTimeout: 2 * time.Second,
		Raft: raft.Config{
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 250 * time.Millisecond,
			ElectionTimeoutMax: 500 * time.Millisecond,
		},
	}
	cluster, err := newKVCluster(sc.NumKVNodes, net, kvcfg, kvClusterOptions{
		seed:                 sc.Seed ^ 0x5EED1,
		replyLoss:            sc.ClientReplyLoss,
		attemptTimeout:       time.Second,
		freshMetaPerDelivery: sc.faults.kvFreshMeta,
	})
	if err != nil {
		return nil, err
	}
	defer cluster.killAll()
	pl := &panicLog{}

	runCtx, cancel := context.WithTimeout(ctx, sc.Duration)
	defer cancel()

	var wg sync.WaitGroup
	var opCount atomic.Int64

	// ---- Raw KV workload: builds a Porcupine operation history. --------
	// Each logical client owns ONE kv/client Session for the whole run: one
	// client id, request ids 1, 2, 3, ... across all of its operations, and
	// kv/client's own retry loop (same RequestMeta on every attempt, leader
	// hints, per-attempt timeouts) underneath each one. That is how a
	// production caller holds an identity, and it is what makes the store's
	// dedup table see a real per-identity sequence, including an abandoned
	// request whose log entry commits after its caller has moved on (the
	// store skips it as stale; Porcupine allows it because a failed op is
	// pending forever).
	var histMu sync.Mutex
	var history []porcupine.Operation
	var kvBadErrs []string
	opStart := time.Now()

	var shared *client.Client
	if sc.faults.kvSharedSession {
		shared = cluster.client.Session() // injected bug: one identity, many concurrent callers
	}
	for cid := 0; cid < sc.NumClients; cid++ {
		sess := shared
		if sess == nil {
			sess = cluster.client.Session()
		}
		pl.goWG(&wg, fmt.Sprintf("kv-client-%d", cid), func() {
			r := rand.New(rand.NewSource(sc.Seed + int64(cid) + 1))
			keys := []string{"a", "b", "c", "d", "e"}
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				in := kvInput{key: keys[r.Intn(len(keys))]}
				switch r.Intn(3) {
				case 0:
					in.op = "get"
				case 1:
					in.op, in.value = "put", fmt.Sprintf("%d-%d", cid, r.Intn(1000))
				default:
					in.op = "cas"
					in.value = fmt.Sprintf("%d-%d", cid, r.Intn(1000))
					in.expected = fmt.Sprintf("%d-%d", r.Intn(sc.NumClients), r.Intn(20))
				}
				octx, ocancel := context.WithTimeout(runCtx, 3*time.Second)
				call := time.Now()
				var out kvOutput
				var err error
				switch in.op {
				case "get":
					var v []byte
					v, out.found, err = sess.Get(octx, in.key)
					out.value = string(v)
				case "put":
					err = sess.Put(octx, in.key, []byte(in.value))
				case "cas":
					var cur []byte
					out.swapped, cur, err = sess.CAS(octx, in.key, []byte(in.expected), false, []byte(in.value))
					out.current = string(cur)
				}
				ocancel()
				ret := time.Now()
				if err != nil {
					// Outcome unknown: the mutation may or may not have
					// taken effect, so Porcupine sees it as pending
					// "forever" (and a failed get constrains nothing).
					out = kvOutput{failed: true}
					ret = opStart.Add(sc.Duration + 10*time.Second)
				}
				histMu.Lock()
				if err != nil && !retryableKVErr(err) && len(kvBadErrs) < 5 {
					kvBadErrs = append(kvBadErrs, fmt.Sprintf("kv: client %d %s(%s) got a non-retryable error: %v", cid, in.op, in.key, err))
				}
				history = append(history, porcupine.Operation{
					ClientId: cid, Input: in, Output: out,
					Call: call.Sub(opStart).Nanoseconds(), Return: ret.Sub(opStart).Nanoseconds(),
				})
				histMu.Unlock()
				opCount.Add(1)
			}
		})
	}

	// ---- Scheduler workload, sharing the same disrupted KV. -------------
	var sw *schedWorkload
	if sc.IncludeScheduler {
		sw = newSchedWorkload(cluster.newAdapter(), sc.faults, sc.schedDrain, pl)
		sw.start(ctx, runCtx, sc.Seed+2, max(1, sc.NumClients/2), sc.NumClients, &wg)
		defer sw.reaperStop() // normally already stopped by check()
	}

	// ---- Optional gateway + inference workers. ---------------------------
	var gwMix *gatewayMix
	if sc.IncludeGateway {
		var err error
		gwMix, err = newGatewayMix(cluster.newAdapter(), sc, pl)
		if err != nil {
			return nil, fmt.Errorf("chaos: wiring gateway: %w", err)
		}
		defer gwMix.close()
		gwMix.run(runCtx, sc, &wg)
	}

	// ---- Nemesis. ---------------------------------------------------------
	// pending tracks every scheduled auto-heal/auto-restart (see
	// nemesisEvent.apply's doc comment): the harness must wait for ALL of
	// them to actually land, not just for the workload to stop, before it
	// is fair to check invariants.
	var pending sync.WaitGroup
	if sc.NemesisInterval > 0 {
		pl.goWG(&wg, "nemesis", func() {
			nr := rand.New(rand.NewSource(sc.Seed ^ 0x7E3E515))
			next := int64(sc.NemesisInterval/2 + nr.Intn(sc.NemesisInterval+1))
			for {
				select {
				case <-runCtx.Done():
					return
				case <-time.After(15 * time.Millisecond):
				}
				if opCount.Load() < next {
					continue
				}
				next = opCount.Load() + int64(sc.NemesisInterval/2+nr.Intn(sc.NemesisInterval+1))
				ev := chooseEvent(cluster, nr, sc.PauseDuration)
				if ev == nil {
					continue
				}
				ev.apply(cluster, &pending)
				idx := int(opCount.Load())
				histMu.Lock()
				rep.Events = append(rep.Events, Event{Index: idx, Kind: ev.kind, Node: ev.node, At: time.Now()})
				histMu.Unlock()
				logf("t+%s op#%d: %s node %d", time.Since(wallStart).Round(10*time.Millisecond), idx, ev.kind, ev.node)
			}
		})
	}

	<-runCtx.Done()
	wg.Wait()
	pending.Wait() // every scheduled restart/heal has now actually run its c.start/c.heal call
	// Standard practice for a nemesis (Jepsen and friends): once the
	// workload stops, heal the network unconditionally before checking
	// anything. The majority bound in chooseEvent means this should rarely
	// find work to do, but a Pause whose auto-heal goroutine is still
	// mid-sleep just landed on a fresh restart, or any bound-logic edge
	// case, is defended against here rather than trusted to resolve itself.
	for i := 0; i < sc.NumKVNodes; i++ {
		if cluster.isPartitioned(i) {
			cluster.heal(i)
		}
	}
	// Raft still needs real time after that to re-elect and catch up. A
	// FIXED sleep here (this harness's first version used one) is the
	// wrong tool: how long that takes depends on how many nodes were down
	// at once and, under `go test -race`, on how much the race detector's
	// instrumentation overhead is currently slowing every goroutine — both
	// vary run to run. Poll for the one thing that actually matters (the
	// cluster answers a request again) instead of guessing a duration; a
	// seed that happens to knock out a majority for longer gets exactly as
	// long as it needs, and a quiet run doesn't pay for a wait it didn't need.
	waitClusterAvailable(ctx, cluster, 15*time.Second, logf)

	rep.KVOps = len(history)
	rep.Elapsed = time.Since(wallStart)

	// ---- Checks. -----------------------------------------------------------
	// Each check phase gets its OWN fresh deadline, created immediately
	// before that phase runs. The first version of this harness created one
	// shared 30s checkCtx before the Porcupine call below — but Porcupine's
	// own search is worst-case exponential in the number of concurrent
	// "never returned" operations a chaotic run produces, and can itself
	// spend most of its 20s budget deciding. checkCtx's clock was already
	// ticking the whole time, so the scheduler check that ran AFTER it
	// could inherit only a few seconds of a nominally-30s budget — timing
	// out on a cluster that was actually fine, not on a real bug. Budgets
	// for unrelated phases must not share a clock.
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 20*time.Second)
	switch res {
	case porcupine.Illegal:
		rep.KVCheck = "illegal"
		rep.Violations = append(rep.Violations, "kv: history is NOT linearizable")
		_ = info // a caller wanting a visualisation can re-run CheckOperationsVerbose itself
	case porcupine.Unknown:
		rep.KVCheck = "unknown"
		logf("kv: linearizability check timed out (inconclusive, not counted as a violation)")
	default:
		rep.KVCheck = "ok"
	}
	rep.Violations = append(rep.Violations, kvBadErrs...)
	rep.KVRetries = cluster.client.Retries()
	rep.KVRedeliveries = cluster.stats.redeliveries.Load()
	rep.KVRepliesLost = cluster.stats.repliesLost.Load()
	cluster.stats.mu.Lock()
	rep.KVIdentities = len(cluster.stats.identities)
	cluster.stats.mu.Unlock()
	if n := cluster.stats.staleLive.Load(); n > 0 {
		rep.Violations = append(rep.Violations, fmt.Sprintf(
			"kv: %d mutation(s) rejected as stale while their caller still waited: a later request id of the same identity overtook an earlier, still-outstanding one", n))
	}
	logf("kv: ops=%d check=%s kv/client retries=%d mutation deliveries=%d redeliveries=%d replies lost=%d identities=%d",
		len(history), rep.KVCheck, rep.KVRetries, cluster.stats.mutations.Load(), rep.KVRedeliveries, rep.KVRepliesLost, rep.KVIdentities)

	if sw != nil {
		// The liveness drain's own budget, plus room for the record scan.
		checkCtx, checkCancel := context.WithTimeout(ctx, sw.drainBudget+30*time.Second)
		rep.Violations = append(rep.Violations, sw.check(checkCtx, sc.NumClients, logf)...)
		checkCancel()
		rep.SchedJobs = int(sw.submitted.Load())
		rep.SchedDone = sw.done.Load()
		rep.SchedFenced = sw.fenced.Load()
		rep.SchedIdemKeys = sw.idemKeys.Load()
		rep.SchedIdemDupSubmits = sw.idemDupCalls.Load()
		logf("scheduler: submitted=%d completed=%d fenced=%d zombieClaims=%d idemKeys=%d idemDupSubmits=%d",
			rep.SchedJobs, rep.SchedDone, rep.SchedFenced, sw.zombieClaims.Load(), rep.SchedIdemKeys, rep.SchedIdemDupSubmits)
	}
	if gwMix != nil {
		vs, st := gwMix.check(ctx) // runs the post-heal final batch first
		rep.Violations = append(rep.Violations, vs...)
		rep.GatewayReq = int(gwMix.requests.Load())
		rep.GatewayErrors = int(gwMix.errors.Load())
		rep.GatewayCacheHits, rep.GatewayStalled = st.cacheHits, st.stalled
		rep.GatewayFinalOK, rep.GatewayFinalTotal = st.finalOK, st.finalTotal
		logf("gateway: requests=%d errors=%d cacheHits=%d slowOK=%d stalled=%d final=%d/%d",
			rep.GatewayReq, rep.GatewayErrors, st.cacheHits, st.slowOK, st.stalled, st.finalOK, st.finalTotal)
	}
	rep.Violations = append(rep.Violations, pl.violations()...)

	return rep, nil
}
