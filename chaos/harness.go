package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"dsys/kv/raftkv"
	"dsys/raft"
	"dsys/raft/simnet"
	"dsys/sched"
)

// Run wires the stack described by sc, drives it for sc.Duration, and
// checks every invariant this package knows how to check. See the package
// doc for exactly what "seed-driven" does and does not guarantee.
// waitClusterAvailable polls cluster with a trivial Get until it succeeds or
// budget elapses, so the harness proceeds to checks exactly when the
// cluster is actually ready rather than after a guessed sleep. ctx is the
// run's parent context (runCtx, timed to sc.Duration, is already done by
// the time this is called), used for the poll's own deadline.
func waitClusterAvailable(ctx context.Context, cluster *kvCluster, budget time.Duration, logf func(string, ...any)) {
	deadline := time.Now().Add(budget)
	attempt := 0
	for time.Now().Before(deadline) {
		attempt++
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, _, err := cluster.Get(pctx, "__chaos_availability_probe__")
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
	cluster := newKVCluster(sc.NumKVNodes, net, kvcfg, sc.Seed^0x5EED1)
	defer cluster.killAll()

	runCtx, cancel := context.WithTimeout(ctx, sc.Duration)
	defer cancel()

	var wg sync.WaitGroup
	var opCount atomic.Int64

	// ---- Raw KV workload: builds a Porcupine operation history. --------
	var histMu sync.Mutex
	var history []porcupine.Operation
	opStart := time.Now()

	for cid := 0; cid < sc.NumClients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
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
					v, out.found, err = cluster.Get(octx, in.key)
					out.value = string(v)
				case "put":
					err = cluster.Put(octx, in.key, []byte(in.value))
				case "cas":
					var cur []byte
					out.swapped, cur, err = cluster.CAS(octx, in.key, []byte(in.expected), false, []byte(in.value))
					out.current = string(cur)
				}
				ocancel()
				ret := time.Now()
				if err != nil {
					out = kvOutput{failed: true}
					ret = opStart.Add(sc.Duration + 10*time.Second) // pending "forever" for Porcupine
				}
				histMu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: cid, Input: in, Output: out,
					Call: call.Sub(opStart).Nanoseconds(), Return: ret.Sub(opStart).Nanoseconds(),
				})
				histMu.Unlock()
				opCount.Add(1)
			}
		}(cid)
	}

	// ---- Scheduler workload, sharing the same disrupted KV. -------------
	var sw *schedWorkload
	if sc.IncludeScheduler {
		sw = newSchedWorkload(cluster)
		reaperCtx, reaperCancel := context.WithCancel(ctx)
		defer reaperCancel()
		go func() {
			<-runCtx.Done()
			time.Sleep(2 * time.Second) // let in-flight jobs settle before the reaper stops
			reaperCancel()
		}()
		sw.runProducers(runCtx, sc.Seed+2, max(1, sc.NumClients/2), &wg)
		sw.runWorkers(runCtx, sc.Seed+3, sc.NumClients, &wg)
		go sched.RunReaper(reaperCtx, sw.q, "chaos-reaper", 200*time.Millisecond)
	}

	// ---- Optional gateway + inference workers. ---------------------------
	var gwMix *gatewayMix
	if sc.IncludeGateway {
		var err error
		gwMix, err = newGatewayMix(cluster, sc)
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
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
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
	if res == porcupine.Illegal {
		rep.Violations = append(rep.Violations, "kv: history is NOT linearizable")
		_ = info // a caller wanting a visualisation can re-run CheckOperationsVerbose itself
	} else if res == porcupine.Unknown {
		logf("kv: linearizability check timed out (inconclusive, not counted as a violation)")
	}

	if sw != nil {
		checkCtx, checkCancel := context.WithTimeout(ctx, 30*time.Second)
		rep.SchedJobs = int(sw.submitted.Load())
		rep.Violations = append(rep.Violations, sw.check(checkCtx, logf)...)
		checkCancel()
		logf("scheduler: submitted=%d completed=%d fenced=%d", sw.submitted.Load(), sw.done.Load(), sw.fenced.Load())
	}
	if gwMix != nil {
		rep.GatewayReq = int(gwMix.requests.Load())
		rep.Violations = append(rep.Violations, gwMix.check()...)
		logf("gateway: requests=%d errors=%d panics=%d", gwMix.requests.Load(), gwMix.errors.Load(), gwMix.panics.Load())
	}

	return rep, nil
}
