// simrun drives the Phase 6 deterministic-ish chaos harness (package chaos)
// from the command line.
//
//	simrun -seed 42 -duration 10s -nemesis-interval 40
//	simrun -seed 42 -duration 10s -nemesis-interval 40 -gateway   # also mix in LLM-serving traffic
//	simrun -quiet -duration 5s                                    # nemesis off: a control run
//
// A seed of 0 (the default) picks a random one and prints it, so every run
// is reproducible after the fact just by re-running with -seed <printed>.
// On any violation, simrun exits 1 and prints the exact repro command.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dsys/chaos"
)

func main() {
	seed := flag.Int64("seed", 0, "0 = pick a random seed and print it")
	duration := flag.Duration("duration", 8*time.Second, "how long to run the workload")
	nodes := flag.Int("nodes", 5, "Raft replicas in the KV cluster")
	clients := flag.Int("clients", 6, "concurrent KV/scheduler workload clients")
	nemesisInterval := flag.Int("nemesis-interval", 30, "roughly one nemesis event per this many KV ops; 0 disables it")
	pauseDur := flag.Duration("pause-duration", 200*time.Millisecond, "how long an EventPause holds a node")
	scheduler := flag.Bool("scheduler", true, "run the scheduler workload alongside the raw KV workload")
	gatewayMix := flag.Bool("gateway", false, "also mix in gateway + mock inference worker traffic")
	workers := flag.Int("workers", 3, "inference workers when -gateway is set")
	replyLoss := flag.Float64("reply-loss", 0.05, "probability an applied KV mutation's reply is dropped, forcing kv/client to retry it (dedup path); 0 disables")
	quiet := flag.Bool("quiet", false, "alias for -nemesis-interval 0: a control run with no injected faults")
	verbose := flag.Bool("v", true, "print each nemesis event as it happens")
	flag.Parse()

	s := *seed
	if s == 0 {
		s = rand.New(rand.NewSource(time.Now().UnixNano())).Int63()
	}
	interval := *nemesisInterval
	if *quiet {
		interval = 0
	}

	sc := chaos.Scenario{
		Seed: s, Duration: *duration, NumKVNodes: *nodes, NumClients: *clients,
		NemesisInterval: interval, PauseDuration: *pauseDur,
		IncludeScheduler: *scheduler, IncludeGateway: *gatewayMix, NumWorkers: *workers,
		ClientReplyLoss: *replyLoss,
	}
	if *verbose {
		sc.Logf = func(format string, args ...any) { log.Printf(format, args...) }
	}

	fmt.Printf("simrun: seed=%d duration=%s nodes=%d clients=%d nemesis-interval=%d gateway=%v\n",
		s, *duration, *nodes, *clients, interval, *gatewayMix)

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	start := time.Now()
	rep, err := chaos.Run(ctx, sc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simrun: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("\n--- report (wall time %s) ---\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("kv ops: %d   scheduler jobs submitted: %d   gateway requests: %d   nemesis events: %d\n",
		rep.KVOps, rep.SchedJobs, rep.GatewayReq, len(rep.Events))
	fmt.Printf("kv check: %s   kv/client retries: %d   mutation redeliveries (dedup path): %d   replies dropped: %d   client identities: %d\n",
		rep.KVCheck, rep.KVRetries, rep.KVRedeliveries, rep.KVRepliesLost, rep.KVIdentities)
	if sc.IncludeScheduler {
		fmt.Printf("scheduler: completes accepted: %d   zombie completes fenced: %d   idempotency keys: %d   duplicate submits: %d\n",
			rep.SchedDone, rep.SchedFenced, rep.SchedIdemKeys, rep.SchedIdemDupSubmits)
	}
	for _, e := range rep.Events {
		fmt.Printf("  op#%-5d %-10s node %d\n", e.Index, e.Kind, e.Node)
	}

	if rep.Passed() {
		fmt.Println("\nPASS: no invariant violated.")
		return
	}

	fmt.Println("\nFAIL:")
	for _, v := range rep.Violations {
		fmt.Println("  -", v)
	}
	fmt.Printf("\nReproduce with:\n  simrun -seed %d -duration %s -nodes %d -clients %d -nemesis-interval %d -pause-duration %s -scheduler=%v -gateway=%v -workers %d -reply-loss %g\n",
		rep.Seed, sc.Duration, sc.NumKVNodes, sc.NumClients, sc.NemesisInterval, sc.PauseDuration, sc.IncludeScheduler, sc.IncludeGateway, sc.NumWorkers, sc.ClientReplyLoss)
	os.Exit(1)
}
