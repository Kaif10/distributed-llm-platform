// Command worker is a demo worker for the sched job queue, built on
// sched/worker. Its handler treats the payload as JSON
// {"sleep_ms": N, "echo": "..."} (anything that is not JSON sleeps 100ms)
// and returns {"echo": "...", "worker": "<name>"} as the result.
//
//	worker -sched 127.0.0.1:7100 [-name w1] [-concurrency 1] [-lease 10s]
//
// # The "pause a worker past its lease" experiment
//
// The roadmap asks you to pause a worker past its lease, resume it, and
// confirm it is fenced. -slow-after n makes the handler sleep 3x the lease
// on the (n+1)th job; -suppress-heartbeat stops the worker renewing leases.
// Together they simulate a worker that was SIGSTOPped or GC-paused across
// its lease and does not know it. With the scheduler on :7100:
//
//	# terminal 1: a healthy worker that will pick up whatever is reaped
//	worker -sched 127.0.0.1:7100 -name healthy -lease 2s
//
//	# terminal 2: the worker that will become a zombie on its 2nd job
//	worker -sched 127.0.0.1:7100 -name zombie -lease 2s -slow-after 1 -suppress-heartbeat
//
//	# terminal 3: give them work, then watch
//	schedctl -sched 127.0.0.1:7100 submit '{"sleep_ms":10,"echo":"a"}'
//	schedctl -sched 127.0.0.1:7100 submit '{"sleep_ms":10,"echo":"b"}'
//	schedctl -sched 127.0.0.1:7100 watch 2
//
// What you should see: zombie claims job 2 at gen 1 and goes quiet for 6s
// with no heartbeats; ~2s in the reaper hands job 2 to healthy at a higher
// gen, which completes it; when zombie wakes and calls Complete(2, gen=1)
// it is refused with FailedPrecondition and logs
// "zombie: Complete refused ... result discarded", its Fenced counter goes
// to 1, and the recorded result carries "worker":"healthy". Run the same
// thing WITHOUT -suppress-heartbeat and the slow job simply completes on
// zombie: heartbeats kept the lease alive, which is the whole point of them.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	schedv1 "dsys/gen/sched/v1"
	"dsys/sched/worker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("sched", "127.0.0.1:7100", "scheduler address")
	name := flag.String("name", "", "worker name (default hostname-pid)")
	concurrency := flag.Int("concurrency", 1, "jobs to run in parallel")
	lease := flag.Duration("lease", 10*time.Second, "lease to request; heartbeats every lease/3")
	slowAfter := flag.Int("slow-after", -1, "after this many jobs, sleep 3x the lease on the next one (-1 = never)")
	suppressHB := flag.Bool("suppress-heartbeat", false, "never renew leases (experiment only: pairs with -slow-after)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial", "err", err)
		os.Exit(1)
	}
	defer cc.Close()

	var seen atomic.Int64
	w := worker.New(schedv1.NewSchedulerClient(cc), func(ctx context.Context, job *schedv1.Job) ([]byte, error) {
		n := seen.Add(1) - 1
		var p struct {
			SleepMs int    `json:"sleep_ms"`
			Echo    string `json:"echo"`
		}
		sleep := 100 * time.Millisecond
		if json.Unmarshal(job.GetPayload(), &p) == nil && p.SleepMs > 0 {
			sleep = time.Duration(p.SleepMs) * time.Millisecond
		}
		if *slowAfter >= 0 && n == int64(*slowAfter) {
			sleep = 3 * *lease
			log.Warn("deliberately sleeping past the lease", "job", job.GetId(), "gen", job.GetGen(), "sleep", sleep)
		}
		// Sleep with ctx so a fenced or stopping worker does not keep us
		// busy; with -suppress-heartbeat nothing fences us early, so the
		// full sleep elapses and Complete is what gets refused.
		t := time.NewTimer(sleep)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.Marshal(map[string]string{"echo": p.Echo, "worker": *name})
	}, worker.Options{
		Name:              *name,
		Lease:             *lease,
		Concurrency:       *concurrency,
		Logger:            log,
		SuppressHeartbeat: *suppressHB,
	})

	// SIGTERM too: it is what `docker stop` (and systemd, k8s) sends, and
	// without it the graceful path below never runs in a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("worker running", "sched", *addr, "concurrency", *concurrency, "lease", *lease, "suppress_heartbeat", *suppressHB)
	if err := w.Run(ctx); err != nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	s := w.Stats()
	fmt.Fprintf(os.Stderr, "stopped: claimed=%d completed=%d failed=%d fenced=%d\n", s.Claimed, s.Completed, s.Failed, s.Fenced)
}
