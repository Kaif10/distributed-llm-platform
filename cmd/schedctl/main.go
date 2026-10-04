// Command schedctl is a small command-line client for the sched gRPC service.
//
//	schedctl [-sched host:port] [-timeout 30s] submit <payload> [-key idem]
//	schedctl [-sched ...] status <id>
//	schedctl [-sched ...] stats
//	schedctl [-sched ...] watch <id>
//	schedctl [-sched ...] bench [-n 1000]
//
// Transient failures (Unavailable, DeadlineExceeded) are retried with
// backoff within -timeout. A Submit refused with ResourceExhausted (queue
// full) is retried after the retry_after_ms the scheduler puts in the
// status message: admission control is the scheduler's, and the client's
// job is to honour it rather than hammer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	schedv1 "dsys/gen/sched/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	backoffMin     = 10 * time.Millisecond
	backoffMax     = 200 * time.Millisecond
	attemptTimeout = 2 * time.Second
	watchInterval  = 200 * time.Millisecond
)

// retryAfter matches the hint the scheduler puts in a ResourceExhausted
// status message.
var retryAfter = regexp.MustCompile(`retry_after_ms=(\d+)`)

func main() {
	addr := flag.String("sched", "127.0.0.1:9001", "scheduler address (cmd/sched's default -addr)")
	timeout := flag.Duration("timeout", 30*time.Second, "give up retrying a request after this long")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: schedctl [-sched host:port] [-timeout 30s] submit|status|stats|watch|bench ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer cc.Close()
	c := schedv1.NewSchedulerClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd, a := flag.Arg(0), flag.Args()[1:]
	switch {
	case cmd == "submit" && len(a) >= 1:
		fs := flag.NewFlagSet("submit", flag.ExitOnError)
		key := fs.String("key", "", "idempotency key: resubmitting with the same key returns the same id")
		fs.Parse(a[1:])
		var id uint64
		id, err = submit(ctx, c, []byte(a[0]), *key)
		if err == nil {
			fmt.Println(id)
		}
	case cmd == "status" && len(a) == 1:
		var job *schedv1.Job
		job, err = getStatus(ctx, c, parseID(a[0]))
		if err == nil {
			printJob(job)
		}
	case cmd == "stats" && len(a) == 0:
		var s *schedv1.StatsResponse
		s, err = getStats(ctx, c)
		if err == nil {
			fmt.Printf("head=%d tail=%d depth=%d max_queue=%d\n", s.GetHead(), s.GetTail(), s.GetTail()-s.GetHead(), s.GetMaxQueue())
		}
	case cmd == "watch" && len(a) == 1:
		err = watch(ctx, c, parseID(a[0]))
	case cmd == "bench":
		err = bench(ctx, c, a)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

// do runs fn, retrying transient failures with backoff until ctx expires.
func do(ctx context.Context, fn func(ctx context.Context) error) error {
	backoff := backoffMin
	for {
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := fn(actx)
		cancel()
		if err == nil {
			return nil
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
		default:
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// submit enqueues one job, sleeping out any ResourceExhausted retry_after
// hints. It returns the job id and, via the second value, how many times
// admission was refused (bench reports this).
func submitCounting(ctx context.Context, c schedv1.SchedulerClient, payload []byte, key string) (id uint64, refused int, err error) {
	req := &schedv1.SubmitRequest{Payload: payload, IdempotencyKey: key}
	for {
		var resp *schedv1.SubmitResponse
		err = do(ctx, func(ctx context.Context) error {
			var err error
			resp, err = c.Submit(ctx, req)
			return err
		})
		if err == nil {
			return resp.GetId(), refused, nil
		}
		st := status.Convert(err)
		if st.Code() != codes.ResourceExhausted {
			return 0, refused, err
		}
		refused++
		wait := 50 * time.Millisecond
		if m := retryAfter.FindStringSubmatch(st.Message()); m != nil {
			if ms, perr := strconv.Atoi(m[1]); perr == nil && ms > 0 {
				wait = time.Duration(ms) * time.Millisecond
			}
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return 0, refused, err
		}
	}
}

func submit(ctx context.Context, c schedv1.SchedulerClient, payload []byte, key string) (uint64, error) {
	id, _, err := submitCounting(ctx, c, payload, key)
	return id, err
}

func getStatus(ctx context.Context, c schedv1.SchedulerClient, id uint64) (*schedv1.Job, error) {
	var resp *schedv1.StatusResponse
	err := do(ctx, func(ctx context.Context) error {
		var err error
		resp, err = c.Status(ctx, &schedv1.StatusRequest{Id: id})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

func getStats(ctx context.Context, c schedv1.SchedulerClient) (*schedv1.StatsResponse, error) {
	var resp *schedv1.StatsResponse
	err := do(ctx, func(ctx context.Context) error {
		var err error
		resp, err = c.Stats(ctx, &schedv1.StatsRequest{})
		return err
	})
	return resp, err
}

func printJob(j *schedv1.Job) {
	fmt.Printf("id=%d state=%s gen=%d worker=%q attempts=%d lease_until=%s\n",
		j.GetId(), stateName(j.GetState()), j.GetGen(), j.GetWorker(), j.GetAttempts(), fmtMs(j.GetLeaseUntilMs()))
	if j.GetSubmittedMs() != 0 {
		fmt.Printf("submitted=%s\n", fmtMs(j.GetSubmittedMs()))
	}
	if j.GetLastError() != "" {
		fmt.Printf("last_error=%q\n", j.GetLastError())
	}
	if len(j.GetResult()) > 0 {
		fmt.Printf("result=%s\n", j.GetResult())
	}
}

// watch polls Status every 200ms and prints a line each time the job's
// (state, gen, worker, attempts) tuple changes, until the job is terminal.
func watch(ctx context.Context, c schedv1.SchedulerClient, id uint64) error {
	type snap struct {
		state    schedv1.State
		gen      uint64
		worker   string
		attempts uint32
	}
	var last *snap
	for {
		j, err := getStatus(ctx, c, id)
		if err != nil {
			return err
		}
		cur := snap{j.GetState(), j.GetGen(), j.GetWorker(), j.GetAttempts()}
		if last == nil || *last != cur {
			fmt.Printf("%s  state=%s gen=%d worker=%q attempts=%d\n",
				time.Now().Format("15:04:05.000"), stateName(cur.state), cur.gen, cur.worker, cur.attempts)
			last = &cur
		}
		if isTerminal(cur.state) {
			if j.GetLastError() != "" {
				fmt.Printf("last_error=%q\n", j.GetLastError())
			}
			if len(j.GetResult()) > 0 {
				fmt.Printf("result=%s\n", j.GetResult())
			}
			return nil
		}
		select {
		case <-time.After(watchInterval):
		case <-ctx.Done():
			return errors.New("watch: timed out before the job became terminal")
		}
	}
}

// bench submits n jobs as fast as admission allows (sleeping out every
// ResourceExhausted retry_after_ms), then polls Stats until head == tail
// and reports submit throughput and time-to-drain. Payloads are tiny JSON
// the demo worker understands.
func bench(ctx context.Context, c schedv1.SchedulerClient, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 1000, "number of jobs to submit")
	fs.Parse(args)
	if *n <= 0 {
		return errors.New("bench: -n must be positive")
	}

	start := time.Now()
	refusedTotal := 0
	for i := 0; i < *n; i++ {
		payload := fmt.Appendf(nil, `{"sleep_ms":1,"echo":"bench-%d"}`, i)
		_, refused, err := submitCounting(ctx, c, payload, "")
		if err != nil {
			return fmt.Errorf("submit %d/%d: %w", i, *n, err)
		}
		refusedTotal += refused
	}
	submitted := time.Since(start)
	fmt.Printf("submitted %d jobs in %v (%.0f submits/s, %d refused-and-retried)\n",
		*n, submitted.Round(time.Millisecond), float64(*n)/submitted.Seconds(), refusedTotal)

	for {
		s, err := getStats(ctx, c)
		if err != nil {
			return err
		}
		if s.GetHead() >= s.GetTail() {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("bench: queue did not drain within timeout (head=%d tail=%d)", s.GetHead(), s.GetTail())
		}
	}
	drained := time.Since(start)
	fmt.Printf("drained in %v total (%v after last submit, %.0f jobs/s end-to-end)\n",
		drained.Round(time.Millisecond), (drained - submitted).Round(time.Millisecond), float64(*n)/drained.Seconds())
	return nil
}

func parseID(s string) uint64 {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		fatal(fmt.Errorf("bad job id %q", s))
	}
	return id
}

func stateName(s schedv1.State) string {
	switch s {
	case schedv1.State_STATE_PENDING:
		return "PENDING"
	case schedv1.State_STATE_RUNNING:
		return "RUNNING"
	case schedv1.State_STATE_DONE:
		return "DONE"
	case schedv1.State_STATE_FAILED:
		return "FAILED"
	case schedv1.State_STATE_UNSPECIFIED:
		// An idempotent Submit writes the record before binding its key
		// and publishes it afterwards (sched/queue.go); until then it is
		// staged and can never be claimed.
		return "STAGED"
	}
	return s.String()
}

func isTerminal(s schedv1.State) bool {
	return s == schedv1.State_STATE_DONE || s == schedv1.State_STATE_FAILED
}

func fmtMs(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).Format("15:04:05.000")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "schedctl:", status.Convert(err).Message())
	os.Exit(1)
}
