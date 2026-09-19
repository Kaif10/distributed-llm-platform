package obs

// Metrics is a small typed Prometheus registry: it exists so main.go call
// sites don't hand-roll registration boilerplate, and so the metric names
// below are defined in exactly one place.
//
// # Final metric names (stable — the Grafana dashboard and k6 checks depend
// on these; do not rename without updating docker/observability/grafana and
// k6/*.js)
//
// From NewMetrics("gateway"), scraped off the gateway's -metrics-addr:
//
//	gateway_requests_total{tenant,cached,hedged}   counter — one per Generate call
//	gateway_rate_limited_total{tenant}             counter — Take() rejections
//	gateway_ttft_ms                                histogram — time to first token, all requests (cached and live)
//	gateway_cache_hit_ratio                        gauge — running cache_hits/requests
//	gateway_live_workers                           gauge — len(Registry.Live()) as of the last request
//	gateway_hedges_launched_total                  counter — hedge timer fired and a 2nd attempt started
//	gateway_hedges_won_total                       counter — a hedge (not the primary) produced the first token
//	gateway_routed_total{worker}                   counter — attempts started per worker id (primary + hedges)
//
// From NewMetrics("sched"), scraped off the sched's -metrics-addr:
//
//	sched_jobs_submitted_total   counter — successful Submit calls
//	sched_jobs_completed_total   counter — successful Complete calls
//	sched_jobs_failed_total      counter — Fail calls that terminated the job (attempts exhausted)
//	sched_jobs_fenced_total      counter — Heartbeat/Complete/Fail calls rejected as fenced/terminal (ErrFenced, ErrTerminal)
//	sched_queue_depth            gauge — tail-head, updated by a background ticker
//
// Both processes also get the standard promhttp process/go collectors for
// free (promauto registers against a fresh, non-default registry, so
// nothing here fights the default global registry other packages might use).
import (
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors for one process (gateway or
// sched). Fields for a service other than the one NewMetrics was built for
// are left nil; every Inc/Observe/Set method below nil-checks before use, so
// it is always safe to call all of them (a gateway calling a sched-only
// method is simply a no-op) — the same "optional, nil-checked" shape as
// gateway.Options.Cache/Limiter.
type Metrics struct {
	reg *prometheus.Registry

	// gateway
	requestsTotal    *prometheus.CounterVec
	rateLimitedTotal *prometheus.CounterVec
	ttftMs           prometheus.Histogram
	cacheHitRatio    prometheus.Gauge
	liveWorkers      prometheus.Gauge
	hedgesLaunched   prometheus.Counter
	hedgesWon        prometheus.Counter
	routedTotal      *prometheus.CounterVec
	reqCount         atomic.Uint64 // denominator for cacheHitRatio
	cacheHitCount    atomic.Uint64 // numerator for cacheHitRatio

	// sched
	jobsSubmittedTotal prometheus.Counter
	jobsCompletedTotal prometheus.Counter
	jobsFailedTotal    prometheus.Counter
	jobsFencedTotal    prometheus.Counter
	queueDepth         prometheus.Gauge
}

// NewMetrics builds and registers the metrics for namespace ("gateway" or
// "sched"); the namespace prefixes every metric name (see the doc comment
// above for the exact resulting names).
func NewMetrics(namespace string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())

	m := &Metrics{reg: reg}
	f := promauto.With(reg)

	switch namespace {
	case "sched":
		m.jobsSubmittedTotal = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_submitted_total", Help: "Submit calls that returned a job id.",
		})
		m.jobsCompletedTotal = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_completed_total", Help: "Complete calls that marked a job DONE.",
		})
		m.jobsFailedTotal = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_failed_total", Help: "Fail calls that terminated a job (attempts exhausted).",
		})
		m.jobsFencedTotal = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_fenced_total", Help: "Heartbeat/Complete/Fail calls rejected as fenced or terminal.",
		})
		m.queueDepth = f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "queue_depth", Help: "tail - head, updated periodically.",
		})
	default: // "gateway", or anything else: default to the gateway metric set
		m.requestsTotal = f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "requests_total", Help: "Generate calls, by tenant/cached/hedged.",
		}, []string{"tenant", "cached", "hedged"})
		m.rateLimitedTotal = f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "rate_limited_total", Help: "Generate calls rejected by the rate limiter, by tenant.",
		}, []string{"tenant"})
		m.ttftMs = f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "ttft_ms", Help: "Time to first token, milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 150, 250, 400, 600, 1000, 1500, 2500, 5000, 10000},
		})
		m.cacheHitRatio = f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "cache_hit_ratio", Help: "Running cache_hits / requests.",
		})
		m.liveWorkers = f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "live_workers", Help: "Live inference workers as of the last request.",
		})
		m.hedgesLaunched = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "hedges_launched_total", Help: "Hedge timer fired and a 2nd attempt started.",
		})
		m.hedgesWon = f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "hedges_won_total", Help: "A hedge (not the primary) produced the first token.",
		})
		m.routedTotal = f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "routed_total", Help: "Attempts started per worker id (primary + hedges).",
		}, []string{"worker"})
	}
	return m
}

// Handler returns the promhttp handler to serve on "/metrics".
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ---------------------------------------------------------------------------
// gateway
// ---------------------------------------------------------------------------

// ObserveRequest records one Generate call. cached/hedged are folded into
// gateway_requests_total's labels and, for cached, into the running
// gateway_cache_hit_ratio.
func (m *Metrics) ObserveRequest(tenant string, cached, hedged bool) {
	if m == nil || m.requestsTotal == nil {
		return
	}
	m.requestsTotal.WithLabelValues(tenant, strconv.FormatBool(cached), strconv.FormatBool(hedged)).Inc()
	total := m.reqCount.Add(1)
	var hits uint64
	if cached {
		hits = m.cacheHitCount.Add(1)
	} else {
		hits = m.cacheHitCount.Load()
	}
	if total > 0 {
		m.cacheHitRatio.Set(float64(hits) / float64(total))
	}
}

// IncRateLimited records one rate-limit rejection for tenant.
func (m *Metrics) IncRateLimited(tenant string) {
	if m == nil || m.rateLimitedTotal == nil {
		return
	}
	m.rateLimitedTotal.WithLabelValues(tenant).Inc()
}

// ObserveTTFT records a time-to-first-token sample, in milliseconds.
func (m *Metrics) ObserveTTFT(ms float64) {
	if m == nil || m.ttftMs == nil {
		return
	}
	m.ttftMs.Observe(ms)
}

// SetLiveWorkers records the current live-worker count.
func (m *Metrics) SetLiveWorkers(n int) {
	if m == nil || m.liveWorkers == nil {
		return
	}
	m.liveWorkers.Set(float64(n))
}

// IncHedgeLaunched records a hedge timer firing and starting a 2nd attempt.
func (m *Metrics) IncHedgeLaunched() {
	if m == nil || m.hedgesLaunched == nil {
		return
	}
	m.hedgesLaunched.Inc()
}

// IncHedgeWon records a hedge (not the primary attempt) winning the race.
func (m *Metrics) IncHedgeWon() {
	if m == nil || m.hedgesWon == nil {
		return
	}
	m.hedgesWon.Inc()
}

// IncRouted records one attempt (primary or hedge) started against worker.
func (m *Metrics) IncRouted(worker string) {
	if m == nil || m.routedTotal == nil {
		return
	}
	m.routedTotal.WithLabelValues(worker).Inc()
}

// ---------------------------------------------------------------------------
// sched
// ---------------------------------------------------------------------------

func (m *Metrics) IncJobsSubmitted() {
	if m == nil || m.jobsSubmittedTotal == nil {
		return
	}
	m.jobsSubmittedTotal.Inc()
}

func (m *Metrics) IncJobsCompleted() {
	if m == nil || m.jobsCompletedTotal == nil {
		return
	}
	m.jobsCompletedTotal.Inc()
}

func (m *Metrics) IncJobsFailed() {
	if m == nil || m.jobsFailedTotal == nil {
		return
	}
	m.jobsFailedTotal.Inc()
}

func (m *Metrics) IncJobsFenced() {
	if m == nil || m.jobsFencedTotal == nil {
		return
	}
	m.jobsFencedTotal.Inc()
}

// SetQueueDepth records tail-head.
func (m *Metrics) SetQueueDepth(depth int64) {
	if m == nil || m.queueDepth == nil {
		return
	}
	m.queueDepth.Set(float64(depth))
}
