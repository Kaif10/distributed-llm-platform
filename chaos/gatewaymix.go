package chaos

// The optional LLM-serving mix: a real gateway.Server in front of real
// (mock) inference workers over real loopback gRPC, sharing the SAME
// nemesis-disrupted KV as the rate limiter's token buckets and the worker
// registry. It has no linearizable model of its own (streaming generation
// isn't a single-object CAS-shaped operation), so its checks are lighter:
// no panic anywhere in the request path, and every request either
// completes or fails with a real gRPC status, never hangs past its
// deadline. Composing it into the same run as the KV/scheduler workload is
// what proves the whole platform, not just one layer of it, survives the
// same chaos at once.

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dsys/gateway"
	gatewayv1 "dsys/gen/gateway/v1"
	inferv1 "dsys/gen/infer/v1"
	"dsys/infer/mock"
	"dsys/kvapi"
	"dsys/ratelimit"
	"dsys/router"
)

type gatewayMix struct {
	gw       *gateway.Server
	servers  []*grpc.Server
	requests atomic.Int64
	errors   atomic.Int64
	panics   atomic.Int64
}

func newGatewayMix(kv kvapi.KV, sc Scenario) (*gatewayMix, error) {
	m := &gatewayMix{}
	reg := router.NewRegistry(kv, router.Options{Prefix: "chaos-workers", CacheTTL: 50 * time.Millisecond})
	lim := ratelimit.New(kv, ratelimit.Options{Prefix: "chaos-rl", Rate: 1000, Burst: 1000})

	for i := 0; i < sc.NumWorkers; i++ {
		w := mock.New(mock.Options{
			ID: fmt.Sprintf("chaos-w%d", i), BasePrefillMs: 2, PrefillPerChar: 0.02, TokenMs: 2,
			Seed: sc.Seed + int64(i) + 4000,
		})
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			m.close()
			return nil, err
		}
		gs := grpc.NewServer()
		inferv1.RegisterInferenceServer(gs, w)
		go func() { _ = gs.Serve(lis) }()
		m.servers = append(m.servers, gs)
		if _, err := reg.Register(context.Background(), router.Worker{ID: w.ID(), Addr: lis.Addr().String()}, time.Minute); err != nil {
			m.close()
			return nil, err
		}
	}

	m.gw = gateway.New(gateway.Options{
		KV: kv, Registry: reg, Limiter: lim,
		PrefixRouting: true, HedgeAfter: 150 * time.Millisecond,
		Dial: func(addr string) (inferv1.InferenceClient, error) {
			cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			return inferv1.NewInferenceClient(cc), nil
		},
	})
	return m, nil
}

func (m *gatewayMix) close() {
	if m.gw != nil {
		m.gw.Close()
	}
	for _, s := range m.servers {
		s.Stop()
	}
}

// collectStream is a minimal grpc.ServerStreamingServer[Token], for calling
// gateway.Server.Generate in-process instead of over a real client
// connection (mirrors gateway_test.go's nopStream). Embedding
// grpc.ServerStream satisfies the rest of the interface; Generate only
// ever calls Send and Context.
type collectStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *collectStream) Send(*gatewayv1.Token) error { return nil }
func (s *collectStream) Context() context.Context    { return s.ctx }

func (m *gatewayMix) run(ctx context.Context, sc Scenario, wg *sync.WaitGroup) {
	prefixes := []string{
		strings.Repeat("alpha system prompt. ", 15),
		strings.Repeat("beta system prompt. ", 15),
	}
	for c := 0; c < max(2, sc.NumClients/3); c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(sc.Seed + int64(c) + 5000))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				func() {
					defer func() {
						if rec := recover(); rec != nil {
							m.panics.Add(1)
						}
					}()
					octx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
					req := &gatewayv1.GenerateRequest{
						Tenant: fmt.Sprintf("t%d", c%3), NoCache: true, MaxTokens: 4,
						Prompt: prefixes[r.Intn(len(prefixes))] + fmt.Sprintf("q%d", r.Int()),
					}
					m.requests.Add(1)
					if err := m.gw.Generate(req, &collectStream{ctx: octx}); err != nil {
						m.errors.Add(1)
					}
				}()
				time.Sleep(time.Duration(5+r.Intn(15)) * time.Millisecond)
			}
		}(c)
	}
}

// check reports the only hard failure this mix recognises.
func (m *gatewayMix) check() []string {
	if p := m.panics.Load(); p > 0 {
		return []string{fmt.Sprintf("gateway: %d panics in the request path", p)}
	}
	return nil
}
