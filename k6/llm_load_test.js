// k6 load test for the gateway's Generate RPC (server-streaming gRPC).
//
// Uses k6's stable `k6/net/grpc` module (verified against k6 v2.2.0, the
// version shipped in the grafana/k6 Docker image at the time this was
// written): grpc.Client + grpc.Stream supports server-streaming RPCs, so
// this drives the REAL request path end to end (rate limit -> cache ->
// route -> hedge -> stream), not a fallback unary RPC like Stats.
//
// Traffic pattern mirrors scripts/e2e_llm.sh: a handful of tenants, a small
// set of prompt PREFIXES repeated across requests (so prefix-cache routing
// and hedging have something to show), ramping VU count up then down.
//
// Run (from the repo root, gateway already listening on GATEWAY_ADDR):
//
//   docker run --rm -i --network host \
//     -e GATEWAY_ADDR=127.0.0.1:7850 \
//     -v "$(pwd)/k6:/scripts" -v "$(pwd)/proto:/proto" \
//     grafana/k6 run /scripts/llm_load_test.js
//
// (On Docker Desktop for Windows/Mac, --network host is not supported;
// use -e GATEWAY_ADDR=host.docker.internal:7850 instead and drop
// --network host.)
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const GATEWAY_ADDR = __ENV.GATEWAY_ADDR || '127.0.0.1:7850';

const client = new grpc.Client();
client.load(['/proto/gateway/v1'], 'gateway.proto');

// NOTE on ttft_ms: k6's grpc Stream delivers 'data'/'end' callbacks
// asynchronously, and in testing they only get flushed to JS when the VU
// yields (e.g. during sleep()) rather than the instant the frame arrives on
// the wire — so Date.now() inside the 'data' handler measures "time until
// k6's event loop got around to it", not true network TTFT (it is biased
// toward whatever the trailing sleep() is). It is kept as a rough,
// best-effort signal; grpc_req_duration (k6's own gRPC metric, unaffected by
// this) is the trustworthy latency number and is what the threshold below is
// really checking.
const ttft = new Trend('ttft_ms', true);
const streamErrors = new Counter('stream_errors');

// A handful of ~600-char "system prompt" style prefixes, the same idea
// e2e_llm.sh uses to give prefix-cache routing something real to do: a
// worker that has seen this exact prefix recently should be faster than a
// cold one.
const PREFIXES = Array.from({ length: 6 }, (_, i) =>
  `You are assistant #${i}. Follow these instructions carefully and answer the user's question in a concise, helpful way. `.repeat(6)
);
const TENANTS = ['tenant-a', 'tenant-b', 'tenant-c'];

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 20 },
        { duration: '30s', target: 20 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    // A generous bound: this is a mock backend on a laptop, not an SLO.
    'ttft_ms': ['p(95)<3000'],
    'grpc_req_duration': ['p(95)<5000'],
    'checks': ['rate>0.95'],
  },
};

export default function () {
  client.connect(GATEWAY_ADDR, { plaintext: true });

  const tenant = TENANTS[Math.floor(Math.random() * TENANTS.length)];
  const prefix = PREFIXES[Math.floor(Math.random() * PREFIXES.length)];
  const prompt = prefix + ' Request id ' + __VU + '-' + __ITER;

  const start = Date.now();
  let gotFirst = false;
  let gotDone = false;
  let sawError = false;

  const stream = new grpc.Stream(client, 'gateway.v1.Gateway/Generate');
  stream.on('data', (token) => {
    if (!gotFirst) {
      gotFirst = true;
      ttft.add(Date.now() - start);
    }
    if (token.done) {
      gotDone = true;
    }
  });
  stream.on('error', (e) => {
    sawError = true;
    streamErrors.add(1);
  });
  stream.on('end', () => {
    // Stream events (data/end) are delivered asynchronously by k6's grpc
    // module; checking here, once 'end' has actually fired, is the only way
    // to see gotFirst/gotDone/sawError in their final state (checking right
    // after stream.end() below would race the delivery and always read the
    // pre-stream defaults).
    check(null, {
      'stream completed without error': () => !sawError,
      'got at least one token': () => gotFirst,
      'stream reached done': () => gotDone,
    });
    client.close();
  });

  stream.write({
    tenant: tenant,
    prompt: prompt,
    max_tokens: 24,
    no_cache: false,
  });
  stream.end();

  sleep(1); // give the async stream time to finish before the VU iterates again
}
