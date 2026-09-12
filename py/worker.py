"""Pull-model worker for the sched.v1 gRPC service (proto/sched/v1/sched.proto).

Same contract as the Go library in sched/worker, read that package doc and
sched/api.go first. The short version:

  * Claim gives us a job under a LEASE and a fencing token GEN. A background
    thread renews the lease every lease/3 with that same gen.
  * If Heartbeat answers FAILED_PRECONDITION the job has been reassigned. We
    set the `fenced` Event the handler was given (a cooperative cancel; Python
    threads cannot be killed) and discard whatever the handler returns.
  * On success we call Complete(id, gen, result). FAILED_PRECONDITION there is
    the zombie case: the work is done but the lease lapsed and somebody else
    owns the job. Log it, count it, move on. Never retry, never re-claim.
  * On a handler exception we call Fail(id, gen, str(exc)); same fenced rule.
  * UNAVAILABLE / DEADLINE_EXCEEDED are retried with backoff and the same gen.
  * Handlers may run more than once per job (at-least-once execution,
    exactly-once commit). Make side effects idempotent on job.id.

Run the demo against a scheduler:
    .venv/Scripts/python.exe py/worker.py [host:port]
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sys
import threading
import time
from typing import Callable

import grpc

# The generated sched_pb2_grpc.py does `from sched.v1 import sched_pb2`, rooted
# at the `protoc -I proto` directory, so py/dsys_sched goes on sys.path (same
# trick as client.py uses for dsys_kv).
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.join(_HERE, "dsys_sched")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dsys_sched.sched.v1 import sched_pb2, sched_pb2_grpc  # noqa: E402

log = logging.getLogger("sched.worker")

# handler(job, fenced) -> result bytes. `fenced` is set when the worker loses
# the job; a handler that checks it between steps wastes the least work.
Handler = Callable[[sched_pb2.Job, threading.Event], bytes]

_TRANSIENT = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


class SchedWorker:
    def __init__(self, addr: str, handler: Handler, name: str | None = None, lease_ms: int = 10_000) -> None:
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.lease_ms = lease_ms
        self.handler = handler
        self._channel = grpc.insecure_channel(addr)
        self._stub = sched_pb2_grpc.SchedulerStub(self._channel)
        self.stats = {"claimed": 0, "completed": 0, "failed": 0, "fenced": 0}

    # -- RPC plumbing --------------------------------------------------------

    def _call(self, fn, *args, deadline: float | None = None):
        """Run fn(*args), retrying transient codes with backoff. The request is
        built once by the caller, so every retry presents the same (id, gen)."""
        backoff = 0.02
        while True:
            try:
                return fn(*args, timeout=2.0)
            except grpc.RpcError as e:
                if e.code() not in _TRANSIENT or (deadline and time.monotonic() > deadline):
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 0.5)

    @staticmethod
    def _is_fenced(e: grpc.RpcError) -> bool:
        return e.code() == grpc.StatusCode.FAILED_PRECONDITION

    # -- lifecycle -----------------------------------------------------------

    def run(self, stop: threading.Event | None = None) -> None:
        """Claim and run jobs until `stop` is set. Backs off 200ms..2s while idle."""
        stop = stop or threading.Event()
        backoff = 0.2
        while not stop.is_set():
            try:
                resp = self._call(self._stub.Claim, sched_pb2.ClaimRequest(worker=self.name, lease_ms=self.lease_ms))
            except grpc.RpcError as e:
                log.error("claim failed: %s: %s", e.code().name, e.details())
                resp = None
            if resp is None or not resp.found:
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
                continue
            backoff = 0.2
            self.stats["claimed"] += 1
            self._run_one(resp.job)
        self._channel.close()

    def _run_one(self, job: sched_pb2.Job) -> None:
        jid, gen = job.id, job.gen
        fenced = threading.Event()   # set when a heartbeat learns we lost the job
        done = threading.Event()     # set when the handler has returned

        def heartbeat() -> None:
            interval = self.lease_ms / 3 / 1000.0
            req = sched_pb2.HeartbeatRequest(id=jid, gen=gen, lease_ms=self.lease_ms)
            while not done.wait(interval):
                try:
                    self._call(self._stub.Heartbeat, req)
                except grpc.RpcError as e:
                    if self._is_fenced(e):
                        # Rule 2: the job is someone else's now. Stop the handler.
                        log.warning("job %d gen %d fenced by heartbeat: %s", jid, gen, e.details())
                        fenced.set()
                        return
                    log.error("heartbeat job %d: %s: %s", jid, e.code().name, e.details())

        hb = threading.Thread(target=heartbeat, name=f"hb-{jid}", daemon=True)
        hb.start()
        try:
            result, err = self.handler(job, fenced), None
        except Exception as exc:  # noqa: BLE001 - one bad job must not kill the worker
            log.exception("handler raised on job %d", jid)
            result, err = b"", exc
        finally:
            done.set()
            hb.join()

        if fenced.is_set():
            self.stats["fenced"] += 1
            log.info("job %d gen %d: discarding handler outcome, fenced mid-run", jid, gen)
            return

        deadline = time.monotonic() + 10.0
        try:
            if err is None:
                self._call(self._stub.Complete, sched_pb2.CompleteRequest(id=jid, gen=gen, result=result), deadline=deadline)
                self.stats["completed"] += 1
            else:
                self._call(self._stub.Fail, sched_pb2.FailRequest(id=jid, gen=gen, error=str(err)), deadline=deadline)
                self.stats["failed"] += 1
        except grpc.RpcError as e:
            if self._is_fenced(e):
                # Rule 3/4: the zombie case. Work done, write refused. Do NOT
                # retry, do NOT re-claim, do NOT crash. Log and move on.
                self.stats["fenced"] += 1
                log.warning("zombie: job %d gen %d %s refused, result discarded: %s",
                            jid, gen, "Complete" if err is None else "Fail", e.details())
            else:
                log.error("report job %d: %s: %s", jid, e.code().name, e.details())


# -- demo -------------------------------------------------------------------

def demo_handler(job: sched_pb2.Job, fenced: threading.Event) -> bytes:
    """Sleep `sleep_ms` from a JSON payload and echo `echo` back."""
    try:
        p = json.loads(job.payload)
    except ValueError:
        p = {}
    deadline = time.monotonic() + p.get("sleep_ms", 100) / 1000.0
    while time.monotonic() < deadline and not fenced.is_set():
        time.sleep(0.01)
    return json.dumps({"echo": p.get("echo", ""), "worker": "py"}).encode()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:7100"
    w = SchedWorker(target, demo_handler, lease_ms=int(os.environ.get("LEASE_MS", "10000")))
    log.info("worker %s pulling from %s", w.name, target)
    stop = threading.Event()
    try:
        w.run(stop)
    except KeyboardInterrupt:
        stop.set()
    finally:
        log.info("stopped: %s", w.stats)
