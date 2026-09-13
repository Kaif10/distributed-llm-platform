from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GenerateRequest(_message.Message):
    __slots__ = ("tenant", "prompt", "max_tokens", "no_cache")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKENS_FIELD_NUMBER: _ClassVar[int]
    NO_CACHE_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    prompt: str
    max_tokens: int
    no_cache: bool
    def __init__(self, tenant: _Optional[str] = ..., prompt: _Optional[str] = ..., max_tokens: _Optional[int] = ..., no_cache: _Optional[bool] = ...) -> None: ...

class Token(_message.Message):
    __slots__ = ("text", "index", "done", "finish_reason", "cached", "worker", "hedged", "hedge_won", "ttft_ms", "prefix_cache_hit", "prefill_ms")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    CACHED_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    HEDGED_FIELD_NUMBER: _ClassVar[int]
    HEDGE_WON_FIELD_NUMBER: _ClassVar[int]
    TTFT_MS_FIELD_NUMBER: _ClassVar[int]
    PREFIX_CACHE_HIT_FIELD_NUMBER: _ClassVar[int]
    PREFILL_MS_FIELD_NUMBER: _ClassVar[int]
    text: str
    index: int
    done: bool
    finish_reason: str
    cached: bool
    worker: str
    hedged: bool
    hedge_won: bool
    ttft_ms: int
    prefix_cache_hit: bool
    prefill_ms: int
    def __init__(self, text: _Optional[str] = ..., index: _Optional[int] = ..., done: _Optional[bool] = ..., finish_reason: _Optional[str] = ..., cached: _Optional[bool] = ..., worker: _Optional[str] = ..., hedged: _Optional[bool] = ..., hedge_won: _Optional[bool] = ..., ttft_ms: _Optional[int] = ..., prefix_cache_hit: _Optional[bool] = ..., prefill_ms: _Optional[int] = ...) -> None: ...

class RegisterWorkerRequest(_message.Message):
    __slots__ = ("worker_id", "addr", "inflight", "lease_ms", "model")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ADDR_FIELD_NUMBER: _ClassVar[int]
    INFLIGHT_FIELD_NUMBER: _ClassVar[int]
    LEASE_MS_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    addr: str
    inflight: int
    lease_ms: int
    model: str
    def __init__(self, worker_id: _Optional[str] = ..., addr: _Optional[str] = ..., inflight: _Optional[int] = ..., lease_ms: _Optional[int] = ..., model: _Optional[str] = ...) -> None: ...

class RegisterWorkerResponse(_message.Message):
    __slots__ = ("lease_until_ms",)
    LEASE_UNTIL_MS_FIELD_NUMBER: _ClassVar[int]
    lease_until_ms: int
    def __init__(self, lease_until_ms: _Optional[int] = ...) -> None: ...

class StatsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatsResponse(_message.Message):
    __slots__ = ("requests", "rate_limited", "cache_hits", "hedges_launched", "hedges_won", "cancelled", "worker_errors", "routed_to", "live_workers")
    class RoutedToEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMITED_FIELD_NUMBER: _ClassVar[int]
    CACHE_HITS_FIELD_NUMBER: _ClassVar[int]
    HEDGES_LAUNCHED_FIELD_NUMBER: _ClassVar[int]
    HEDGES_WON_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    WORKER_ERRORS_FIELD_NUMBER: _ClassVar[int]
    ROUTED_TO_FIELD_NUMBER: _ClassVar[int]
    LIVE_WORKERS_FIELD_NUMBER: _ClassVar[int]
    requests: int
    rate_limited: int
    cache_hits: int
    hedges_launched: int
    hedges_won: int
    cancelled: int
    worker_errors: int
    routed_to: _containers.ScalarMap[str, int]
    live_workers: int
    def __init__(self, requests: _Optional[int] = ..., rate_limited: _Optional[int] = ..., cache_hits: _Optional[int] = ..., hedges_launched: _Optional[int] = ..., hedges_won: _Optional[int] = ..., cancelled: _Optional[int] = ..., worker_errors: _Optional[int] = ..., routed_to: _Optional[_Mapping[str, int]] = ..., live_workers: _Optional[int] = ...) -> None: ...
