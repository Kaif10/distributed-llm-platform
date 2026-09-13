from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GenerateRequest(_message.Message):
    __slots__ = ("request_id", "tenant", "prompt", "max_tokens")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKENS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    tenant: str
    prompt: str
    max_tokens: int
    def __init__(self, request_id: _Optional[str] = ..., tenant: _Optional[str] = ..., prompt: _Optional[str] = ..., max_tokens: _Optional[int] = ...) -> None: ...

class Token(_message.Message):
    __slots__ = ("text", "index", "done", "finish_reason", "prefill_ms", "prefix_cache_hit", "cached_prefix_chars")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    PREFILL_MS_FIELD_NUMBER: _ClassVar[int]
    PREFIX_CACHE_HIT_FIELD_NUMBER: _ClassVar[int]
    CACHED_PREFIX_CHARS_FIELD_NUMBER: _ClassVar[int]
    text: str
    index: int
    done: bool
    finish_reason: str
    prefill_ms: int
    prefix_cache_hit: bool
    cached_prefix_chars: int
    def __init__(self, text: _Optional[str] = ..., index: _Optional[int] = ..., done: _Optional[bool] = ..., finish_reason: _Optional[str] = ..., prefill_ms: _Optional[int] = ..., prefix_cache_hit: _Optional[bool] = ..., cached_prefix_chars: _Optional[int] = ...) -> None: ...

class HealthRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthResponse(_message.Message):
    __slots__ = ("worker_id", "inflight", "model")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    INFLIGHT_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    inflight: int
    model: str
    def __init__(self, worker_id: _Optional[str] = ..., inflight: _Optional[int] = ..., model: _Optional[str] = ...) -> None: ...
