from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATE_UNSPECIFIED: _ClassVar[State]
    STATE_PENDING: _ClassVar[State]
    STATE_RUNNING: _ClassVar[State]
    STATE_DONE: _ClassVar[State]
    STATE_FAILED: _ClassVar[State]
STATE_UNSPECIFIED: State
STATE_PENDING: State
STATE_RUNNING: State
STATE_DONE: State
STATE_FAILED: State

class Job(_message.Message):
    __slots__ = ("id", "payload", "state", "gen", "worker", "lease_until_ms", "result", "attempts", "last_error", "submitted_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    GEN_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    LEASE_UNTIL_MS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_MS_FIELD_NUMBER: _ClassVar[int]
    id: int
    payload: bytes
    state: State
    gen: int
    worker: str
    lease_until_ms: int
    result: bytes
    attempts: int
    last_error: str
    submitted_ms: int
    def __init__(self, id: _Optional[int] = ..., payload: _Optional[bytes] = ..., state: _Optional[_Union[State, str]] = ..., gen: _Optional[int] = ..., worker: _Optional[str] = ..., lease_until_ms: _Optional[int] = ..., result: _Optional[bytes] = ..., attempts: _Optional[int] = ..., last_error: _Optional[str] = ..., submitted_ms: _Optional[int] = ...) -> None: ...

class SubmitRequest(_message.Message):
    __slots__ = ("payload", "idempotency_key")
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    idempotency_key: str
    def __init__(self, payload: _Optional[bytes] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SubmitResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class ClaimRequest(_message.Message):
    __slots__ = ("worker", "lease_ms")
    WORKER_FIELD_NUMBER: _ClassVar[int]
    LEASE_MS_FIELD_NUMBER: _ClassVar[int]
    worker: str
    lease_ms: int
    def __init__(self, worker: _Optional[str] = ..., lease_ms: _Optional[int] = ...) -> None: ...

class ClaimResponse(_message.Message):
    __slots__ = ("found", "job")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    JOB_FIELD_NUMBER: _ClassVar[int]
    found: bool
    job: Job
    def __init__(self, found: _Optional[bool] = ..., job: _Optional[_Union[Job, _Mapping]] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("id", "gen", "lease_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    GEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_MS_FIELD_NUMBER: _ClassVar[int]
    id: int
    gen: int
    lease_ms: int
    def __init__(self, id: _Optional[int] = ..., gen: _Optional[int] = ..., lease_ms: _Optional[int] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("lease_until_ms",)
    LEASE_UNTIL_MS_FIELD_NUMBER: _ClassVar[int]
    lease_until_ms: int
    def __init__(self, lease_until_ms: _Optional[int] = ...) -> None: ...

class CompleteRequest(_message.Message):
    __slots__ = ("id", "gen", "result")
    ID_FIELD_NUMBER: _ClassVar[int]
    GEN_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    id: int
    gen: int
    result: bytes
    def __init__(self, id: _Optional[int] = ..., gen: _Optional[int] = ..., result: _Optional[bytes] = ...) -> None: ...

class CompleteResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FailRequest(_message.Message):
    __slots__ = ("id", "gen", "error")
    ID_FIELD_NUMBER: _ClassVar[int]
    GEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    id: int
    gen: int
    error: str
    def __init__(self, id: _Optional[int] = ..., gen: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class FailResponse(_message.Message):
    __slots__ = ("requeued",)
    REQUEUED_FIELD_NUMBER: _ClassVar[int]
    requeued: bool
    def __init__(self, requeued: _Optional[bool] = ...) -> None: ...

class StatusRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("job",)
    JOB_FIELD_NUMBER: _ClassVar[int]
    job: Job
    def __init__(self, job: _Optional[_Union[Job, _Mapping]] = ...) -> None: ...

class StatsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatsResponse(_message.Message):
    __slots__ = ("head", "tail", "max_queue")
    HEAD_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    MAX_QUEUE_FIELD_NUMBER: _ClassVar[int]
    head: int
    tail: int
    max_queue: int
    def __init__(self, head: _Optional[int] = ..., tail: _Optional[int] = ..., max_queue: _Optional[int] = ...) -> None: ...
