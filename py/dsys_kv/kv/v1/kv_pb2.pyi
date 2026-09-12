from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Op(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OP_UNSPECIFIED: _ClassVar[Op]
    OP_PUT: _ClassVar[Op]
    OP_DELETE: _ClassVar[Op]
    OP_CAS: _ClassVar[Op]
    OP_GET: _ClassVar[Op]
OP_UNSPECIFIED: Op
OP_PUT: Op
OP_DELETE: Op
OP_CAS: Op
OP_GET: Op

class RequestMeta(_message.Message):
    __slots__ = ("client_id", "request_id")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    request_id: int
    def __init__(self, client_id: _Optional[str] = ..., request_id: _Optional[int] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("meta", "key", "value")
    META_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    meta: RequestMeta
    key: str
    value: bytes
    def __init__(self, meta: _Optional[_Union[RequestMeta, _Mapping]] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("value", "found")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    found: bool
    def __init__(self, value: _Optional[bytes] = ..., found: _Optional[bool] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("meta", "key")
    META_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    meta: RequestMeta
    key: str
    def __init__(self, meta: _Optional[_Union[RequestMeta, _Mapping]] = ..., key: _Optional[str] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("existed",)
    EXISTED_FIELD_NUMBER: _ClassVar[int]
    existed: bool
    def __init__(self, existed: _Optional[bool] = ...) -> None: ...

class CASRequest(_message.Message):
    __slots__ = ("meta", "key", "expected", "expect_absent", "value")
    META_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    EXPECT_ABSENT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    meta: RequestMeta
    key: str
    expected: bytes
    expect_absent: bool
    value: bytes
    def __init__(self, meta: _Optional[_Union[RequestMeta, _Mapping]] = ..., key: _Optional[str] = ..., expected: _Optional[bytes] = ..., expect_absent: _Optional[bool] = ..., value: _Optional[bytes] = ...) -> None: ...

class CASResponse(_message.Message):
    __slots__ = ("swapped", "current")
    SWAPPED_FIELD_NUMBER: _ClassVar[int]
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    swapped: bool
    current: bytes
    def __init__(self, swapped: _Optional[bool] = ..., current: _Optional[bytes] = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("op", "key", "value", "expected", "expect_absent", "meta")
    OP_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    EXPECT_ABSENT_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    op: Op
    key: str
    value: bytes
    expected: bytes
    expect_absent: bool
    meta: RequestMeta
    def __init__(self, op: _Optional[_Union[Op, str]] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ..., expected: _Optional[bytes] = ..., expect_absent: _Optional[bool] = ..., meta: _Optional[_Union[RequestMeta, _Mapping]] = ...) -> None: ...
