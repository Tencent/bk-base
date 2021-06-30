# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor.FileDescriptor(
    name="direct_access.proto",
    package="",
    syntax="proto3",
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
    serialized_pb=b'\n\x13\x64irect_access.proto"\x7f\n\x07Request\x12\x0c\n\x04task\x18\x01 \x01(\x0c\x12\x17\n\x0f'
    b"pickle_protocol\x18\x02 \x01(\x05\x12\r\n\x05\x64\x65lay\x18\x03 \x01(\x08\x12\r\n\x05\x62lock"
    b"\x18\x04 \x01(\x08\x12\x0f\n\x07timeout\x18\x05 \x01(\x02\x12\r\n\x05\x61\x62ort\x18\x06 \x01"
    b'(\x08\x12\x0f\n\x07\x63hunked\x18\x07 \x01(\t"\x1b\n\x08Response\x12\x0f\n\x07\x63ontent'
    b'\x18\x01 \x01(\x0c"T\n\x0c\x43\x61\x63heContent\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value'
    b"\x18\x02 \x01(\x0c\x12\x17\n\x0fpickle_protocol\x18\x03 \x01(\x05\x12\x0f\n\x07\x63hunked"
    b"\x18\x04 \x01(\x08\x32\x8b\x03\n\x0c\x44irectAccess\x12\x1e\n\x05\x61pply\x12\x08.Request"
    b'\x1a\t.Response"\x00\x12#\n\nget_result\x12\x08.Request\x1a\t.Response"\x00\x12#\n\nget_status'
    b'\x12\x08.Request\x1a\t.Response"\x00\x12\x1f\n\x06\x63\x61ncel\x12\x08.Request\x1a\t.Response'
    b'"\x00\x12+\n\tset_cache\x12\r.CacheContent\x1a\r.CacheContent"\x00\x12+\n\tget_cache'
    b'\x12\r.CacheContent\x1a\r.CacheContent"\x00\x12.\n\x0c\x64\x65lete_cache\x12\r.CacheContent'
    b'\x1a\r.CacheContent"\x00\x12\x32\n\x10stream_set_cache\x12\r.CacheContent\x1a\r.CacheContent"'
    b'\x00\x12\x32\n\x10stream_get_cache\x12\r.CacheContent\x1a\r.CacheContent"\x00\x62\x06proto3',
)


_REQUEST = _descriptor.Descriptor(
    name="Request",
    full_name="Request",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    create_key=_descriptor._internal_create_key,
    fields=[
        _descriptor.FieldDescriptor(
            name="task",
            full_name="Request.task",
            index=0,
            number=1,
            type=12,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=b"",
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="pickle_protocol",
            full_name="Request.pickle_protocol",
            index=1,
            number=2,
            type=5,
            cpp_type=1,
            label=1,
            has_default_value=False,
            default_value=0,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="delay",
            full_name="Request.delay",
            index=2,
            number=3,
            type=8,
            cpp_type=7,
            label=1,
            has_default_value=False,
            default_value=False,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="block",
            full_name="Request.block",
            index=3,
            number=4,
            type=8,
            cpp_type=7,
            label=1,
            has_default_value=False,
            default_value=False,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="timeout",
            full_name="Request.timeout",
            index=4,
            number=5,
            type=2,
            cpp_type=6,
            label=1,
            has_default_value=False,
            default_value=float(0),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="abort",
            full_name="Request.abort",
            index=5,
            number=6,
            type=8,
            cpp_type=7,
            label=1,
            has_default_value=False,
            default_value=False,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="chunked",
            full_name="Request.chunked",
            index=6,
            number=7,
            type=9,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=b"".decode("utf-8"),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=23,
    serialized_end=150,
)


_RESPONSE = _descriptor.Descriptor(
    name="Response",
    full_name="Response",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    create_key=_descriptor._internal_create_key,
    fields=[
        _descriptor.FieldDescriptor(
            name="content",
            full_name="Response.content",
            index=0,
            number=1,
            type=12,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=b"",
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=152,
    serialized_end=179,
)


_CACHECONTENT = _descriptor.Descriptor(
    name="CacheContent",
    full_name="CacheContent",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    create_key=_descriptor._internal_create_key,
    fields=[
        _descriptor.FieldDescriptor(
            name="key",
            full_name="CacheContent.key",
            index=0,
            number=1,
            type=9,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=b"".decode("utf-8"),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="value",
            full_name="CacheContent.value",
            index=1,
            number=2,
            type=12,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=b"",
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="pickle_protocol",
            full_name="CacheContent.pickle_protocol",
            index=2,
            number=3,
            type=5,
            cpp_type=1,
            label=1,
            has_default_value=False,
            default_value=0,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="chunked",
            full_name="CacheContent.chunked",
            index=3,
            number=4,
            type=8,
            cpp_type=7,
            label=1,
            has_default_value=False,
            default_value=False,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=181,
    serialized_end=265,
)

DESCRIPTOR.message_types_by_name["Request"] = _REQUEST
DESCRIPTOR.message_types_by_name["Response"] = _RESPONSE
DESCRIPTOR.message_types_by_name["CacheContent"] = _CACHECONTENT
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Request = _reflection.GeneratedProtocolMessageType(
    "Request",
    (_message.Message,),
    {
        "DESCRIPTOR": _REQUEST,
        "__module__": "direct_access_pb2"
        # @@protoc_insertion_point(class_scope:Request)
    },
)
_sym_db.RegisterMessage(Request)

Response = _reflection.GeneratedProtocolMessageType(
    "Response",
    (_message.Message,),
    {
        "DESCRIPTOR": _RESPONSE,
        "__module__": "direct_access_pb2"
        # @@protoc_insertion_point(class_scope:Response)
    },
)
_sym_db.RegisterMessage(Response)

CacheContent = _reflection.GeneratedProtocolMessageType(
    "CacheContent",
    (_message.Message,),
    {
        "DESCRIPTOR": _CACHECONTENT,
        "__module__": "direct_access_pb2"
        # @@protoc_insertion_point(class_scope:CacheContent)
    },
)
_sym_db.RegisterMessage(CacheContent)


_DIRECTACCESS = _descriptor.ServiceDescriptor(
    name="DirectAccess",
    full_name="DirectAccess",
    file=DESCRIPTOR,
    index=0,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
    serialized_start=268,
    serialized_end=663,
    methods=[
        _descriptor.MethodDescriptor(
            name="apply",
            full_name="DirectAccess.apply",
            index=0,
            containing_service=None,
            input_type=_REQUEST,
            output_type=_RESPONSE,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="get_result",
            full_name="DirectAccess.get_result",
            index=1,
            containing_service=None,
            input_type=_REQUEST,
            output_type=_RESPONSE,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="get_status",
            full_name="DirectAccess.get_status",
            index=2,
            containing_service=None,
            input_type=_REQUEST,
            output_type=_RESPONSE,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="cancel",
            full_name="DirectAccess.cancel",
            index=3,
            containing_service=None,
            input_type=_REQUEST,
            output_type=_RESPONSE,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="set_cache",
            full_name="DirectAccess.set_cache",
            index=4,
            containing_service=None,
            input_type=_CACHECONTENT,
            output_type=_CACHECONTENT,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="get_cache",
            full_name="DirectAccess.get_cache",
            index=5,
            containing_service=None,
            input_type=_CACHECONTENT,
            output_type=_CACHECONTENT,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="delete_cache",
            full_name="DirectAccess.delete_cache",
            index=6,
            containing_service=None,
            input_type=_CACHECONTENT,
            output_type=_CACHECONTENT,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="stream_set_cache",
            full_name="DirectAccess.stream_set_cache",
            index=7,
            containing_service=None,
            input_type=_CACHECONTENT,
            output_type=_CACHECONTENT,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.MethodDescriptor(
            name="stream_get_cache",
            full_name="DirectAccess.stream_get_cache",
            index=8,
            containing_service=None,
            input_type=_CACHECONTENT,
            output_type=_CACHECONTENT,
            serialized_options=None,
            create_key=_descriptor._internal_create_key,
        ),
    ],
)
_sym_db.RegisterServiceDescriptor(_DIRECTACCESS)

DESCRIPTOR.services_by_name["DirectAccess"] = _DIRECTACCESS

# @@protoc_insertion_point(module_scope)
