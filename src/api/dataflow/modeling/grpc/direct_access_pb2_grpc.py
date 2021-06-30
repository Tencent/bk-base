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

import grpc

from . import direct_access_pb2 as direct__access__pb2


class DirectAccessStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.apply = channel.unary_unary(
            "/DirectAccess/apply",
            request_serializer=direct__access__pb2.Request.SerializeToString,
            response_deserializer=direct__access__pb2.Response.FromString,
        )
        self.get_result = channel.unary_unary(
            "/DirectAccess/get_result",
            request_serializer=direct__access__pb2.Request.SerializeToString,
            response_deserializer=direct__access__pb2.Response.FromString,
        )
        self.get_status = channel.unary_unary(
            "/DirectAccess/get_status",
            request_serializer=direct__access__pb2.Request.SerializeToString,
            response_deserializer=direct__access__pb2.Response.FromString,
        )
        self.cancel = channel.unary_unary(
            "/DirectAccess/cancel",
            request_serializer=direct__access__pb2.Request.SerializeToString,
            response_deserializer=direct__access__pb2.Response.FromString,
        )
        self.set_cache = channel.unary_unary(
            "/DirectAccess/set_cache",
            request_serializer=direct__access__pb2.CacheContent.SerializeToString,
            response_deserializer=direct__access__pb2.CacheContent.FromString,
        )
        self.get_cache = channel.unary_unary(
            "/DirectAccess/get_cache",
            request_serializer=direct__access__pb2.CacheContent.SerializeToString,
            response_deserializer=direct__access__pb2.CacheContent.FromString,
        )
        self.delete_cache = channel.unary_unary(
            "/DirectAccess/delete_cache",
            request_serializer=direct__access__pb2.CacheContent.SerializeToString,
            response_deserializer=direct__access__pb2.CacheContent.FromString,
        )
        self.stream_set_cache = channel.unary_unary(
            "/DirectAccess/stream_set_cache",
            request_serializer=direct__access__pb2.CacheContent.SerializeToString,
            response_deserializer=direct__access__pb2.CacheContent.FromString,
        )
        self.stream_get_cache = channel.unary_unary(
            "/DirectAccess/stream_get_cache",
            request_serializer=direct__access__pb2.CacheContent.SerializeToString,
            response_deserializer=direct__access__pb2.CacheContent.FromString,
        )


class DirectAccessServicer(object):
    """Missing associated documentation comment in .proto file."""

    def apply(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def get_result(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def get_status(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def cancel(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def set_cache(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def get_cache(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def delete_cache(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def stream_set_cache(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def stream_get_cache(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_DirectAccessServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "apply": grpc.unary_unary_rpc_method_handler(
            servicer.apply,
            request_deserializer=direct__access__pb2.Request.FromString,
            response_serializer=direct__access__pb2.Response.SerializeToString,
        ),
        "get_result": grpc.unary_unary_rpc_method_handler(
            servicer.get_result,
            request_deserializer=direct__access__pb2.Request.FromString,
            response_serializer=direct__access__pb2.Response.SerializeToString,
        ),
        "get_status": grpc.unary_unary_rpc_method_handler(
            servicer.get_status,
            request_deserializer=direct__access__pb2.Request.FromString,
            response_serializer=direct__access__pb2.Response.SerializeToString,
        ),
        "cancel": grpc.unary_unary_rpc_method_handler(
            servicer.cancel,
            request_deserializer=direct__access__pb2.Request.FromString,
            response_serializer=direct__access__pb2.Response.SerializeToString,
        ),
        "set_cache": grpc.unary_unary_rpc_method_handler(
            servicer.set_cache,
            request_deserializer=direct__access__pb2.CacheContent.FromString,
            response_serializer=direct__access__pb2.CacheContent.SerializeToString,
        ),
        "get_cache": grpc.unary_unary_rpc_method_handler(
            servicer.get_cache,
            request_deserializer=direct__access__pb2.CacheContent.FromString,
            response_serializer=direct__access__pb2.CacheContent.SerializeToString,
        ),
        "delete_cache": grpc.unary_unary_rpc_method_handler(
            servicer.delete_cache,
            request_deserializer=direct__access__pb2.CacheContent.FromString,
            response_serializer=direct__access__pb2.CacheContent.SerializeToString,
        ),
        "stream_set_cache": grpc.unary_unary_rpc_method_handler(
            servicer.stream_set_cache,
            request_deserializer=direct__access__pb2.CacheContent.FromString,
            response_serializer=direct__access__pb2.CacheContent.SerializeToString,
        ),
        "stream_get_cache": grpc.unary_unary_rpc_method_handler(
            servicer.stream_get_cache,
            request_deserializer=direct__access__pb2.CacheContent.FromString,
            response_serializer=direct__access__pb2.CacheContent.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("DirectAccess", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class DirectAccess(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def apply(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/apply",
            direct__access__pb2.Request.SerializeToString,
            direct__access__pb2.Response.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def get_result(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/get_result",
            direct__access__pb2.Request.SerializeToString,
            direct__access__pb2.Response.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def get_status(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/get_status",
            direct__access__pb2.Request.SerializeToString,
            direct__access__pb2.Response.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def cancel(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/cancel",
            direct__access__pb2.Request.SerializeToString,
            direct__access__pb2.Response.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def set_cache(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/set_cache",
            direct__access__pb2.CacheContent.SerializeToString,
            direct__access__pb2.CacheContent.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def get_cache(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/get_cache",
            direct__access__pb2.CacheContent.SerializeToString,
            direct__access__pb2.CacheContent.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def delete_cache(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/delete_cache",
            direct__access__pb2.CacheContent.SerializeToString,
            direct__access__pb2.CacheContent.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def stream_set_cache(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/stream_set_cache",
            direct__access__pb2.CacheContent.SerializeToString,
            direct__access__pb2.CacheContent.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def stream_get_cache(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/DirectAccess/stream_get_cache",
            direct__access__pb2.CacheContent.SerializeToString,
            direct__access__pb2.CacheContent.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
