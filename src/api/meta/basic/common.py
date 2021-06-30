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

import functools
import random

from common.bklanguage import BkLanguage
from common.log import logger_api
from common.views import APIView, APIViewSet
from django.conf import settings as active_settings
from django.utils.translation import ugettext_lazy as _
from tinyrpc import RPCCall, RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCErrorResponse, JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

from meta.exceptions import (
    MetaDataCallError,
    MetaDataComplianceError,
    RemoteSyncError,
    SyncError,
)
from meta.resource import meta_access_session


def rpc_communicate(rpc_call_error=None, rpc_error=None, ret_all=False):
    """
    rpc请求装饰器

    :param rpc_call_error: 发起RPC请求失败抛出的异常类型
    :param rpc_error: RPC发出后流程失败抛出的异常类型
    :param ret_all: 当前RPC请求均为并发模式，指定返回数据的方式(False-返回头部第一个/True-返回整个列表)
    :return:
    """
    rpc_call_error = MetaDataComplianceError if not rpc_call_error else rpc_call_error
    rpc_error = MetaDataCallError if not rpc_error else rpc_error

    def __func(func):
        @functools.wraps(func)
        def _func(*args, **kwargs):
            try:
                rpc_extra = kwargs.get(str("rpc_extra"), {})
                rpc_extra["language"] = BkLanguage.current_language()
                kwargs[str("rpc_extra")] = rpc_extra
                kwargs[str("token_level")] = 0
                data = func(*args, **kwargs)
                if not isinstance(data, list):
                    raise rpc_call_error(message_kv={"inner_error": _("请使用call_all并发RPC流程.")})
                # 对所有返回进行校验解析，确保所有返回都正常
                for item in data:
                    if isinstance(item, tuple) and len(item) == 3:
                        e_type, e_value, e_trace = item
                        raise e_type(e_value).with_traceback(e_trace)
                    if isinstance(item, JSONRPCErrorResponse):
                        dispatch_id = getattr(item, "data", {}).get("dispatch_id", "")
                        if item._jsonrpc_error_code > 0:
                            inner_error = _("{} | {} | 调度ID：{}").format(
                                item.error, item._jsonrpc_error_code, dispatch_id
                            )
                        else:
                            inner_error = _("错误已记录. | 调度ID：{}").format(dispatch_id)
                        errors = {
                            "exc": item.error
                            if not getattr(item, "data", {}).get("exc_msg", None)
                            else item.data["exc_msg"]
                        }
                        trace_back = getattr(item, "data", {}).get("tb", None)
                        if trace_back:
                            errors["tb"] = trace_back
                        raise rpc_call_error(message_kv={"inner_error": inner_error}, errors=errors)
            except Exception as e:
                if isinstance(e, rpc_call_error):
                    raise
                logger_api.exception("Fail in RPC.")
                raise rpc_error(message_kv={"inner_error": _("RPC流程失败:{}.").format(repr(e))}, errors=repr(e))
            if data:
                return data if ret_all else data[0]
            else:
                raise rpc_error(message_kv={"inner_error": _("RPC返回结果为空.")})

        return _func

    return __func


class RPCMixIn(object):
    @property
    def rpc_client(self):
        return RPCClient(
            JSONRPCProtocol(),
            HttpPostClientTransport(
                self.get_meta_access_rpc_endpoint(),
                meta_access_session.post,
                timeout=active_settings.METADATA_ACCESS_RPC_TIMEOUT,
            ),
        )

    @staticmethod
    def get_meta_access_rpc_endpoint():
        endpoint_hosts = active_settings.METADATA_ACCESS_RPC_ENDPOINT_HOSTS
        endpoint_host = random.choice(endpoint_hosts)
        endpoint_port = active_settings.METADATA_ACCESS_RPC_ENDPOINT_PORT
        return active_settings.METADATA_ACCESS_RPC_ENDPOINT_TEMPLATE.format(
            METADATA_RPC_HOST=endpoint_host, METADATA_RPC_PORT=endpoint_port
        )

    @rpc_communicate()
    def entity_complex_search(self, statement, backend_type="mysql", **kwargs):
        kwargs["statement"] = statement
        kwargs["backend_type"] = backend_type
        return self.rpc_client.call_all([RPCCall("entity_complex_search", args=[], kwargs=kwargs)])

    @rpc_communicate()
    def entity_query_via_erp(self, retrieve_args, backend_type="dgraph", **kwargs):
        kwargs["retrieve_args"] = retrieve_args
        kwargs["backend_type"] = backend_type
        return self.rpc_client.call_all([RPCCall("entity_query_via_erp", args=[], kwargs=kwargs)])

    @rpc_communicate()
    def entity_query_lineage(self, **kwargs):
        return self.rpc_client.call_all([RPCCall("entity_query_lineage", args=[], kwargs=kwargs)])

    @rpc_communicate(rpc_call_error=RemoteSyncError, rpc_error=SyncError)
    def bridge_sync(self, db_operations_list, **kwargs):
        kwargs[str("db_operations_list")] = db_operations_list
        return self.rpc_client.call_all([RPCCall("bridge_sync", args=[], kwargs=kwargs)])

    @rpc_communicate()
    def tag_system_search_targets(self, **kwargs):
        return self.rpc_client.call_all([RPCCall("tag_search_targets", args=[], kwargs=kwargs)])

    @rpc_communicate()
    def analyse_result_table_similarity(self, **kwargs):
        return self.rpc_client.call_all([RPCCall("analyse_result_table_similarity", args=[], kwargs=kwargs)])

    @rpc_communicate()
    def analyse_suggest_field_alias(self, **kwargs):
        return self.rpc_client.call_all([RPCCall("analyse_result_table_similarity", args=[], kwargs=kwargs)])


class RPCView(RPCMixIn, APIView):
    pass


class RPCViewSet(RPCMixIn, APIViewSet):
    pass


class RPCUtil(RPCMixIn):
    pass
