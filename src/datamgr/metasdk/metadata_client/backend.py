# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from __future__ import absolute_import, print_function, unicode_literals

import functools
import logging

from metadata_client.exc import RPCProcedureError
from metadata_client.resource import http_session

module_logger = logging.getLogger(__name__)


def pre_query():
    def __func(func):
        @functools.wraps(func)
        def _func(*args, **kwargs):
            try:
                rpc_extra = kwargs.get(str("rpc_extra"), {})
                kwargs[str("rpc_extra")] = rpc_extra
                resp = func(*args, **kwargs)
                resp.raise_for_status()
            except Exception as e:
                module_logger.exception("Query to metaapi failed. detail: {}".format(e))
                raise
            if resp:
                return resp.json()
            else:
                raise RPCProcedureError(message_kv={"error": "Query返回结果为空."})

        return _func

    return __func


class ApiBackendMixIn(object):

    settings = None

    @pre_query()
    def entity_complex_search(self, statement, backend_type="mysql", **kwargs):
        kwargs[str("statement")] = statement
        kwargs[str("backend_type")] = backend_type
        return http_session.post(self.settings.endpoints("complex_search"), data=None, json=kwargs)

    @pre_query()
    def entity_query_via_erp(self, retrieve_args, **kwargs):
        kwargs[str("retrieve_args")] = retrieve_args
        return http_session.post(self.settings.endpoints("query_via_erp"), data=None, json=kwargs)

    @pre_query()
    def entity_edit(self, operate_records_lst, **kwargs):
        kwargs[str("operate_records_lst")] = operate_records_lst
        return http_session.post(self.settings.endpoints("entity_edit"), data=None, json=kwargs)

    @pre_query()
    def bridge_sync(self, sync_contents, content_mode="id", batch=False, **kwargs):
        kwargs[str("db_operations_list")] = sync_contents
        kwargs[str("content_mode")] = content_mode
        kwargs[str("batch")] = batch
        return http_session.post(self.settings.endpoints("bridge_sync"), data=None, json=kwargs)
