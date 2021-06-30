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
from __future__ import absolute_import, print_function, unicode_literals

from common.elasticsearch import ElasticSearchClient
from conf.settings import (
    DATA_TRACE_ES_API_HOST,
    DATA_TRACE_ES_API_PORT,
    DATA_TRACE_ES_PASS_TOKEN,
    ES_USER,
)
from datatrace.data_trace_dict import (
    DATA_TRACE_ES_INDEX_NAME,
    DATA_TRACE_ES_NAME,
    DATA_TRACE_ES_TYPE_NAME,
)


class DataTraceElasticSearchClient(object):
    def __init__(self):
        self._es = None
        self.es_name = DATA_TRACE_ES_NAME
        self.host = DATA_TRACE_ES_API_HOST
        self.port = DATA_TRACE_ES_API_PORT
        self.user = ES_USER
        self.token = DATA_TRACE_ES_PASS_TOKEN
        self.index_name = DATA_TRACE_ES_INDEX_NAME
        self.type_name = DATA_TRACE_ES_TYPE_NAME

    @property
    def es(self):
        if self._es is None:
            self._es = ElasticSearchClient(
                self.es_name, self.host, self.port, self.user, self.token
            )
        return self._es

    def bulk_data(self, bulk_data_list):
        self.es.bulk_data(
            bulk_data_list, index_name=self.index_name, type_name=self.type_name
        )
