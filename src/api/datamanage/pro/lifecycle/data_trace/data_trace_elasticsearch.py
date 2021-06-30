# coding=utf-8
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


import json

from common.log import logger

from datamanage.pro import pizza_settings
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.lifecycle.models_dict import (
    DATA_TRACE_ES_INDEX_NAME,
    DATA_TRACE_ES_MAX_RESULT_WINDOW,
    DATA_TRACE_ES_NAME,
    DATA_TRACE_ES_TYPE_NAME,
)
from datamanage.utils.dbtools.es_utils import ElasticSearchClient

DATA_TRACE_ES_TOKEN = pizza_settings.DATA_TRACE_ES_TOKEN
ES_USER = pizza_settings.ES_USER
DATA_TRACE_ES_API_HOST = pizza_settings.DATA_TRACE_ES_API_HOST
DATA_TRACE_ES_API_PORT = pizza_settings.DATA_TRACE_ES_API_PORT


class DataTraceElasticSearchClient(object):
    def __init__(self):
        self._es = None
        self.es_name = DATA_TRACE_ES_NAME
        self.host = DATA_TRACE_ES_API_HOST
        self.port = DATA_TRACE_ES_API_PORT
        self.user = ES_USER
        self.token = DATA_TRACE_ES_TOKEN
        self.index_name = DATA_TRACE_ES_INDEX_NAME
        self.type_name = DATA_TRACE_ES_TYPE_NAME
        self.max_result_window = DATA_TRACE_ES_MAX_RESULT_WINDOW

    @property
    def es(self):
        if self._es is None:
            self._es = ElasticSearchClient(self.es_name, self.host, self.port, self.user, self.token)
        return self._es

    def search_data_trace(self, dataset_id):
        """查询dataset_id

        :param dataset_id:
        :return:
        """
        query_tpl_dict = json.loads(
            """
        {
            "query": {
                "match": {
                    "data_set_id": "%s"
                }
            }
        }"""
            % dataset_id
        )
        try:
            search_dict = self.es.search_data(query_tpl_dict, self.index_name, from_=0, size=self.max_result_window)
        except dm_pro_errors.EsSearchError as e:
            logger.error('data_set_id:{} elastic search data error, for:{}'.format(dataset_id, e))
            raise e.__class__(message_kv={'error_info': e})
        return search_dict

    def bulk_data(self, bulk_data_list):
        self.es.bulk_data(bulk_data_list, index_name=self.index_name, type_name=self.type_name)
