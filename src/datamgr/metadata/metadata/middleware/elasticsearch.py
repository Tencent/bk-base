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

# rabbitmq 中间件类

import json

import requests


class ElasticSearchClient(object):

    INDEX_NAME = 'data_trace_log'
    TYPE_NAME = 'data_trace'
    JSON_HEADERS = {"Content-Type": "application/json"}
    X_NDJSON_HEADERS = {"content-type": "application/x-ndjson"}
    TIMEOUT_S = 10

    def __init__(self, api_addr, auth_tuple):
        self.api_addr = api_addr
        self.es_auth = auth_tuple

    def insert_data(self, data_obj):
        response = requests.post(
            url='{}/{}/{}'.format(self.api_addr, self.INDEX_NAME, self.TYPE_NAME),
            headers=self.JSON_HEADERS,
            auth=self.es_auth,
            timeout=self.TIMEOUT_S,
            json=data_obj,
        )
        try:
            response.raise_for_status()
        except Exception as he:
            message = '[elasticsearch insert] {}: code {}. {}'.format(he, response.status_code, response.text)
            return False, message
        return True, response.text

    def bulk_data(self, bulk_obj_list):
        if not isinstance(bulk_obj_list, list):
            return False, 'bulk_obj_list is not a list: {}'.format(bulk_obj_list)
        bulk_data_list = list()
        for item in bulk_obj_list:
            method = item.get('method', None)
            data_obj = item.get('data', {})
            data_id = data_obj.get('id', None)
            if all([method, data_id]):
                bulk_data_list.append(
                    '{{"create":{{"_index":"{}","_type":"{}","_id":"{}"}}}}'.format(
                        self.INDEX_NAME, self.TYPE_NAME, data_id
                    )
                )
                bulk_data_list.append(json.dumps(data_obj))
        bulk_data_list.append('')
        bulk_data = "\n".join(bulk_data_list)
        response = requests.post(
            url='{}/_bulk'.format(self.api_addr),
            headers=self.X_NDJSON_HEADERS,
            auth=self.es_auth,
            timeout=self.TIMEOUT_S,
            data=bulk_data,
        )
        try:
            response.raise_for_status()
        except Exception as he:
            message = '[elasticsearch bulk] {}: code {}. {}'.format(he, response.status_code, response.text)
            return False, message
        return True, response.text

    def search_data(self):
        query_tpl = """
        {
            "query": {
                "match": {
                    "data_set_id": "data_set_id"
                }
            }
        }
        """
        response = requests.post(
            url='{}/{}/{}/_search'.format(self.api_addr, self.INDEX_NAME, self.TYPE_NAME),
            headers=self.JSON_HEADERS,
            auth=self.es_auth,
            timeout=self.TIMEOUT_S,
            data=query_tpl,
        )
        try:
            response.raise_for_status()
        except Exception as he:
            message = '[elasticsearch search] {}: code {}. {}'.format(he, response.status_code, response.text)
            return False, message
        return True, response.text
