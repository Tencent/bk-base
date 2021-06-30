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
import bkbase.dataflow.core.api.custom_http as http


class MetaApi(object):
    def __init__(self, api_url):
        self.api_url = api_url
        self.get_result_table_url = self.api_url["base_meta_url"] + "result_tables/{result_table_id}/"

    def get_result_table(self, result_table_id, related=None):
        if related is None:
            related = []
        data = {"related": related}
        _, result = http.get(self.get_result_table_url.format(result_table_id=result_table_id), data)
        if result["result"]:
            # test
            # kafka = {
            #     "storage_channel": {
            #         "cluster_domain": "kafka-dev.service.dev-1.bk",
            #         "cluster_port": 9092,
            #         "cluster_type": "kafka"
            #     },
            #     "physical_table_name": "table_%s" % result_table_id,
            #     "result_table_id": result_table_id
            # }
            # result['data']['storages']['kafka'] = kafka
            return result["data"]
        else:
            raise Exception("Get the result table (%s) info failed. " % result_table_id + str(result))
