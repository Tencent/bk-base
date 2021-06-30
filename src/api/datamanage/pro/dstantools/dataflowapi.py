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
from common.api.base import DataAPI

from datamanage.pizza_settings import DATAFLOW_API_ROOT
from datamanage.pizza_settings import BKSQL_V1_API_ROOT
from datamanage.pizza_settings import DATAQUERY_API_ROOT


class _DataFlowApi(object):
    def __init__(self):
        self.parse_sql = DataAPI(
            method='POST',
            url=BKSQL_V1_API_ROOT + '/convert/common',
            module='sqlparse',
            description='sqlparse',
            custom_headers={'postman-token': 'b44df4fb-3008-04b8-9dc1-00d7d2604877'},
        )

        self.create_dataflow = DataAPI(
            method='POST', url=DATAFLOW_API_ROOT + '/flow/flows/create/', module='dataflow', description='dataflow'
        )

        self.start_dataflow = DataAPI(
            method='POST',
            url=DATAFLOW_API_ROOT + '/flow/flows/{flow_id}/start/',
            module='dataflow',
            url_keys=['flow_id'],
            description='dataflow',
        )

        self.stop_dataflow = DataAPI(
            method='POST',
            url=DATAFLOW_API_ROOT + '/flow/flows/{flow_id}/stop/',
            module='dataflow',
            url_keys=['flow_id'],
            description='dataflow',
        )

        self.get_dataflow_status = DataAPI(
            method='GET',
            url=DATAFLOW_API_ROOT + '/flow/flows/{flow_id}/',
            module='dataflow',
            url_keys=['flow_id'],
            description='dataflow',
        )

        self.data_query = DataAPI(
            method='POST', url=DATAQUERY_API_ROOT + '/query/', module='data_query', description='data_query'
        )

        self.flink_sql = DataAPI(
            method='POST', url=BKSQL_V1_API_ROOT + '/convert/flink-sql', module='dataflow', description='dataflow'
        )

        self.flink_sql_check = DataAPI(
            method='POST', url=BKSQL_V1_API_ROOT + '/convert/flink-sql-check', module='dataflow', description='dataflow'
        )


DataFlowAPI = _DataFlowApi()
