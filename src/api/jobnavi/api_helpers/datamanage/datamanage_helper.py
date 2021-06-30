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
from jobnavi.api_helpers.api.modules.datamanage import DatamanageApi
from jobnavi.api_helpers.api.util.api_driver import APIResponseUtil as res_util


class DatamanageHelper(object):

    @staticmethod
    def op_metric_report(message, kafka_topic, tags):
        request_params = {
            'message': json.dumps(message),
            'kafka_topic': kafka_topic,
            'tags': tags
        }
        res = DatamanageApi.metrics.report(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table_metric(database, sql, geog_area_code):
        request_params = {
            'database': database,
            'sql': sql,
            'tags': [geog_area_code]
        }
        res = DatamanageApi.metrics.query(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_batch_executions(processing_ids):
        """
        {
            'interval': 3600,
            'execute_history': []
        }
        @param processing_ids:
        @return:
        """
        request_params = {
            'processing_ids': processing_ids
        }
        res = DatamanageApi.dmonitor.batch_executions(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_alert_details(flow_id, start_time, end_time):
        """
        [{
            'message': 'xxx',
            'message_en': 'xxx',
            'full_message': 'xxx',
            'full_message_en': 'xxx'
        }]
        @param flow_id:
        @param start_time:
        @param end_time:
        @return:
        """
        request_params = {
            'flow_id': flow_id,
            'start_time': start_time,
            'end_time': end_time
        }
        res = DatamanageApi.dmonitor.alert_details(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_alert_config(flow_id, dmonitor_type='alert_configs'):
        request_params = {
            'flow_id': flow_id,
            'dmonitor_type': dmonitor_type,
        }
        res = DatamanageApi.dmonitor_dataflow.create(request_params)
        res_util.check_response(res)
        return res.data
