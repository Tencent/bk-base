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

import json

from django.utils.translation import ugettext as _

from dataflow.pizza_settings import KAFKA_OP_ROLE_NAME
from dataflow.shared.api.modules.datamanage import DatamanageApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.databus.databus_helper import DatabusHelper


class DatamanageHelper(object):
    @staticmethod
    def op_metric_report(message, kafka_topic, tags):
        request_params = {
            "message": json.dumps(message),
            "kafka_topic": kafka_topic,
            "tags": tags,
        }
        res = DatamanageApi.metrics.report(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table_metric(database, sql, geog_area_code):
        request_params = {"database": database, "sql": sql, "tags": [geog_area_code]}
        res = DatamanageApi.metrics.query(request_params)
        res_util.check_response(res)
        return res.data

    # 优化指标，不再采用 sql 查询的方式
    @staticmethod
    def get_result_table_metric_v2(request_params):
        res = DatamanageApi.metrics_v2.list(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_alert_detail_v2(request_params):
        res = DatamanageApi.alert_detail.list(request_params)
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
        request_params = {"processing_ids": processing_ids}
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
            "flow_id": flow_id,
            "start_time": start_time,
            "end_time": end_time,
            "dimensions": json.dumps({"generate_type": "user"}),
        }
        res = DatamanageApi.dmonitor.alert_details(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_metric_kafka_server(geog_area_code):
        channels_info = DatabusHelper.list_channel_info([KAFKA_OP_ROLE_NAME, geog_area_code])
        if len(channels_info) != 1:
            raise Exception(_("kafka-op 连接信息不唯一，请联系管理员处理."))
        metric_kafka_server = "{}:{}".format(
            channels_info[0]["cluster_domain"],
            channels_info[0]["cluster_port"],
        )
        return metric_kafka_server

    @staticmethod
    def create_alert_config(flow_id, dmonitor_type="alert_configs"):
        request_params = {
            "flow_id": flow_id,
            "dmonitor_type": dmonitor_type,
        }
        res = DatamanageApi.dmonitor_dataflow.create(request_params)
        res_util.check_response(res)
        return res.data

    # 数据修正相关
    @staticmethod
    def create_data_correct(params):
        res = DatamanageApi.data_correct.create(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_data_correct(params):
        res = DatamanageApi.data_correct.update(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_data_correct(params):
        res = DatamanageApi.data_correct.retrieve(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def del_data_correct(params):
        res = DatamanageApi.data_correct.delete(params)
        res_util.check_response(res)
        return res.data

    # 数据模型应用相关
    @staticmethod
    def create_data_model_instance(params):
        res = DatamanageApi.data_model_instance.create(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_data_model_instance(params):
        res = DatamanageApi.data_model_instance.update(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_data_model_instance(params):
        res = DatamanageApi.data_model_instance.retrieve(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def del_data_model_instance(params):
        res = DatamanageApi.data_model_instance.delete(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def rollback_data_model_instance(params):
        res = DatamanageApi.data_model_instance.rollback(params)
        res_util.check_response(res)
        return res.data

    # 数据模型指标相关
    @staticmethod
    def create_data_model_indicator(params):
        res = DatamanageApi.data_model_indicator.create(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_data_model_indicator(params):
        res = DatamanageApi.data_model_indicator.update(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_data_model_indicator(params):
        res = DatamanageApi.data_model_indicator.retrieve(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def del_data_model_indicator(params):
        res = DatamanageApi.data_model_indicator.delete(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def rollback_data_model_indicator(params):
        res = DatamanageApi.data_model_indicator.rollback(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def check_data_model_instance(params):
        res = DatamanageApi.data_model_instance_check.list(params)
        return res
