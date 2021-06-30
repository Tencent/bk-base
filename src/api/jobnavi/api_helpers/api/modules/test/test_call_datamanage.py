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

from .base import BaseTest


class TestDatamanage(BaseTest):
    def result_tables_status(self):
        data = {
            "stream__591_stream_test_abc": {
                "status": {
                    "interval": 3600,
                    "execute_history": [
                        {
                            "status": "fail",
                            "period_end_time": "2018-11-23 15:03:00",
                            "period_start_time": "2018-11-23 14:03:00",
                            "execute_list": [
                                {
                                    "status": "fail",
                                    "status_str": "失败",
                                    "exec_id": 6026,
                                    "end_time": "2018-11-23 14:03:02",
                                    "flow_id": "batch_sql_591_etl_p_b_13",
                                    "start_time": "2018-11-23 14:03:00",
                                    "err_msg": "000",
                                    "err_code": "Optional format Exception! : None",
                                }
                            ],
                            "status_str": "失败",
                        },
                        {
                            "status": "fail",
                            "period_end_time": "2018-11-23 14:03:00",
                            "period_start_time": "2018-11-23 13:03:00",
                            "execute_list": [
                                {
                                    "status": "fail",
                                    "status_str": "失败",
                                    "exec_id": 6007,
                                    "end_time": "2018-11-23 13:03:03",
                                    "flow_id": "batch_sql_591_etl_p_b_13",
                                    "start_time": "2018-11-23 13:03:00",
                                    "err_msg": "000",
                                    "err_code": "Optional format Exception! : None",
                                }
                            ],
                            "status_str": "失败",
                        },
                    ],
                },
                "alerts": [],
                "module": "stream",
                "component": "flink",
            },
            "databus__591_new_van": {
                "status": {
                    "drop_cnt": 0,
                    "start_time": "2018-11-23 14:49:33",
                    "input_cnt_10min": 0,
                    "input_cnt": 0,
                    "output_cnt": 0,
                },
                "alerts": [],
                "module": "databus",
                "component": "kafka",
            },
        }
        return self.success_response(data)

    def metrics_query(self):
        data = {
            "data_format": "simple",
            "series": [
                {
                    # data_delay_max
                    "logical_tag": "591_stream_test_abc1",
                    "delay_max": 0,
                    "time": 1542367740,
                    # dmonitor_alerts
                    # node
                    "last_full_message_zh-cn": "last_full_message_zh-cn",
                    # flow
                    "full_message_zh-cn": "最近10分钟数据量平均值77.1条/分，比7天前同比下降90.74%, 7天前平均833.3条/分",
                    # data_loss_io_total
                    # node
                    "cnt": 1,
                    # flow
                    "data_inc": 446,
                    "component": "*",
                    "module": "stream",
                    "node_id": "3",
                    "flow_id": "217",
                    "node_type": "realtime",
                    "result_table_id": "591_1",
                    "project_id": "1",
                    "version": "2.0",
                },
                {
                    "logical_tag": "591_stream_test_abc2",
                    "delay_max": 1,
                    "time": 1542367800,
                    # node
                    "last_full_message_zh-cn": "last_full_message_zh-cn",
                    # flow
                    "full_message_zh-cn": "最近10分钟数据量平均值77.1条/分，比7天前同比下降90.74%, 7天前平均833.3条/分",
                    # data_loss_io_total
                    # node
                    "cnt": 2,
                    # flow
                    "data_inc": 446,
                    "component": "*",
                    "module": "stream",
                    "node_id": "2",
                    "flow_id": "217",
                    "node_type": "realtime",
                    "result_table_id": "591_2",
                    "project_id": "1",
                    "version": "2.0",
                },
                {
                    "logical_tag": "591_new_van",
                    "delay_max": None,
                    "time": 1542367800,
                    # node
                    "last_full_message_zh-cn": "last_full_message_zh-cn",
                    # flow
                    "full_message_zh-cn": "最近10分钟数据量平均值77.1条/分，比7天前同比下降90.74%, 7天前平均833.3条/分",
                    # data_loss_io_total
                    # node
                    "cnt": 3,
                    # flow
                    "data_inc": 446,
                    "component": "default_component",
                    "module": "databus",
                    "node_id": "1",
                    "flow_id": "217",
                    "node_type": "realtime",
                    "result_table_id": "591_2",
                    "project_id": "1",
                    "version": "2.0",
                },
            ],
        }
        return self.success_response(data)

    def dmonitor_batch_executions(self):
        data = {
            "2_f1_batch_01": {
                "interval": 3600,
                "execute_history": [
                    {
                        "status": "fail",
                        "status_str": "失败",
                        "execute_id": "",
                        "err_msg": "",
                        "err_code": "0",
                        "start_time": "2018-11-11 11:11:11",
                        "end_time": "2018-11-11 12:11:11",
                        "period_start_time": "2018-11-11 11:11:11",
                        "period_end_time": "2018-11-11 12:11:11",
                    }
                ],
            }
        }
        return self.success_response(data)

    def dmonitor_alert_details(self):
        data = [
            {
                "id": 1437730,
                "message": "任务(flink关联数据2)节点(test_sssss)从2019-07-01 10:59:11开始持续2天执行异常",
                "message_en": "There are errors about Task(flink关联数据2) Node(test_sssss) "
                              "at 2019-07-01 10:59:11 lasted for 2d.",
                "full_message": "任务(flink关联数据2)节点(test_sssss)从2019-07-01 10:59:11开始持续2天执行异常",
                "full_message_en": "There are errors about Task(flink关联数据2) Node(test_sssss) "
                                   "at 2019-07-01 10:59:11 lasted for 2d.",
                "alert_config_id": 800,
                "alert_code": "task",
                "monitor_config": {"failed_succeeded_error": False, "monitor_status": "on", "no_metrics_interval": 600},
                "receivers": ["xxx"],
                "notify_ways": ["eewechat", "wechat"],
                "flow_id": "1535",
                "node_id": "7876",
                "dimensions": "",
                "alert_id": "e28b3e63521d4d1abe4de090b09f06ed",
                "alert_level": "danger",
                "alert_status": "alerting",
                "alert_send_status": "success",
                "alert_send_error": "",
                "alert_time": "2019-07-01 10:59:11",
                "alert_send_time": "2019-07-01 11:00:13",
                "alert_recover_time": None,
                "alert_converged_info": "{}",
                "created_at": "2019-07-01 10:59:13",
                "updated_at": "2019-07-01 11:00:22",
                "description": "",
            },
            {
                "id": 1437731,
                "message": "任务(flink关联数据2)节点(test_sss222)从2019-07-01 10:59:11开始持续2天执行异常",
                "message_en": "There are errors about Task(flink关联数据2) Node(test_sss222) "
                              "at 2019-07-01 10:59:11 lasted for 2d.",
                "full_message": "任务(flink关联数据2)节点(test_sss222)从2019-07-01 10:59:11开始持续2天执行异常",
                "full_message_en": "There are errors about Task(flink关联数据2) Node(test_sss222) "
                                   "at 2019-07-01 10:59:11 lasted for 2d.",
                "alert_config_id": 800,
                "alert_code": "task",
                "monitor_config": {"failed_succeeded_error": False, "monitor_status": "on", "no_metrics_interval": 600},
                "receivers": ["xxx"],
                "notify_ways": ["eewechat", "wechat"],
                "flow_id": "1535",
                "node_id": "7892",
                "dimensions": "",
                "alert_id": "b3f6a91d0c0f41409b01f3ae3457d9fa",
                "alert_level": "danger",
                "alert_status": "alerting",
                "alert_send_status": "success",
                "alert_send_error": "",
                "alert_time": "2019-07-01 10:59:11",
                "alert_send_time": "2019-07-01 11:00:13",
                "alert_recover_time": None,
                "alert_converged_info": "{}",
                "created_at": "2019-07-01 10:59:13",
                "updated_at": "2019-07-01 11:00:26",
                "description": "",
            },
        ]
        return self.success_response(data)

    def dmonitor_dataflow(self):
        return self.common_success()
