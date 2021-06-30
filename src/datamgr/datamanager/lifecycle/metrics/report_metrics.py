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

import json
import logging
import time

from api import datamanage_api

logger = logging.getLogger(__name__)


class MetricDictMapper(dict):
    def __init__(self, original_dict, metric_type):
        if metric_type == "range":
            new_dict = {
                "biz_count": original_dict["biz_count"],
                "proj_count": original_dict["project_count"],
                "depth": original_dict["depth"],
                "node_count": original_dict["node_count_list"],
                "weighted_node": original_dict["weighted_node_count"],
                "range_score": original_dict["range_score"],
                "normalized_range_score": original_dict["normalized_range_score"],
                "app_code_count": original_dict["app_code_count"],
            }
        elif metric_type == "asset_value":
            new_dict = {
                "normalized_range_score": original_dict["normalized_range_score"],
                "heat_score": original_dict["heat_score"],
                "importance_score": original_dict["importance_score"],
                "asset_value_score": original_dict["asset_value_score"],
            }
        elif metric_type == "lifecycle":
            new_dict = {
                "assetvalue_to_cost": original_dict["assetvalue_to_cost"],
            }
        elif metric_type == "importance":
            new_dict = {
                "dataset_score": original_dict["dataset_score"],
                "biz_score": original_dict["biz_score"],
                "is_bip": 1 if original_dict["is_bip"] else 0,
                "oper_state_name": original_dict["oper_state_name"],
                "oper_state": original_dict["oper_state"],
                "bip_grade_name": original_dict["bip_grade_name"],
                "bip_grade_id": original_dict["bip_grade_id"],
                "app_important_level_name": original_dict["app_important_level_name"],
                "app_important_level": original_dict["app_important_level"],
                "project_score": original_dict["project_score"],
                "active": 1 if original_dict["active"] else 0,
                "importance_score": original_dict["importance_score"],
            }
        elif metric_type == "heat":
            new_dict = {
                "query_count": original_dict["query_count"],
                "queue_service_count": original_dict["queue_service_count"],
                "heat_score": original_dict["heat_score"],
            }
        elif metric_type == "cost":
            new_dict = {
                "hdfs_capacity": float(original_dict["hdfs_capacity"]),
                "tspider_capacity": float(original_dict["tspider_capacity"]),
                "total_capacity": float(original_dict["total_capacity"]),
                "capacity_score": float(original_dict["capacity_score"]),
            }

        super(MetricDictMapper, self).__init__(**new_dict)


def send_metric_to_kafka(dataset_id, dataset_type, metric_dict, metric_type):
    """
    指标写kafka topic
    :param dataset_id:
    :param dataset_type:
    :param metric_dict:
    :return:
    """
    kafka_metric_dict = MetricDictMapper(metric_dict, metric_type)
    send_message_to_kafka(
        "{}_metric".format(metric_type), dataset_id, dataset_type, kafka_metric_dict
    )


def send_heat_metric_to_kafka(dataset_id, dataset_type, metric_dict):
    """
    热度指标写kafka topic
    :param dataset_id:
    :param dataset_type:
    :param heat_metric:
    :return:
    """
    heat_metric = MetricDictMapper(metric_dict, "heat")
    send_message_to_kafka("heat_metrics", dataset_id, dataset_type, heat_metric)


def send_message_to_kafka(
    measurement, dataset_id, dataset_type, metric_dict, dimension_dict={}
):
    """
    :param measurement: kafka measurement
    :param dataset_id: 数据集id
    :param dataset_type: 数据集类型
    :param metric_dict: 数据集
    :return:
    """
    # 1）database统一为monitor_custom_metrics
    # 2）measurement为不同的表
    # 3）tags（维度字段）统一有dataset_id和dataset_type
    # 4）time统一为当前时间戳
    time_stamp = int(time.time() - 24 * 3600)
    message = {
        "database": "monitor_custom_metrics",
        measurement: {
            "tags": {"dataset_id": dataset_id, "dataset_type": dataset_type},
        },
        "time": time_stamp,
    }
    # 5) 添加维度字段
    if dimension_dict:
        message[measurement]["tags"].update(dimension_dict)
    # 6）会对度量字段做处理
    property_list = list(metric_dict.keys())
    for each_property in property_list:
        message[measurement][each_property] = metric_dict.get(each_property)
    # 7）send_message_to_kafka, 上报influxdb
    message_str = json.dumps(message)
    try:
        report_dict = datamanage_api.influx_report(
            {"message": message_str, "kafka_topic": "bkdata_monitor_metrics591"},
            retry_times=3,
            raise_exception=True,
        )
    except Exception as e:
        logger.error(
            "influx_report error:{}, dataset_id:{}".format(e.message, dataset_id)
        )
        return
    if not report_dict.is_success() or not report_dict.data:
        logger.error(
            "influx_report error:{}, dataset_id:{}".format(
                report_dict.message, dataset_id
            )
        )


def send_rt_day_query_count_to_kafka(
    dataset_id, dataset_type, metric_dict, dimension_dict
):
    """
    热度指标(rt日查询量)写kafka topic
    包括按天统计的查询量，app_code
    :param dataset_id:
    :param dataset_type:
    :param count:
    :return:
    """
    send_message_to_kafka(
        "heat_related_metric", dataset_id, dataset_type, metric_dict, dimension_dict
    )
