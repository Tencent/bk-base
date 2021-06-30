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


import copy
import datetime
import logging
import math
import time

import pandas as pd
from celery import shared_task
from celery.schedules import crontab
from datamanage import celery_app
from datamanage.pro.datastocktake.views import SCORE_DICT
from datamanage.pro.lifecycle.utils import generate_influxdb_time_range, get_timestamp
from datamanage.pro.pizza_settings import ASSET_VALUE_SCORE
from datamanage.pro.utils.time import get_date
from datamanage.utils.api import DatamanageApi, DataqueryApi, PaasApi
from django.conf import settings
from django.core.cache import cache

from common.log import ErrorCodeLogger

# 定时任务日志
logger = ErrorCodeLogger(logging.getLogger("range_task"))
heat_logger = ErrorCodeLogger(logging.getLogger("heat_task"))
sort_logger = ErrorCodeLogger(logging.getLogger("score_sort_task"))
data_inventory_logger = ErrorCodeLogger(logging.getLogger("data_inventory_task"))
app_code_logger = ErrorCodeLogger(logging.getLogger("app_code_task"))

COUNT = 100
DATASET_LIST_PARAMS = {
    "bk_biz_id": None,
    "project_id": None,
    "tag_ids": [],
    "keyword": "",
    "tag_code": "virtual_data_mart",
    "me_type": "tag",
    "has_standard": 1,
    "cal_type": ["standard"],
    "data_set_type": "all",
    "page": 1,
    "page_size": COUNT,
    "platform": "bk_data",
}
RUN_MODE = getattr(settings, "RUN_MODE", "DEVELOP")
clean_project_id = 4

CONDITION_DICT = {
    "asset_value": {
        "metric_condition": "last(asset_value_score) as asset_value_score",
        "measurement": "asset_value_metric",
    },
    "heat": {
        "metric_condition": "last(heat_score) as heat_score",
        "measurement": "heat_metrics",
    },
    "range": {
        "metric_condition": "last(normlized_range_score) as normalized_range_score",
        "measurement": "range_metric",
    },
    "importance": {
        "metric_condition": "last(importance_score) as importance_score",
        "measurement": "importance_metric",
    },
    "assetvalue_to_cost": {
        "metric_condition": "last(assetvalue_to_cost) as assetvalue_to_cost",
        "measurement": "lifecycle_metric",
    },
    "cost": {
        "metric_condition": """last(hdfs_capacity) as hdfs_capacity, last(tspider_capacity) as tspider_capacity,
        last(total_capacity) as total_capacity""",
        "measurement": "cost_metric",
    },
}


@shared_task(queue="datamanage_lifecycle")
def data_value_inventory_task():
    """
    对rt和数据源进行数据价值盘点
    @return:
    """
    data_inventory_logger.info("data value inventory task start, time:{time}".format(time=datetime.datetime.now()))
    params = copy.deepcopy(DATASET_LIST_PARAMS)
    lifecycle_target_dict = {
        "assetvalue_to_cost": True,
        "asset_value": {"asset_value_score": True},
        "cost": {
            "capacity": {
                "hdfs_capacity": True,
                "tspider_capacity": True,
                "total_capacity": True,
            }
        },
        "range": {
            "node_count": True,
            "node_count_list": True,
            "project_count": True,
            "biz_count": True,
            "normalized_range_score": True,
            "app_code_count": True,
        },
        "heat": {"heat_score": True, "query_count": True},
        "importance": {
            "importance_score": True,
            # 是否关联BIP系统
            "is_bip": True,
            # 业务重要度
            "app_important_level_name": True,
            # 业务星级
            "bip_grade_name": True,
            # 运营状态
            "oper_state_name": True,
            # 项目运营状态
            "active": True,
        },
    }
    erp_dict = {
        "ResultTable": {
            "expression": {
                "bk_biz_id": True,
                "sensitivity": True,
                "project_id": True,
                "generate_type": True,
                "~LifeCycle.target": lifecycle_target_dict,
            }
        },
        "AccessRawData": {
            "expression": {
                "bk_biz_id": True,
                "sensitivity": True,
                "~LifeCycle.target": lifecycle_target_dict,
            }
        },
        "TdwTable": {
            "expression": {
                "table_id": True,
            }
        },
    }

    # 数据集大小
    search_dict = DatamanageApi.get_data_dict_count_product(params).data
    search_count = search_dict.get("count", 0)
    data_inventory_logger.info("get data dict count:{}".format(search_count))
    metric_list = []
    params["extra_retrieve"] = erp_dict
    try:
        for i in range(int(math.ceil(search_count / float(COUNT)))):
            params["page"] = i + 1
            # 数据集列表
            try:
                search_list = DatamanageApi.get_data_dict_list_product(params, raise_exception=True).data
                data_inventory_logger.info("data value inventory task get data dict list, page:{}".format(i + 1))
            except Exception as e:
                data_inventory_logger.error("data_value_inventory_task error:{}, params page:{}".format(str(e), i + 1))
                continue
            for each_dataset in search_list:
                heat_metric = (
                    each_dataset["~LifeCycle.target"][0]["heat"][0]
                    if each_dataset.get("~LifeCycle.target") and each_dataset["~LifeCycle.target"][0].get("heat")
                    else {"query_count": 0, "heat_score": 0.0}
                )
                if "identifier_value" in heat_metric:
                    del heat_metric["identifier_value"]

                range_metric = (
                    each_dataset["~LifeCycle.target"][0]["range"][0]
                    if each_dataset.get("~LifeCycle.target") and each_dataset["~LifeCycle.target"][0].get("range")
                    else {
                        "project_count": 1,
                        "normalized_range_score": 13.1,
                        "node_count": 0,
                        "biz_count": 1,
                        "app_code_count": 0,
                    }
                )
                if "identifier_value" in range_metric:
                    del range_metric["identifier_value"]

                importance_metric = (
                    each_dataset["~LifeCycle.target"][0]["importance"][0]
                    if each_dataset.get("~LifeCycle.target") and each_dataset["~LifeCycle.target"][0].get("importance")
                    else {
                        "oper_state_name": "其他",
                        "is_bip": False,
                        "active": True,
                        "app_important_level_name": "其他",
                        "importance_score": 0,
                        "bip_grade_name": "其他",
                    }
                )
                if not importance_metric["oper_state_name"]:
                    importance_metric["oper_state_name"] = "其他"
                if not importance_metric["bip_grade_name"]:
                    importance_metric["bip_grade_name"] = "其他"
                if not importance_metric["app_important_level_name"]:
                    importance_metric["app_important_level_name"] = "其他"

                if "identifier_value" in importance_metric:
                    del importance_metric["identifier_value"]

                capacity_metric = (
                    each_dataset["~LifeCycle.target"][0]["cost"][0]["capacity"][0]
                    if each_dataset.get("~LifeCycle.target")
                    and each_dataset["~LifeCycle.target"][0].get("cost")
                    and each_dataset["~LifeCycle.target"][0]["cost"][0].get("capacity")
                    else {
                        "hdfs_capacity": 0,
                        "tspider_capacity": 0,
                        "total_capacity": 0,
                    }
                )

                if "identifier_value" in capacity_metric:
                    del capacity_metric["identifier_value"]

                asset_value_metric = (
                    each_dataset["~LifeCycle.target"][0]["asset_value"][0]
                    if each_dataset.get("~LifeCycle.target") and each_dataset["~LifeCycle.target"][0].get("asset_value")
                    else {"asset_value_score": ASSET_VALUE_SCORE}
                )
                if "identifier_value" in asset_value_metric:
                    del asset_value_metric["identifier_value"]

                assetvalue_to_cost = (
                    each_dataset["~LifeCycle.target"][0].get("assetvalue_to_cost", -1)
                    if each_dataset.get("~LifeCycle.target")
                    else -1
                )

                metric_dict = {
                    "dataset_id": each_dataset["data_set_id"],
                    "dataset_type": each_dataset["data_set_type"],
                    "sensitivity": each_dataset.get("sensitivity", "private"),
                    "generate_type": each_dataset.get("generate_type", "user"),
                    "bk_biz_id": each_dataset.get("bk_biz_id", -1),
                    "project_id": each_dataset.get("project_id", -1)
                    if each_dataset["data_set_type"] == "result_table"
                    else clean_project_id,
                }
                metric_dict.update(heat_metric)
                metric_dict.update(range_metric)
                metric_dict.update(importance_metric)
                metric_dict.update(capacity_metric)
                metric_dict.update(asset_value_metric)
                metric_dict.update({"assetvalue_to_cost": assetvalue_to_cost})
                metric_list.append(metric_dict)
    except Exception as e:
        data_inventory_logger.exception("data inventory task shut down unexpectedly, errors: [%s]" % str(e))
        return

    data_inventory_logger.info("metric_list length:{}".format(len(metric_list)))
    cache.set("data_value_stocktake", metric_list, 24 * 60 * 60 * 7)

    data_inventory_logger.info(
        "data_value_inventory_task periodic task end, time:{time}".format(time=datetime.datetime.now())
    )


@shared_task(queue="datamanage_lifecycle")
def data_trend_task():
    """
    对rt和数据源进行数据价值盘点
    @return:
    """
    data_inventory_logger.info("data trend task start, time:{time}".format(time=datetime.datetime.now()))
    params = copy.deepcopy(DATASET_LIST_PARAMS)
    # 数据集大小
    search_dict = DatamanageApi.get_data_dict_count_product(params).data
    search_count = search_dict.get("count", 0)
    data_inventory_logger.info("get data dict count:{}".format(search_count))
    start_time = get_timestamp(day=6)
    end_time = int(time.time())
    start_timestamp, end_timestamp = generate_influxdb_time_range(start_time, end_time)
    frequency = "1d"
    group_by = "GROUP BY time({})".format(frequency)
    trend_list = []

    try:
        for i in range(int(math.ceil(search_count / float(COUNT)))):
            params["page"] = i + 1
            # 数据集列表
            try:
                search_list = DatamanageApi.get_data_dict_list_product(params, raise_exception=True).data
                data_inventory_logger.info("data_trend task get data dict list, page:{}".format(i + 1))
            except Exception as e:
                data_inventory_logger.error("data_trend_task error:{}, params page:{}".format(str(e), i + 1))
                continue
            for each_dataset in search_list:
                dataset_condition = "dataset_id = '%s'" % each_dataset["data_set_id"]
                data_counts_dict = {}
                for each_metric in list(CONDITION_DICT.keys()):
                    metric_condition = CONDITION_DICT[each_metric]["metric_condition"]
                    measurement = CONDITION_DICT[each_metric]["measurement"]
                    sql = """select {} from {} where {} and time >= {} and time <= {} {} fill(previous)
                        tz('Asia/Shanghai')""".format(
                        metric_condition,
                        measurement,
                        dataset_condition,
                        start_timestamp,
                        end_timestamp,
                        group_by,
                    )
                    try:
                        ret_dict = DatamanageApi.dmonitor_metrics_query_product(
                            {"sql": sql, "database": "monitor_custom_metrics"}, raise_exception=True
                        ).data
                    except Exception as e:
                        data_inventory_logger.error(
                            "dmonitor_metrics_query error:{}, params page:{}".format(str(e), i + 1)
                        )
                        continue
                    data_counts_list = ret_dict.get("series", [])
                    for each_day in data_counts_list:
                        if each_day["time"] not in list(data_counts_dict.keys()):
                            data_counts_dict[each_day["time"]] = {}
                        if each_metric == "cost":
                            for each_capacity in SCORE_DICT["cost"]:
                                data_counts_dict[each_day["time"]][each_capacity] = each_day[each_capacity]
                        else:
                            data_counts_dict[each_day["time"]][SCORE_DICT[each_metric]] = each_day[
                                SCORE_DICT[each_metric]
                            ]

                for key, item in list(data_counts_dict.items()):
                    item["dataset_id"] = each_dataset["data_set_id"]
                    item["time"] = key
                    trend_list.append(item)

    except Exception as e:
        data_inventory_logger.exception("data trend task shut down unexpectedly, errors: [%s]" % str(e))
        return
    data_inventory_logger.info("metric_list length:{}".format(len(trend_list)))
    cache.set("data_trend", trend_list, 24 * 60 * 60 * 7)
    data_trend_df = pd.DataFrame(trend_list)
    cache.set("data_trend_df", data_trend_df, 24 * 60 * 60 * 7)

    data_inventory_logger.info("data_trend_task periodic task end, time:{time}".format(time=datetime.datetime.now()))


@shared_task(queue="datamanage_lifecycle")
def app_code_task():
    """
    获得运营数据app_code对应的中文名
    @return:
    """
    app_code_logger.info("app code task start, time:{time}".format(time=datetime.datetime.now()))
    # 获取前日日期，格式：'20200115'，用昨日日期的话，凌晨1点前若离线任务没有算完会导致热门查询没有数据
    date = get_date(2)

    sql = """SELECT app_code
                    FROM 591_dataquery_opdata_biz_rt
                    WHERE thedate={} and app_code is not null
                    GROUP BY app_code limit 1000""".format(
        date
    )
    prefer_storage = "hdfs"

    query_dict = DataqueryApi.query({"sql": sql, "prefer_storage": prefer_storage}).data
    app_code_dict = {}
    if query_dict:
        query_list = query_dict.get("list", [])
        app_code_logger.info("query_list:{}".format(query_list))
        for each_app in query_list:
            app_code = each_app.get("app_code", "")
            if app_code:
                res_dict = PaasApi.get_app_info({"app_code": app_code}).message
                if isinstance(res_dict, dict):
                    app_code_dict[app_code] = res_dict["name"]
                    app_code_logger.info("app_code:{}, app_code_alias:{}".format(app_code, res_dict["name"]))
        cache.set("app_code_dict", app_code_dict, 24 * 60 * 60 * 7)
        app_code_logger.info("app_code periodic task end, time:{time}".format(time=datetime.datetime.now()))


celery_app.conf.beat_schedule = {
    "data_value_inventory_task": {
        "task": "datamanage.pro.lifecycle.tasks.data_value_inventory_task",
        "schedule": crontab(minute="50", hour="16"),
    },
    "app_code_task": {
        "task": "datamanage.pro.lifecycle.tasks.app_code_task",
        "schedule": crontab(minute="30", hour="8"),
    },
    "data_trend_task": {
        "task": "datamanage.pro.lifecycle.tasks.data_trend_task",
        "schedule": crontab(minute="40", hour="18"),
    },
}
