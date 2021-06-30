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
from conf import settings


GEOG_AREA_CODE = getattr(settings, "GEOG_AREA_CODE", "inland")


META_CACHE_CONFIGS = {
    "data_set": {
        "key_sets": "dmonitor_data_set_sets",  # 缓存数据key的集合
        "key_template": "dmonitor_data_set_{}",  # 缓存中存放元数据配置的key模板
        "primary_key": "data_set_id",
    },
    "data_operation": {
        "key_sets": "dmonitor_data_operation_sets",
        "key_template": "dmonitor_data_operation_{}",
        "primary_key": "data_operation_id",
    },
    "flow": {
        "key_sets": "dmonitor_flow_sets",
        "key_template": "dmonitor_flow_{}",
        "primary_key": "flow_id",
    },
    "dataflow": {
        "key_sets": "dmonitor_dataflow_sets",
        "key_template": "dmonitor_dataflow_{}",
        "primary_key": "flow_id",
    },
}


BIG_DIMENSIONS = ["local_ip", "physical_tag"]


DMONITOR_TOPICS = {
    "data_cleaning": "bkdata_monitor_metrics591",
    "dmonitor_data_loss": "dmonitor_loss_audit",
    "dmonitor_batch_data_loss": "dmonitor_batch_loss_audit",
    "dmonitor_data_drop": "dmonitor_drop_audit",
    "dmonitor_data_delay": "dmonitor_delay_audit",
    "dmonitor_output_total": "dmonitor_output_total",
    "dmonitor_batch_output_total": "dmonitor_batch_output_total",
    "data_loss_metric": "data_loss_metric",
    "data_delay_metric": "data_delay_metric",
    "data_drop_metric": "data_drop_metric",
    "dmonitor_alerts": "dmonitor_alerts",
    "data_io_total": "data_io_total",
    "dmonitor_raw_alert": "dmonitor_raw_alert",
}


STORAGE_COMPONENT_NODE_TYPES = {
    "tspider": "tspider_storage",
    "es": "elastic_storage",
    "druid": "druid_storage",
    "hdfs": "hdfs_storage",
    "hermes": "hermes_storage",
    "mysql": "mysql_storage",
    "queue": "queue_storage",
    "tredis": "tredis_storage",
    "tsdb": "tsdb_storage",
    "postgresql": "pgsql_storage",
    "tpg": "tpg",
    "ignite": "ignite",
}


TSDB_OP_ROLE_NAME = getattr(settings, "TSDB_OP_ROLE_NAME", "op")
KAFKA_OP_ROLE_NAME = getattr(settings, "KAFKA_OP_ROLE_NAME", "op")
KAFKA_OUTER_ROLE_NAME = getattr(settings, "KAFKA_OUTER_ROLE_NAME", "outer")

INFLUXDB_CONFIG = {
    "monitor_data_metrics": {
        "host": settings.DMONITOR_DATA_TSDB_HOST,
        "port": settings.DMONITOR_DATA_TSDB_PORT,
        "database": "monitor_data_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_custom_metrics": {
        "host": settings.DMONITOR_CUSTOM_TSDB_HOST,
        "port": settings.DMONITOR_CUSTOM_TSDB_PORT,
        "database": "monitor_custom_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_performance_metrics": {
        "host": settings.DMONITOR_PERFORMANCE_TSDB_HOST,
        "port": settings.DMONITOR_PERFORMANCE_TSDB_PORT,
        "database": "monitor_performance_metrics",
        "username": settings.DMONITOR_TSDB_USER,
        "password": settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
}

INFLUX_RETENTION_POLICIES = {
    "autogen": {
        "name": "autogen",
        "default": True,
        "duration": "192h0m0s",
        "replication": 1,
    },
    "rp_month": {
        "name": "rp_month",
        "default": False,
        "duration": "720h0m0s",
        "replication": 1,
    },
}
