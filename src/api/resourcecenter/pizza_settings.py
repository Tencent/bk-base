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
from pizza.settings_default import INSTALLED_APPS
from conf.dataapi_settings import (
    APP_ID,
    APP_TOKEN,
    AUTH_API_HOST,
    AUTH_API_PORT,
    META_API_HOST,
    META_API_PORT,
    STOREKIT_API_HOST,
    STOREKIT_API_PORT,
    DATAFLOW_API_HOST,
    DATAFLOW_API_PORT,
    DATAQUERY_API_HOST,
    DATAQUERY_API_PORT,
    CONFIG_DB_USER,
    CONFIG_DB_PASSWORD,
    CONFIG_DB_HOST,
    CONFIG_DB_PORT,
    RESOURCECENTER_API_HOST,
    RESOURCECENTER_API_PORT,
    RESOURCE_CENTER_METRIC_STORAGE_TYPE,
    RESOURCE_CENTER_COLLECT_STORAGE_TYPES,
    STORAGE_METRIC_DATA_RT,
    RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES,
    PROCESSING_CLUSTER_QUEUE_TEMPLATE,
    YARN_QUEUE_TYPE,
    K8S_QUEUE_TYPE,
    PROCESSING_CLUSTER_QUEUE_TYPE,
    SPARK_JOB_CLUSTER_TYPES,
    SPARK_APPLICATION_TYPE,
    FLINK_JOB_CLUSTER_TYPES,
    FLINK_APPLICATION_TYPE,
    YARN_APPLICATION_TYPES,
    K8S_RESOURCE_QUOTA_URL,
    K8S_METRICS_URL,
    PROCESSING_METRIC_CALCULATION_WINDOW_LENGTH,
    PROCESSING_METRIC_DATA_RT,
    DEFAULT_RESULT_SET_LIMIT,
    MINIMUM_JOB_SUBMIT_RECORD_KEEP_TIME_DAY,
    DEFAULT_RESOURCE_GROUP_ID,
)

APP_ID = APP_ID
APP_TOKEN = APP_TOKEN
APP_NAME = "resourcecenter"
INSTALLED_APPS += (APP_NAME,)

BASE_AUTH_URL = "http://{host}:{port}/v3/auth/".format(host=AUTH_API_HOST, port=AUTH_API_PORT)
BASE_META_URL = "http://{host}:{port}/v3/meta/".format(host=META_API_HOST, port=META_API_PORT)
BASE_STOREKIT_URL = "http://{host}:{port}/v3/storekit/".format(host=STOREKIT_API_HOST, port=STOREKIT_API_PORT)
BASE_RESOURCE_CENTER_URL = "http://{host}:{port}/v3/resourcecenter/".format(
    host=RESOURCECENTER_API_HOST, port=RESOURCECENTER_API_PORT
)
BASE_DATAFLOW_URL = "http://{host}:{port}/v3/dataflow/".format(host=DATAFLOW_API_HOST, port=DATAFLOW_API_PORT)
BASE_DATAQUERY_URL = "http://{host}:{port}/v3/dataquery/".format(host=DATAQUERY_API_HOST, port=DATAQUERY_API_PORT)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_basic",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "ENCRYPTED": False,
    },
    "bkdata_basic": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_basic",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "ENCRYPTED": False,
    },
}
DATABASE_ROUTERS = ["resourcecenter.db_router.DBRouter"]

# [专属] resourcecenter api 配置
# 资源系统运营数据存储的类型
RESOURCE_CENTER_METRIC_STORAGE_TYPE = RESOURCE_CENTER_METRIC_STORAGE_TYPE
# 存储集群可以采集的类型
RESOURCE_CENTER_COLLECT_STORAGE_TYPES = RESOURCE_CENTER_COLLECT_STORAGE_TYPES
# 存储集群的运营数据结果RT
STORAGE_METRIC_DATA_RT = STORAGE_METRIC_DATA_RT
# 计算资源可以采集的集群类型
RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES = RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES
# 计算集群队列名名规则模板
PROCESSING_CLUSTER_QUEUE_TEMPLATE = PROCESSING_CLUSTER_QUEUE_TEMPLATE
# 计算集群队列名类型
YARN_QUEUE_TYPE = YARN_QUEUE_TYPE
K8S_QUEUE_TYPE = K8S_QUEUE_TYPE
PROCESSING_CLUSTER_QUEUE_TYPE = PROCESSING_CLUSTER_QUEUE_TYPE
SPARK_JOB_CLUSTER_TYPES = SPARK_JOB_CLUSTER_TYPES
SPARK_APPLICATION_TYPE = SPARK_APPLICATION_TYPE
FLINK_JOB_CLUSTER_TYPES = FLINK_JOB_CLUSTER_TYPES
FLINK_APPLICATION_TYPE = FLINK_APPLICATION_TYPE
YARN_APPLICATION_TYPES = YARN_APPLICATION_TYPES
# k8s api url
K8S_RESOURCE_QUOTA_URL = K8S_RESOURCE_QUOTA_URL
K8S_METRICS_URL = K8S_METRICS_URL
# 计算集群统计任务的窗口长度
PROCESSING_METRIC_CALCULATION_WINDOW_LENGTH = PROCESSING_METRIC_CALCULATION_WINDOW_LENGTH
# 计算集群的运营数据结果RT
PROCESSING_METRIC_DATA_RT = PROCESSING_METRIC_DATA_RT
# 默认查询结果集大小
DEFAULT_RESULT_SET_LIMIT = DEFAULT_RESULT_SET_LIMIT
# 任务提交记录最小保存时间（单位：天）
MINIMUM_JOB_SUBMIT_RECORD_KEEP_TIME_DAY = MINIMUM_JOB_SUBMIT_RECORD_KEEP_TIME_DAY
# 默认资源组ID
DEFAULT_RESOURCE_GROUP_ID = DEFAULT_RESOURCE_GROUP_ID
