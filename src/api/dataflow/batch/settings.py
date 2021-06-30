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

from conf import dataapi_settings
from pizza import settings_default

from dataflow import pizza_settings

DATA_TIME_ZONE = dataapi_settings.DATA_TIME_ZONE

CODE_VERSION_DEFAULT = "0.1.0"
CLUSTER_GROUP_DEFAULT = "default"
CLUSTER_NAME_DEFAULT = "root.dataflow.batch.default"
DEBUG_QUEUE = "root.dataflow.batch.debug"

DEPLOY_MODE_DEFAULT = "yarn"
DEPLOY_CONFIG_DEFAULT = "{}"
JOBSERVER_CONFIG_DEFAULT = "default"
COMPONENT_TYPE_DEFAULT = "spark"
CODECHECK_BLACKLIST_GROUP = "py_spark_1"

HDFS_DATA_HOME = "/kafka/data"
DIR_API_OUT = "/api/outdir"
HDFS_FLOW_HOME = "/api/flow"
DIR_HDFS_LOCATION = "/api/tmp"
QUERY_NAMESPACE = "/api/query"
offline_reserved_fields = {"dtEventTime": "string", "dtEventTimeStamp": "timestamp"}
databus_max_retry_times = 3
tspider_job_max_retry_times = 5

hdfs_max_retry_times = 3
HDFS_BIN_DIR = dataapi_settings.HDFS_BIN_DIR
HDFS_BACKUP_DIR = dataapi_settings.HDFS_BACKUP_DIR
HDFS_EXPIRE_TIME = 7

# other api
PRODUCT_MODE_URL = {
    "deployment.collector.api": "%s/api/c/compapi/data/databus" % dataapi_settings.BK_PAAS_HOST,
    "trt.api": "%s/api/c/compapi/data/trt" % dataapi_settings.BK_PAAS_HOST,
    "tool.api": "%s/api/c/compapi/data/tool" % dataapi_settings.BK_PAAS_HOST,
}
URL_DICT = PRODUCT_MODE_URL
timezone = dataapi_settings.TIME_ZONE
# PASS
PASS_APP_CODE = dataapi_settings.APP_ID
PASS_APP_TOKEN = dataapi_settings.APP_TOKEN
PASS_BK_HOST = dataapi_settings.BK_PAAS_HOST
PASS_APP_NAME = "蓝鲸"
PASS_ACCOUNT = "root"
BKDATA_DIR = settings_default.BASE_DIR

TDW_PREFIX = pizza_settings.TDW_PREFIX
TDW_SPARK_TYPE = pizza_settings.TDW_SPARK_TYPE
TDW_DESCRIPTION = pizza_settings.TDW_DESCRIPTION
TDW_BASE_FILE_NAME = pizza_settings.TDW_BASE_FILE_NAME
TDW_BASE_FILE_MD5 = pizza_settings.TDW_BASE_FILE_MD5
TDW_JAR_FILE_NAME = pizza_settings.TDW_JAR_FILE_NAME
TDW_CLASS_NAME = pizza_settings.TDW_CLASS_NAME
TDW_JAR_CLASS_NAME = pizza_settings.TDW_JAR_CLASS_NAME
TDW_DRIVER_MEMORY = pizza_settings.TDW_DRIVER_MEMORY
TDW_EXECUTOR_CORES = pizza_settings.TDW_EXECUTOR_CORES
TDW_NUM_EXECUTORS = pizza_settings.TDW_NUM_EXECUTORS
TDW_EXECUTOR_MEMORY = pizza_settings.TDW_EXECUTOR_MEMORY
TDW_UC_JAR_FILE_PATH = dataapi_settings.TDW_UC_JAR_FILE_PATH
TDW_BATCH_LZ_ADMIN_USERS = dataapi_settings.TDW_BATCH_LZ_ADMIN_USERS
TDW_LOG_SERVER = dataapi_settings.TDW_LOG_SERVER
TDW_TASK_NODE_LABEL = dataapi_settings.TDW_TASK_NODE_LABEL

DEPLOY_CONFIG_RESOURCE = [
    "executor.memory",
    "executor.cores",
    "executor.memoryOverhead",
]
RESULT_TABLE_TYPE_MAP = {
    "stream": "stream",
    "clean": "stream",
    "batch_model": "stream",
    "stream_model": "stream",
    "transform": "stream",
    "storage": "stream",
    "batch": "batch",
}

SPARK_SQL_NODE_LABEL = dataapi_settings.SPARK_SQL_NODE_LABEL
SPARK_SQL_DEFAULT_CLUSTER_GROUP = pizza_settings.SPARK_SQL_DEFAULT_CLUSTER_GROUP
SPARK_SQL_CLUSTER_NAME_PREFIX = pizza_settings.SPARK_SQL_CLUSTER_NAME_PREFIX
SPARK_SQL_NODE_LABEL_DEFAULT_LIST = dataapi_settings.SPARK_SQL_NODE_LABEL_DEFAULT_LIST
SPARK_SQL_UDF_ENGINE_CONF = pizza_settings.SPARK_SQL_UDF_ENGINE_CONF
SPARK_SQL_TEXT_MAX_LENGTH = pizza_settings.SPARK_SQL_TEXT_MAX_LENGTH

LIVY_NODE_LABEL = dataapi_settings.LIVY_NODE_LABEL
LIVY_DEFAULT_CLUSTER_GROUP = dataapi_settings.LIVY_DEFAULT_CLUSTER_GROUP
LIVY_CLUSTER_NAME_PREFIX = dataapi_settings.LIVY_CLUSTER_NAME_PREFIX
