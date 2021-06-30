# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from __future__ import absolute_import, print_function, unicode_literals

import os

# Enviroment settings
APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"
RUN_MODE = "PRODUCT"  # PRODUCT | DEVELOP
RUN_VERSION = "ee"
APP_NAME = "datamanagerV2"
TIMEZONE = "__BK_TIMEZONE__"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Function settings
HAS_TDW_TABLE = False

# HTTP settings
INTERAPI_POOL_SIZE = 2
INTERAPI_POOL_MAXSIZE = 10

# DataBase settings
CONFIG_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT = __MYSQL_DATAHUB_PORT__
CONFIG_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"

REDIS_HOST = "__REDIS_BKDATA_HOST__"
REDIS_NAME = "__REDIS_MASTER_NAME__"
REDIS_PORT = "__REDIS_BKDATA_PORT__"
REDIS_PASS = "__REDIS_BKDATA_PASS__"


DATABASES = {
    "basic": {
        "NAME": "bkdata_basic",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "POOL_RECYCLE": 3600,
        "POOL_SIZE": 10,
    },
    "log": {
        "NAME": "bkdata_log",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "POOL_RECYCLE": 3600,
        "POOL_SIZE": 10,
    },
}
REDIS_CONFIG = {
    "default": {"port": REDIS_PORT, "host": REDIS_HOST, "password": REDIS_PASS}
}
# Meta OperationRecords
KAFKA_ADDR = "__KAFKA_META_HOST__:__KAFKA_META_PORT__"
KAFKA_CLUSTERS = {"op": "__KAFKA_OP_HOST__:__KAFKA_PORT__"}
KAFKA_PRODUCE_MAX_MESSAGES = 1000000
KAFKA_PRODUCE_MAX_BYTES = 2097151
KAFKA_CONSUME_MAX_BYTES = 1024000
KAFKA_CONSUME_OFFSET_RESET = "latest"
KAFKA_CONSUME_COMMIT_INTERVAL = 1000

# API settings
TOF_API_URL = ""
TOF_ACCESS_TOKEN = ""

SMCS_API_URL = ""
UWORK_API_URL = ""
CMSI_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/cmsi/"
CC_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/cc/"
CMDB_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/cc/"
USERMANAGE_API_URL = (
    "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/usermanage/"
)

META_API_URL = "http://__BKDATA_METAAPI_HOST__:__BKDATA_METAAPI_PORT__/v3/meta/"
META_ACCESS_RPC_ENDPOINT = (
    "http://__BKDATA_METADATA_HOST__:__BKDATA_METADATA_PORT__/jsonrpc/2.0/"
)

APP_DATAWEB_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/o/bk_dataweb/"

DATAMANAGE_API_URL = (
    "http://__BKDATA_DATAMANAGEAPI_HOST__:__BKDATA_DATAMANAGEAPI_PORT__/v3/datamanage/"
)
ACCESS_API_URL = (
    "http://__BKDATA_DATAHUBAPI_HOST__:__BKDATA_DATAHUBAPI_PORT__/v3/access/"
)
DATABUS_API_URL = (
    "http://__BKDATA_DATAHUBAPI_HOST__:__BKDATA_DATAHUBAPI_PORT__/v3/databhus/"
)
STOREKIT_API_URL = (
    "http://__BKDATA_DATAHUBAPI_HOST__:__BKDATA_DATAHUBAPI_PORT__/v3/storekit/"
)
DATAQUERY_API_URL = (
    "http://__BKDATA_QUERYENGINE_HOST__:__BKDATA_QUERYENGINE_PORT__/v3/dataquery/"
)
DATAQUERY_ENGINE_API_URL = (
    "http://__BKDATA_QUERYENGINE_HOST__:__BKDATA_QUERYENGINE_PORT__/v3/queryengine/"
)
DATAFLOW_API_URL = (
    "http://__BKDATA_DATAFLOWAPI_HOST__:__BKDATA_DATAFLOWAPI_PORT__/v3/dataflow/"
)
AUTH_API_URL = "http://__BKDATA_AUTHAPI_HOST__:__BKDATA_AUTHAPI_PORT__/v3/auth/"


# 待规范化
metadata_settings = {
    "METADATA_HA": False,
    "ZK_ADDR": "__ZK_META_HOST__:__ZK_PORT__",
    "META_ACCESS_RPC_ENDPOINT": META_ACCESS_RPC_ENDPOINT,
    "BK_APP_CODE": APP_ID,
    "BK_APP_SECRET": APP_TOKEN,
    "CC_V1_URL": "",
    "CC_V3_URL": "{CC_API_URL}/search_business/".format(CC_API_URL=CC_API_URL),
}

# Audit Settings
PLAT_RECEIVERS = []
TEST_RECEIVERS = []

# DM_ENGINE settings
DM_ENGINE_TIMEZONE = TIMEZONE
DM_ENGINE_LOG_LEVEL = "INFO"
DM_ENGINE_LOG_DIR = "__LOGS_HOME__/datamanagerv2/"
DM_ENGINE_DB_CONFIG = {
    "default": {
        "db_name": "bkdata_basic",
        "db_user": CONFIG_DB_USER,
        "db_password": CONFIG_DB_PASSWORD,
        "db_host": CONFIG_DB_HOST,
        "db_port": CONFIG_DB_PORT,
        "charset": "utf8",
    }
}
DM_ENGINE_REDIS_CONFIG = {
    "default": {"port": REDIS_PORT, "host": REDIS_HOST, "password": REDIS_PASS}
}

# iam
SUPPORT_IAM = True
IAM_REGISTERED_SYSTEM = "__APP_CODE__"
IAM_REGISTERED_APP_CODE = "__APP_CODE__"
IAM_REGISTERED_APP_SECRET = "__APP_TOKEN__"
IAM_API_HOST = "http://__BKIAM_HOST__:__BKIAM_PORT__"
IAM_API_PAAS_HOST = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__"
BK_DATA_URL = (
    "http://__BKDATA_AUTHAPI_HOST__:__BKDATA_AUTHAPI_PORT__/v3/auth/core_config/"
)

# 存储数据平台数据监控指标的时序DB
DMONITOR_TSDB_HOST = "__BKDATA_INFLUXDB_PROXY_HOST__"
DMONITOR_TSDB_PORT = __BKDATA_INFLUXDB_PROXY_PORT__
DMONITOR_TSDB_USER = "__INFLUXDB_BKDATA_USER__"
DMONITOR_TSDB_PASS = "__INFLUXDB_BKDATA_PASS__"

DMONITOR_DATA_TSDB_HOST = "__BKDATA_INFLUXDB_PROXY_HOST__"
DMONITOR_DATA_TSDB_PORT = __BKDATA_INFLUXDB_PROXY_PORT__
DMONITOR_CUSTOM_TSDB_HOST = "__BKDATA_INFLUXDB_PROXY_HOST__"
DMONITOR_CUSTOM_TSDB_PORT = __BKDATA_INFLUXDB_PROXY_PORT__
DMONITOR_PERFORMANCE_TSDB_HOST = "__BKDATA_INFLUXDB_PROXY_HOST__"
DMONITOR_PERFORMANCE_TSDB_PORT = __BKDATA_INFLUXDB_PROXY_PORT__

INFLUXDB_CONFIG = {
    "monitor_data_metrics": {
        "host": DMONITOR_DATA_TSDB_HOST,
        "port": DMONITOR_DATA_TSDB_PORT,
        "database": "monitor_data_metrics",
        "username": DMONITOR_TSDB_USER,
        "password": DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_custom_metrics": {
        "host": DMONITOR_CUSTOM_TSDB_HOST,
        "port": DMONITOR_CUSTOM_TSDB_PORT,
        "database": "monitor_custom_metrics",
        "username": DMONITOR_TSDB_USER,
        "password": DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_performance_metrics": {
        "host": DMONITOR_PERFORMANCE_TSDB_HOST,
        "port": DMONITOR_PERFORMANCE_TSDB_PORT,
        "database": "monitor_performance_metrics",
        "username": DMONITOR_TSDB_USER,
        "password": DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
}

# 数据足迹相关配置
ES_HOST = "__ES_HOST__"
ES_PORT = __ES_REST_PORT__
ES_USER = "__ES_USER__"
ES_PASS = "__ES_PASS__"

DATAMAP_DICT_TOKEN_PKEY = "datamap_dict"
LIFE_CYCLE_TOKEN_PKEY = "life_cycle"

# 加解密配置
CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"

# [可选] ElasticSearch (数据足迹流水)配置
DATA_TRACE_ES_API_HOST = "__BKDATA_TRACE_ES_API_HOST__"
DATA_TRACE_ES_API_PORT = __BKDATA_TRACE_ES_API_PORT__
DATA_TRACE_ES_PASS_TOKEN = "__BKDATA_TRACE_ES_PASS_TOKEN__"
DATA_TRACE_ES_API_ADDR = "http://{}:{}".format(
    DATA_TRACE_ES_API_HOST, DATA_TRACE_ES_API_PORT
)
ES_SETTINGS = {
    "data_trace": {
        "host": DATA_TRACE_ES_API_HOST,
        "port": DATA_TRACE_ES_API_PORT,
        "user": ES_USER,
        "token": DATA_TRACE_ES_PASS_TOKEN,
    },
}
ES_TIMEOUT = 10

DATA_TRACE_ES_INDEX_NAME = "data_trace_log"
DATA_TRACE_ES_TYPE_NAME = "data_trace"
DATA_TRACE_ES_NAME = "data_trace"
