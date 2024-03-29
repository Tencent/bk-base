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

"""
模板注释说明

[公共] PIZZA框架依赖的配置，并且是各项目模块共用的配置
[可选] 平台提供的公共服务相关的链接信息，按需引入
[专属] 项目模块自身的配置，根据自身模块的情况进行修改和增减
"""

# [公共] API 启动环境
RUN_MODE = "PRODUCT"  # PRODUCT/DEVELOP
RUN_VERSION = 'ee'
DATAMANAGEAPI_SERVICE_VERSION = 'pro'

# [专属] 项目部署相关信息
pizza_port = __BKDATA_DATAMANAGEAPI_PORT__
BIND_IP_ADDRESS = "__BKDATA_DATAMANAGEAPI_HOST__"
APP_NAME = 'datamanage'

# [公共] 其他模块 API 参数
AUTH_API_HOST = "__BKDATA_AUTHAPI_HOST__"
AUTH_API_PORT = __BKDATA_AUTHAPI_PORT__
ACCESS_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
ACCESS_API_PORT = __BKDATA_DATAHUBAPI_PORT__
DATABUS_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
DATABUS_API_PORT = __BKDATA_DATAHUBAPI_PORT__
DATAFLOW_API_HOST = "__BKDATA_DATAFLOWAPI_HOST__"
DATAFLOW_API_PORT = __BKDATA_DATAFLOWAPI_PORT__
DATAMANAGE_API_HOST = "__BKDATA_DATAMANAGEAPI_HOST__"
DATAMANAGE_API_PORT = __BKDATA_DATAMANAGEAPI_PORT__
DATAQUERY_API_HOST = "__BKDATA_QUERYENGINEAPI_HOST__"
DATAQUERY_API_PORT = __BKDATA_QUERYENGINEAPI_PORT__
JOBNAVI_API_HOST = "__BKDATA_JOBNAVIAPI_HOST__"
JOBNAVI_API_PORT = __BKDATA_JOBNAVIAPI_PORT__
META_API_HOST = "__BKDATA_METAAPI_HOST__"
META_API_PORT = __BKDATA_METAAPI_PORT__
STOREKIT_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
STOREKIT_API_PORT = __BKDATA_DATAHUBAPI_PORT__
MODEL_API_HOST = "__BKDATA_MODELAPI_HOST__"
MODEL_API_PORT = "__BKDATA_MODELAPI_PORT__"
BKSQL_HOST = "__BKDATA_BKSQL_HOST__"
BKSQL_PORT = __BKDATA_BKSQL_PORT__

# [公共] 第三方系统 URL
CC_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/cc/'
JOB_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/job/'
GSE_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/gse/'
CMSI_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/cmsi/'
BK_LOGIN_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/bk_login/'

PAAS_API_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/app_api/'
PAAS_XAPI_URL = 'http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/'


# [公共] 运行时区设置
TIME_ZONE = '__BK_TIMEZONE__'

# PaaS注册之后生成的APP_ID, APP_TOKEN, BK_PAAS_HOST, SECRET_KEY
APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"
BK_PAAS_HOST = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__"
SECRET_KEY = "__DJANGO_SECRET_KEY__"

# 功能配置
SUPPORT_USER_REPORT = False

# [公共] 加解密配置
CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"

# [可选] MYSQL01 配置，提供给数据集成「基础版」使用
CONFIG_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT = __MYSQL_DATAHUB_PORT__
CONFIG_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"

CONFIG_DB_HOST_SLAVE = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT_SLAVE = __MYSQL_DATAHUB_PORT__
CONFIG_DB_USER_SLAVE = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD_SLAVE = "__MYSQL_DATAHUB_PASS__"

# [可选] MYSQL02 配置
FLOW_DB_HOST = "__MYSQL_DATAFLOW_IP0__"
FLOW_DB_PORT = __MYSQL_DATAFLOW_PORT__
FLOW_DB_USER = "__MYSQL_DATAFLOW_USER__"
FLOW_DB_PASSWORD = "__MYSQL_DATAFLOW_PASS__"

JOBNAVI_DB_HOST = "__MYSQL_JOBNAVI_IP0__"
JOBNAVI_DB_PORT = __MYSQL_JOBNAVI_PORT__
JOBNAVI_DB_USER = "__MYSQL_JOBNAVI_USER__"
JOBNAVI_DB_PASSWORD = "__MYSQL_JOBNAVI_PASS__"
JOBNAVI_BATCH_DB = 'bkdata_jobnavi'

# [可选] Redis 配置
REDIS_HOST = "__REDIS_BKDATA_HOST__"
REDIS_NAME = "__REDIS_MASTER_NAME__"
REDIS_PORT = "__REDIS_BKDATA_PORT__"
REDIS_PASS = "__REDIS_BKDATA_PASS__"

# [可选] 存储数据平台数据监控指标的时序DB
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

# [可选] 内部Kafka配置
# 数据平台清洗模块输出数据存放的KAFKA集群
KAFKA_INNER_ADDR = ["__KAFKA_INNER_HOST__:__KAFKA_PORT__"]
# 数据平台数据监控埋点信息使用的KAFKA集群
KAFKA_OP_ADDR = ["__KAFKA_OP_HOST__:__KAFKA_PORT__"]
# kafka用户集群
KAFKA_OUTER_ADDR = ["__KAFKA_OUTER_HOST__:__KAFKA_PORT__"]

# [可选] 数据平台任务埋点的KAFKA TOPIC名称（需要与STORM，AGENT，DATASVR, DATABUS任务配置的Topic对应）
BK_MONITOR_METRIC_TOPIC = "bkdata_monitor_metrics591"
BK_MONITOR_ALERT_TOPIC = "dmonitor_alerts_for_bkalert"

TSDB_OP_ROLE_NAME = "__TSDB_OP_ROLE_NAME__"
KAFKA_OP_ROLE_NAME = "__KAFKA_OP_ROLE_NAME__"

MULTI_GEOG_AREA = __MULTI_GEOG_AREA__
NOTIFY_WITH_ENV = 1

# 生命周期内容写元数据
ATLAS_RPC_HOST = "__BKDATA_METADATA_HOST__"
ATLAS_RPC_PORT = "__BKDATA_METADATA_PORT__"
ZK_ADDR = '__ZK_CONFIG_HOST__:__ZK_PORT__'
ZK_PATH = '/metadata_access_service'
METADATA_HA = False
META_ACCESS_RPC_ENDPOINT = 'http://__BKDATA_METADATA_HOST__:__BKDATA_METADATA_PORT__/jsonrpc/2.0/'

# RabbitMQ
RABBITMQ_HOST = "__RABBITMQ_HOST__"
RABBITMQ_PORT = "__RABBITMQ_PORT__"
RABBITMQ_USER = APP_ID
RABBITMQ_PASS = APP_TOKEN
RABBITMQ_VHOST = APP_ID

# es
ES_USER = "__ES_USER__"
ES_PASS = "__ES_PASS__"
