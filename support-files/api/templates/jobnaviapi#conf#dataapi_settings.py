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

#  API启动端口
pizza_port = __BKDATA_JOBNAVIAPI_PORT__
# 绑定端口
BIND_IP_ADDRESS = "__BKDATA_JOBNAVIAPI_HOST__"
# API启动环境
RUN_MODE = "__BKDATA_RUN_MODE__"  # PRODUCT/DEVELOP
RUN_VERSION = "__BKDATA_RUN_VERSION__"
APP_NAME = 'jobnavi'

# 其他模块API参数
AUTH_API_HOST = "__BKDATA_AUTHAPI_HOST__"
AUTH_API_PORT = __BKDATA_AUTHAPI_PORT__
ACCESS_API_HOST = "__BKDATA_ACCESSAPI_HOST__"
ACCESS_API_PORT = __BKDATA_ACCESSAPI_PORT__
DATABUS_API_HOST = "__BKDATA_DATABUSAPI_HOST__"
DATABUS_API_PORT = __BKDATA_DATABUSAPI_PORT__
DATAFLOW_API_HOST = "__BKDATA_DATAFLOWAPI_HOST__"
DATAFLOW_API_PORT = __BKDATA_DATAFLOWAPI_PORT__
DATAMANAGE_API_HOST = "__BKDATA_DATAMANAGEAPI_HOST__"
DATAMANAGE_API_PORT = __BKDATA_DATAMANAGEAPI_PORT__
DATAQUERY_API_HOST = "__BKDATA_DATAQUERYAPI_HOST__"
DATAQUERY_API_PORT = __BKDATA_DATAQUERYAPI_PORT__
JOBNAVI_API_HOST = "__BKDATA_JOBNAVIAPI_HOST__"
JOBNAVI_API_PORT = __BKDATA_JOBNAVIAPI_PORT__
META_API_HOST = "__BKDATA_METAAPI_HOST__"
META_API_PORT = __BKDATA_METAAPI_PORT__
STOREKIT_API_HOST = "__BKDATA_STOREKITAPI_HOST__"
STOREKIT_API_PORT = __BKDATA_STOREKITAPI_PORT__
RESOURCECENTER_API_HOST = "__BKDATA_RESOURCECENTERAPI_HOST__"
RESOURCECENTER_API_PORT = __BKDATA_RESOURCECENTERAPI_PORT__

# [公共] 第三方系统 URL
CC_API_URL = "http://__PAAS_FQDN__/component/compapi/cc/"
JOB_API_URL = "http://__PAAS_FQDN__/component/compapi/job/"
TOF_API_URL = "http://__PAAS_FQDN__/component/compapi/tof/"
SMCS_API_URL = "http://__PAAS_FQDN__/component/compapi/smcs/"
GSE_API_URL = "http://__PAAS_FQDN__/component/compapi/gse/"
CMSI_API_URL = "http://__PAAS_FQDN__/component/compapi/cmsi/"
BK_LOGIN_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/bk_login/"
ES_LOG_API_URL = "http://__LOG_SEARCH__"

# 运行时区设置
TIME_ZONE = '__BK_TIMEZONE__'

# PaaS注册之后生成的APP_ID, APP_TOKEN, BK_PAAS_HOST
APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"
BK_PAAS_HOST = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__"
SECRET_KEY = "__DJANGO_SECRET_KEY__"

# [公共] 加解密配置
CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"
CRYPT_ROOT_KEY = "__BKDATA_ROOT_KEY__"
CRYPT_ROOT_IV = "__BKDATA_ROOT_IV__"

# config db
CONFIG_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT = __MYSQL_DATAHUB_PORT__
CONFIG_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"

# 数据开发
DATAFLOW_DB_HOST = "__MYSQL_DATAFLOW_IP0__"
DATAFLOW_DB_PORT = __MYSQL_DATAFLOW_PORT__
DATAFLOW_DB_USER = "__MYSQL_DATAFLOW_USER__"
DATAFLOW_DB_PASSWORD = "__MYSQL_DATAFLOW_PASS__"

# consul 配置
CONSUL_HOST = "127.0.0.1"
CONSUL_HTTP_PORT = __CONSUL_HTTP_PORT__
CONSUL_DC = "dc"
CONSUL_DOMAIN = "consul"

# 数据平台任务埋点的KAFKA TOPIC名称（需要与STORM，AGENT，DATASVR, DATABUS任务配置的Topic对应）
BK_MONITOR_METRIC_TOPIC = "bkdata_monitor_metrics591"
BK_MONITOR_ALERT_TOPIC = "dmonitor_alerts_for_bkalert"

MULTI_GEOG_AREA = __MULTI_GEOG_AREA__

JOBNAVI_RESOURCE_TYPE = 'schedule'
JOBNAVI_CLUSTER_TYPE = 'jobnavi'
PROCESSING_RESOURCE_TYPE = 'processing'
