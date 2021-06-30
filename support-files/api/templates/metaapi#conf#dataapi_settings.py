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
RUN_VERSION = "__BKDATA_VERSION_TYPE__"
GEOG_AREA_CODE = "__GEOG_AREA_CODE__"  # 默认地域标签
MULTI_GEOG_AREA = __MULTI_GEOG_AREA__  # 多地域标识: True / False    # noqa

# [专属] 项目部署相关信息
APP_NAME = "meta"
pizza_port = __BKDATA_METAAPI_PORT__  # noqa
BIND_IP_ADDRESS = "__BKDATA_METAAPI_HOST__"

# [公共] 其他模块 API 参数
AUTH_API_HOST = "__BKDATA_AUTHAPI_HOST__"
AUTH_API_PORT = __BKDATA_AUTHAPI_PORT__  # noqa
ACCESS_API_HOST = "__BKDATA_ACCESSAPI_HOST__"
ACCESS_API_PORT = __BKDATA_ACCESSAPI_PORT__  # noqa
DATABUS_API_HOST = "__BKDATA_DATABUSAPI_HOST__"
DATABUS_API_PORT = __BKDATA_DATABUSAPI_PORT__  # noqa
DATAFLOW_API_HOST = "__BKDATA_DATAFLOWAPI_HOST__"
DATAFLOW_API_PORT = __BKDATA_DATAFLOWAPI_PORT__  # noqa
DATAMANAGE_API_HOST = "__BKDATA_DATAMANAGEAPI_HOST__"
DATAMANAGE_API_PORT = __BKDATA_DATAMANAGEAPI_PORT__  # noqa
DATAQUERY_API_HOST = "__BKDATA_DATAQUERYAPI_HOST__"
DATAQUERY_API_PORT = __BKDATA_DATAQUERYAPI_PORT__  # noqa
JOBNAVI_API_HOST = "__BKDATA_JOBNAVIAPI_HOST__"
JOBNAVI_API_PORT = __BKDATA_JOBNAVIAPI_PORT__  # noqa
META_API_HOST = "__BKDATA_METAAPI_HOST__"
META_API_PORT = __BKDATA_METAAPI_PORT__  # noqa
STOREKIT_API_HOST = "__BKDATA_STOREKITAPI_HOST__"
STOREKIT_API_PORT = __BKDATA_STOREKITAPI_PORT__  # noqa

# [公共] 第三方系统 URL
CC_API_URL = "http://__PAAS_FQDN__/component/compapi/cc/"
JOB_API_URL = "http://__PAAS_FQDN__/component/compapi/job/"
GSE_API_URL = "http://__PAAS_FQDN__/component/compapi/gse/"
CMSI_API_URL = "http://__PAAS_FQDN__/component/compapi/cmsi/"

# [公共] 运行时区设置
TIME_ZONE = "__BK_TIMEZONE__"

# [公共] PaaS注册之后生成的APP_ID, APP_TOKEN, BK_PAAS_HOST, SECRET_KEY
APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"
BK_PAAS_HOST = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__"
SECRET_KEY = "__DJANGO_SECRET_KEY__"

# [公共] 加解密配置
CRYPT_ROOT_KEY = "__BKDATA_ROOT_KEY__"
CRYPT_ROOT_IV = "__BKDATA_ROOT_IV__"
CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"

# [公共] Django trace设置
DO_TRACE = True
TRACING_SAMPLE_RATE = 1

# [可选] MYSQL01 配置，提供给数据集成「基础版」使用
CONFIG_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT = __MYSQL_DATAHUB_PORT__  # noqa
CONFIG_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"

# [可选] Redis 配置
REDIS_HOST = "__REDIS_HOST__"
REDIS_NAME = "__REDIS_MASTER_NAME__"
REDIS_PORT = "__REDIS_PORT__"
REDIS_PASS = "__REDIS_PASS__"

# [专属] METADATA RPC后台配置
METADATA_ACCESS_RPC_ENDPOINT_HOSTS = __BKDATA_METADATA_RPC_ENDPOINT_HOSTS__  # noqa
METADATA_ACCESS_RPC_ENDPOINT_PORT = __BKDATA_METADATA_RPC_ENDPOINT_PORT__  # noqa
METADATA_ACCESS_RPC_TIMEOUT = __BKDATA_METADATA_RPC_ENDPOINT_TIMEOUT__  # noqa

# [专属] TDW相关配置
ENABLED_TDW = __BKDATA_METADATA_ENABLED_TDW__  # noqa
LZ_URL = "http://__TDW_OPEN_HOST__/LService/QueryTask"
GAIA_INFO_MAP = __TDW_GAIA_INFO_MAP__  # noqa
GAIA_VERSIONS = __TDW_GAIA_VERSIONS__  # noqa
GAIA_VERSIONS_MAP = __TDW_GAIA_VERSIONS_MAP__  # noqa
BG_ID_INFO_MAP = __TDW_BG_ID_INFO_MAP__  # noqa

# [专属] 元数据事件系统 RabbitMQ 配置
META_EVENT_SYSTEM_NAME = "__BKDATA_META_EVENT_NAME__"
META_EVENT_RABBITMQ_USER = "__BKDATA_META_EVENT_RABBITMQ_USER__"
META_EVENT_RABBITMQ_PASS = "__BKDATA_META_EVENT_RABBITMQ_PASS__"
META_EVENT_RABBITMQ_HOST = "__BKDATA_META_EVENT_RABBITMQ_HOST__"
META_EVENT_RABBITMQ_PORT = "__BKDATA_META_EVENT_RABBITMQ_PORT__"
META_EVENT_RABBITMQ_VHOST = "__BKDATA_META_EVENT_RABBITMQ_VHOST__"

# [专属] 结果表字段数目限制
FIELDS_LIMIT = __FIELDS_LIMIT__  # noqa

# [专属] 专项业务配置
TDW_STD_BIZ_ID = __TDW_STD_BIZ_ID__  # TDW接入业务ID    # noqa
LOL_BILLING_PRJ_ID = __LOL_BILLING_PRJ_ID__  # LOL计费项目ID    # noqa

# [专属] 内建标签配置
BUILT_IN_TAGS_LIMIT = __BUILT_IN_TAGS_LIMIT__  # noqa
BUILT_IN_TAGS_TYPE_LIST = __BUILT_IN_TAGS_TYPE_LIST__  # noqa
