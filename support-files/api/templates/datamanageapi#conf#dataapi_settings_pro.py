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

CELERY_TIMEZONE = '__BK_TIMEZONE__'

# [可选] Redis 配置
REDIS_HOST = "__REDIS_BKDATA_HOST__"
REDIS_NAME = "__REDIS_MASTER_NAME__"
REDIS_PORT = "__REDIS_BKDATA_PORT__"
REDIS_PASS = "__REDIS_BKDATA_PASS__"

STANDARD_PROJECT_ID = 4172

# 生命周期内容写元数据
ATLAS_RPC_HOST = "__BKDATA_METADATA_HOST__"
ATLAS_RPC_PORT = "__BKDATA_METADATA_PORT__"
ZK_ADDR = '__ZK_CONFIG_HOST__:__ZK_PORT__'
ZK_PATH = '/metadata_access_service'
METADATA_HA = False
META_ACCESS_RPC_ENDPOINT = 'http://__BKDATA_METADATA_HOST__:__BKDATA_METADATA_PORT__/jsonrpc/2.0/'

APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"

# [可选] RabbitMQ
RABBITMQ_HOST = "__RABBITMQ_HOST__"
RABBITMQ_PORT = "__RABBITMQ_PORT__"
RABBITMQ_USER = APP_ID
RABBITMQ_PASS = APP_TOKEN
RABBITMQ_VHOST = APP_ID

ES_HOST = "__ES_HOST__"
ES_PORT = __ES_REST_PORT__
ES_USER = "__ES_USER__"
ES_PASS = "__ES_PASS__"

# [可选] ElasticSearch (数据足迹流水)配置
DATA_TRACE_ES_API_HOST = "__BKDATA_TRACE_ES_API_HOST__"
DATA_TRACE_ES_API_PORT = __BKDATA_TRACE_ES_API_PORT__
DATA_TRACE_ES_PASS_TOKEN = "__BKDATA_TRACE_ES_PASS_TOKEN__"
ES_SETTINGS = {
    'data_trace': {
        'host': DATA_TRACE_ES_API_HOST,
        'port': DATA_TRACE_ES_API_PORT,
        'user': ES_USER,
        'token': DATA_TRACE_ES_PASS_TOKEN,
    },
}

DATAMANAGEAPI_SERVICE_VERSION = 'pro'
# 数据修正调试虚拟表
DEBUG_VIRTUAL_TABLE = '591_virtual_table'
PARQUET_FILE_TMP_FOLDER = "__BK_HOME__/public/bkdata/datamanageapi"

ASSET_VALUE_SCORE = 4.37
