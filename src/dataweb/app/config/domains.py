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
import logging

from django.conf import settings

logger = logging.getLogger("root")

PAAS_API_HOST = settings.PAAS_API_HOST
ESB_PREFIX = "/api/c/compapi/"
ESB_PREFIX_V2 = "/api/c/compapi/v2/"

# 蓝鲸平台模块接口地址
CC_APIGATEWAY_ROOT_V2 = PAAS_API_HOST + ESB_PREFIX_V2 + "cc/"
GSE_APIGATEWAY_ROOT_V2 = PAAS_API_HOST + ESB_PREFIX_V2 + "gse/"

BK_LOGIN_APIGATEWAY_ROOT = PAAS_API_HOST + ESB_PREFIX + "bk_login/"
BK_PAAS_APIGATEWAY_ROOT = PAAS_API_HOST + ESB_PREFIX + "bk_paas/"

# 数据平台模块接口地址统一前缀
BKDATA_ESB_PREFIX = PAAS_API_HOST + ESB_PREFIX + "data/v3/"

# 数据平台模块接口地址
ACCESS_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "access/"
DATAFLOW_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "dataflow/"
META_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "meta/"
DATAQUERY_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "dataquery/"
DATAMANAGE_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "datamanage/"
AUTH_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "auth/"
STOREKIT_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "storekit/"
DATABUS_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "databus/"
COLLECTOR_HUB_API_URL = BKDATA_ESB_PREFIX + "collectorhub/"
ALGO_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "algorithm/"
BKSQL_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "bksql/"
DATALAB_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "datalab/"
QUERYENGINE_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "queryengine/"
RESOURCECENTER_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "resourcecenter/"
DATACUBE_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "datacube/"

# 流程化建模
MODEL_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "model/"
AIOPS_APIGATEWAY_ROOT = BKDATA_ESB_PREFIX + "aiops/"


DOMAIN_PATCH = {"ieod": "extend.tencent.config.domains"}
if settings.RUN_VER in DOMAIN_PATCH:
    module_path = DOMAIN_PATCH[settings.RUN_VER]
    try:
        _module = __import__(module_path, globals(), locals(), ["*"])
        for _setting in dir(_module):
            if _setting == _setting.upper():
                locals()[_setting] = getattr(_module, _setting)
    except ImportError as err:
        logger.warning("Could not import domain patch file '{}' (Is it on sys.path?): {}".format(module_path, err))
