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
from conf.dataapi_settings import (
    BK_MONITOR_ALERT_TOPIC,
    BK_MONITOR_METRIC_TOPIC,
    CONFIG_DB_HOST,
    CONFIG_DB_PASSWORD,
    CONFIG_DB_PORT,
    CONFIG_DB_USER,
    DATAFLOW_DB_HOST,
    DATAFLOW_DB_PASSWORD,
    DATAFLOW_DB_PORT,
    DATAFLOW_DB_USER,
    DATAMANAGE_API_HOST,
    DATAMANAGE_API_PORT,
    JOBNAVI_CLUSTER_TYPE,
    JOBNAVI_RESOURCE_TYPE,
    MULTI_GEOG_AREA,
    PROCESSING_RESOURCE_TYPE,
    RESOURCECENTER_API_HOST,
    RESOURCECENTER_API_PORT,
)

DATA_UPLOAD_MAX_MEMORY_SIZE = 300 * 1024 * 1024

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_flow",
        "USER": DATAFLOW_DB_USER,
        "PASSWORD": DATAFLOW_DB_PASSWORD,
        "HOST": DATAFLOW_DB_HOST,
        "PORT": DATAFLOW_DB_PORT,
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

JOBNAVI_APPS = ["jobnavi"]

DATABASE_ROUTERS = ["jobnavi.db_router.DBRouter"]

# 支持标签应用
TAG_RELATED_MODELS = {
    "DataflowJobNaviClusterConfig": {
        "target_type": "jobnavi_cluster",
        "table_primary_key": "dataflow_jobnavi_cluster_config.id",
    }
}

BASE_DATAMANAGE_URL = "http://{host}:{port}/v3/datamanage/".format(host=DATAMANAGE_API_HOST, port=DATAMANAGE_API_PORT)

BASE_RESOURCECENTER_URL = "http://{host}:{port}/v3/resourcecenter/".format(
    host=RESOURCECENTER_API_HOST, port=RESOURCECENTER_API_PORT
)

BK_MONITOR_METRIC_TOPIC = BK_MONITOR_METRIC_TOPIC
BK_MONITOR_ALERT_TOPIC = BK_MONITOR_ALERT_TOPIC

MULTI_GEOG_AREA = MULTI_GEOG_AREA

JOBNAVI_RESOURCE_TYPE = JOBNAVI_RESOURCE_TYPE
JOBNAVI_CLUSTER_TYPE = JOBNAVI_CLUSTER_TYPE
PROCESSING_RESOURCE_TYPE = PROCESSING_RESOURCE_TYPE

DEFAULT_GEOG_AREA_CODE = "inland"
if hasattr(dataapi_settings, "DEFAULT_GEOG_AREA_CODE"):
    DEFAULT_GEOG_AREA_CODE = getattr(dataapi_settings, "DEFAULT_GEOG_AREA_CODE")


# 加载扩展模块配置
class ExtendSettings(object):
    pass


EXTEND_SETTINGS = ExtendSettings()

EXTENDED = False
if hasattr(dataapi_settings, "EXTENDED"):
    EXTENDED = getattr(dataapi_settings, "EXTENDED")
if EXTENDED:
    from jobnavi.extend import extend_settings

    for setting in dir(extend_settings):
        if setting.isupper():
            setattr(EXTEND_SETTINGS, setting, getattr(extend_settings, setting))
