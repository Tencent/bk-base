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

from pizza.settings_default import *  # noqa
from pizza.settings_default import DATABASES, INSTALLED_APPS, dataapi_settings

INSTALLED_APPS += ("meta", "meta.tag")

ALLOWED_HOSTS = ["*"]
DATABASE_ROUTERS = ["meta.dbrouter.DbRouter"]

INIT_PROJECTS = [1, 2, 3, 4]

# 资源池配置
INTERAPI_POOL_SIZE = getattr(dataapi_settings, "INTERAPI_POOL_SIZE", 5)
INTERAPI_POOL_MAXSIZE = getattr(dataapi_settings, "INTERAPI_POOL_MAXSIZE", 20)

# DB配置
DATABASES.update(
    {
        "bkdata_basic": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_basic",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
            "TEST": {
                "NAME": "bkdata_basic",
            },
            "CONN_MAX_AGE": 3600,
        },
        "bkdata_log": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_log",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
            "TEST": {
                "NAME": "bkdata_basic",
            },
            "CONN_MAX_AGE": 3600,
        },
    }
)

# 模块业务配置
METADATA_ACCESS_RPC_ENDPOINT_TEMPLATE = "http://{METADATA_RPC_HOST}:{METADATA_RPC_PORT}/jsonrpc/2.0/"
METADATA_ACCESS_RPC_ENDPOINT_HOSTS = getattr(dataapi_settings, "METADATA_ACCESS_RPC_ENDPOINT_HOSTS", [])
METADATA_ACCESS_RPC_ENDPOINT_PORT = getattr(dataapi_settings, "METADATA_ACCESS_RPC_ENDPOINT_PORT", 80)
METADATA_ACCESS_RPC_TIMEOUT = getattr(dataapi_settings, "METADATA_ACCESS_RPC_TIMEOUT", 120)

# 通用redis缓存配置
REDIS_CONFIG = {
    "default": {
        "sentinel": False,
        "port": dataapi_settings.REDIS_PORT,
        "host": dataapi_settings.REDIS_HOST,
        "sentinel_name": dataapi_settings.REDIS_NAME,
        "password": dataapi_settings.REDIS_PASS,
    }
}

# 元数据事件系统[名称]
META_EVENT_SYSTEM_NAME = getattr(dataapi_settings, "META_EVENT_SYSTEM_NAME", "meta_event_system")
# 元数据事件系统[Rabbitmq配置]
RABBITMQ_ADDR = "amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{RABBITMQ_VHOST}".format(
    RABBITMQ_USER=getattr(dataapi_settings, "META_EVENT_RABBITMQ_USER", "user"),
    RABBITMQ_PASS=getattr(dataapi_settings, "META_EVENT_RABBITMQ_PASS", "pass"),
    RABBITMQ_HOST=getattr(dataapi_settings, "META_EVENT_RABBITMQ_HOST", "localhost"),
    RABBITMQ_PORT=getattr(dataapi_settings, "META_EVENT_RABBITMQ_PORT", 80),
    RABBITMQ_VHOST=getattr(dataapi_settings, "META_EVENT_RABBITMQ_VHOST", ""),
)

# 元数据结果表字段数目限制
FIELDS_LIMIT = getattr(dataapi_settings, "FIELDS_LIMIT", 1500)

# TDW[地址]
LZ_URL = getattr(dataapi_settings, "LZ_URL", "")
# TDW[开关]
ENABLED_TDW = getattr(dataapi_settings, "ENABLED_TDW", False)
GAIA_INFO_MAP = getattr(dataapi_settings, "GAIA_INFO_MAP", {})
GAIA_VERSIONS = getattr(dataapi_settings, "GAIA_VERSIONS", {})
GAIA_VERSIONS_MAP = getattr(dataapi_settings, "GAIA_VERSIONS_MAP", {})
BG_ID_INFO_MAP = getattr(dataapi_settings, "BG_ID_INFO_MAP", {})

# 标签[目标类型和模型名称映射关系]
TAG_RELATED_MODELS = {
    "ResultTable": {"target_type": "result_table", "table_primary_key": "result_table.result_table_id"},
    "AccessRawData": {"target_type": "raw_data", "table_primary_key": "access_raw_data.id"},
    "DataProcessing": {"target_type": "data_processing", "table_primary_key": "data_processing.processing_id"},
    "DataTransferring": {"target_type": "data_transferring", "table_primary_key": "data_transferring.transferring_id"},
    "Project": {"target_type": "project", "table_primary_key": "project_info.project_id"},
    "ClusterGroupConfig": {
        "target_type": "cluster_group",
        "table_primary_key": "cluster_group_config.cluster_group_id",
    },
    "StorageClusterConfig": {"target_type": "storage_cluster", "table_primary_key": "storage_cluster_config.id"},
    "DatabusChannelClusterConfig": {
        "target_type": "channel_cluster",
        "table_primary_key": "databus_channel_cluster_config.id",
    },
    "DatabusConnectorClusterConfig": {
        "target_type": "connector_cluster",
        "table_primary_key": "databus_connector_cluster_config.id",
    },
    "ProcessingClusterConfig": {
        "target_type": "processing_cluster",
        "table_primary_key": "processing_cluster_config.id",
    },
    "DmStandardConfig": {"target_type": "standard", "table_primary_key": "dm_standard_config.id"},
    "DmStandardContentConfig": {
        "target_type": ("detail_data", "indicator"),
        "table_primary_key": "dm_standard_content_config.id",
    },
    "DataflowJobNaviClusterConfig": {
        "target_type": "jobnavi_cluster",
        "table_primary_key": "dataflow_jobnavi_cluster_config.id",
    },
    "DatamonitorAlertConfig": {"target_type": "alert_config", "table_primary_key": "datamonitor_alert_config.id"},
    "AlgorithmBaseModel": {
        "target_type": "algorithm_base_model",
        "table_primary_key": "algorithm_base_model.model_id",
    },
    "SampleSet": {"target_type": "sample_set", "table_primary_key": "sample_set.sample_set_id"},
    "AlgorithmModelInstance": {
        "target_type": "algorithm_model_instance",
        "table_primary_key": "algorithm_model_instance.instance_id",
    },
    "DmmModelInfo": {"target_type": "data_model", "table_primary_key": "dmm_model_info.model_id"},
}
# 标签[内建标签id最大范围, 此范围之后分配给用户自定义标签]
BUILT_IN_TAGS_LIMIT = getattr(dataapi_settings, "BUILT_IN_TAGS_LIMIT", 1000000)
# 标签[内建标签类型枚举配置]
BUILT_IN_TAGS_TYPE_LIST = getattr(dataapi_settings, "BUILT_IN_TAGS_TYPE_LIST", ["manage"])

# 多地域标志
MULTI_GEOG_AREA = getattr(dataapi_settings, "MULTI_GEOG_AREA", False)

# 特定业务[TDW]
TDW_STD_BIZ_ID = getattr(dataapi_settings, "TDW_STD_BIZ_ID", 5000139)
# 特定业务[LOL billing]
LOL_BILLING_PRJ_ID = getattr(dataapi_settings, "LOL_BILLING_PRJ_ID", 7550)

# 可以访问的存储列表配置
ENABLE_QUERY_STORAGE_LIST = [
    "ignite",
    "druid",
    "mysql",
    "tdw",
    "tsdb",
    "es",
    "tpg",
    "hdfs",
    "postgresql",
    "presto",
    "tspider",
    "hermes",
]
