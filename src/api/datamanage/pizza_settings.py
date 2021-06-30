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
import logging

from conf import dataapi_settings
from pizza.settings_default import *  # noqa

# app名, 需与api部署的app name一致
APP_NAME = getattr(dataapi_settings, "APP_NAME", "")
APP_ID = getattr(dataapi_settings, "APP_ID", "")
APP_TOKEN = getattr(dataapi_settings, "APP_TOKEN", "")
APP_CODE = APP_ID
APP_SECRET = APP_TOKEN

# 当前运行在哪个运行环境
DATAMANAGEAPI_SERVICE_VERSION = dataapi_settings.DATAMANAGEAPI_SERVICE_VERSION
RUN_MODE = dataapi_settings.RUN_MODE
RUN_VERSION = dataapi_settings.RUN_VERSION

# 时区信息
TIME_ZONE = getattr(dataapi_settings, "TIME_ZONE", "Asia/Shanghai")

# 消息通知相关参数
NOTIFY_WITH_ENV = getattr(dataapi_settings, "NOTIFY_WITH_ENV", False)

KAFKA_ADDRS = {
    "KAFKA_OP_ADDR": getattr(dataapi_settings, "KAFKA_OP_ADDR", []),
    "KAFKA_COMMON_ADDR": getattr(dataapi_settings, "KAFKA_OUTER_ADDR", []),
    "KAFKA_INNER_ADDR": getattr(dataapi_settings, "KAFKA_INNER_ADDR", []),
}

DATABASES.update(  # noqa
    {
        "bkdata_basic": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_basic",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "bkdata_log": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_log",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "bkdata_basic_slave": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_basic",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER_SLAVE", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD_SLAVE", ""),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST_SLAVE", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT_SLAVE", 3306),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "bkdata_flow": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_flow",
            "USER": getattr(dataapi_settings, "FLOW_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "FLOW_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "FLOW_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "FLOW_DB_PORT", 3306),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "bkdata_jobnavi_batch": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": getattr(dataapi_settings, "JOBNAVI_BATCH_DB", "bkdata_jobnavi_batch"),
            "USER": getattr(
                dataapi_settings,
                "JOBNAVI_DB_USER",
                getattr(dataapi_settings, "FLOW_DB_USER", "root"),
            ),
            "PASSWORD": getattr(
                dataapi_settings,
                "JOBNAVI_DB_PASSWORD",
                getattr(dataapi_settings, "FLOW_DB_PASSWORD", ""),
            ),
            "HOST": getattr(
                dataapi_settings,
                "JOBNAVI_DB_HOST",
                getattr(dataapi_settings, "FLOW_DB_HOST", "127.0.0.1"),
            ),
            "PORT": getattr(
                dataapi_settings,
                "JOBNAVI_DB_PORT",
                getattr(dataapi_settings, "FLOW_DB_PORT", 3306),
            ),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
    }
)

INFLUX_RETENTION_POLICIES = {
    "autogen": {
        "name": "autogen",
        "default": True,
        "duration": "192h0m0s",
        "replication": 1,
    },
    "rp_month": {
        "name": "rp_month",
        "default": False,
        "duration": "720h0m0s",
        "replication": 1,
    },
}

DB_SETTINGS = {
    "monitor_data_metrics": {
        "host": dataapi_settings.DMONITOR_DATA_TSDB_HOST,
        "port": dataapi_settings.DMONITOR_DATA_TSDB_PORT,
        "database": "monitor_data_metrics",
        "username": dataapi_settings.DMONITOR_TSDB_USER,
        "password": dataapi_settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_custom_metrics": {
        "host": dataapi_settings.DMONITOR_CUSTOM_TSDB_HOST,
        "port": dataapi_settings.DMONITOR_CUSTOM_TSDB_PORT,
        "database": "monitor_custom_metrics",
        "username": dataapi_settings.DMONITOR_TSDB_USER,
        "password": dataapi_settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
    "monitor_performance_metrics": {
        "host": dataapi_settings.DMONITOR_PERFORMANCE_TSDB_HOST,
        "port": dataapi_settings.DMONITOR_PERFORMANCE_TSDB_PORT,
        "database": "monitor_performance_metrics",
        "username": dataapi_settings.DMONITOR_TSDB_USER,
        "password": dataapi_settings.DMONITOR_TSDB_PASS,
        "timeout": 30,
    },
}

REDIS_CONFIG = {
    "default": {
        "sentinel": False,
        "port": dataapi_settings.REDIS_PORT,
        "host": dataapi_settings.REDIS_HOST,
        "sentinel_name": dataapi_settings.REDIS_NAME,
        "password": dataapi_settings.REDIS_PASS,
    },
}

DATABASE_ROUTERS = ["datamanage.dbrouter.DbRouter"]

AUTH_API_ROOT = "http://{host}:{port}/v3/auth".format(
    host=dataapi_settings.AUTH_API_HOST, port=dataapi_settings.AUTH_API_PORT
)
CC_API_URL = dataapi_settings.CC_API_URL
PAAS_API_URL = dataapi_settings.PAAS_API_URL
DATABUS_API_ROOT = "http://{host}:{port}/v3/databus".format(
    host=dataapi_settings.DATABUS_API_HOST, port=dataapi_settings.DATABUS_API_PORT
)
META_API_ROOT = "http://{host}:{port}/v3/meta".format(
    host=dataapi_settings.META_API_HOST, port=dataapi_settings.META_API_PORT
)
DATAFLOW_API_ROOT = "http://{host}:{port}/v3/dataflow".format(
    host=dataapi_settings.DATAFLOW_API_HOST, port=dataapi_settings.DATAFLOW_API_PORT
)
JOBNAVI_API_ROOT = "http://{host}:{port}/v3/jobnavi".format(
    host=dataapi_settings.JOBNAVI_API_HOST, port=dataapi_settings.JOBNAVI_API_PORT
)
ACCESS_API_ROOT = "http://{host}:{port}/v3/access".format(
    host=dataapi_settings.ACCESS_API_HOST, port=dataapi_settings.ACCESS_API_PORT
)
DATAMANAGE_API_ROOT = "http://{host}:{port}/v3/datamanage".format(
    host=dataapi_settings.DATAMANAGE_API_HOST, port=dataapi_settings.DATAMANAGE_API_PORT
)
DATAMANAGE_API_ROOT_PRODUCT = "http://{host}:{port}/v3/datamanage".format(
    host=getattr(
        dataapi_settings,
        "DATAMANAGE_API_HOST_PRODUCT",
        dataapi_settings.DATAMANAGE_API_HOST,
    ),
    port=dataapi_settings.DATAMANAGE_API_PORT,
)
DATAQUERY_API_ROOT = "http://{host}:{port}/v3/dataquery".format(
    host=dataapi_settings.DATAQUERY_API_HOST, port=dataapi_settings.DATAQUERY_API_PORT
)
DATAQUERY_API_ROOT_PRODUCT = "http://{host}:{port}/v3/dataquery".format(
    host=getattr(
        dataapi_settings,
        "DATAQUERY_API_HOST_PRODUCT",
        dataapi_settings.DATAQUERY_API_HOST,
    ),
    port=dataapi_settings.DATAQUERY_API_PORT,
)
STOREKIT_API_ROOT = "http://{host}:{port}/v3/storekit".format(
    host=dataapi_settings.STOREKIT_API_HOST, port=dataapi_settings.STOREKIT_API_PORT
)
BKSQL_V1_API_ROOT = "http://{host}:{port}/v3/bksql/api/v1".format(
    host=dataapi_settings.BKSQL_HOST,
    port=dataapi_settings.BKSQL_PORT,
)
BKSQL_CONVERT_API_ROOT = "http://{host}:{port}/v3/bksql/api".format(
    host=dataapi_settings.BKSQL_HOST,
    port=dataapi_settings.BKSQL_PORT,
)
MODEL_API_ROOT = "http://{host}:{port}/v3/model".format(
    host=getattr(dataapi_settings, "MODEL_API_HOST"),
    port=getattr(dataapi_settings, "MODEL_API_PORT"),
)

STANDARD_PROJECT_ID = getattr(dataapi_settings, "STANDARD_PROJECT_ID", 0)

# 数据监控配置
DMONITOR_KAFKA_CLUSTER = "kafka-op"
DMONITOR_RAW_METRICS_KAFKA_TOPIC = "bkdata_data_monitor_metrics591"
TSDB_OP_ROLE_NAME = getattr(dataapi_settings, "TSDB_OP_ROLE_NAME", "op")
KAFKA_OP_ROLE_NAME = getattr(dataapi_settings, "KAFKA_OP_ROLE_NAME", "op")

# 标签相关配置
TAG_RELATED_MODELS = {
    "DatamonitorAlertConfig": {
        "target_type": "alert_config",
        "table_primary_key": "datamonitor_alert_config.id",
    },
}

INSTALLED_APPS += ("datamanage",)  # noqa

LOL_BLACK_LIST_RETRY_CNT = 3
LOL_BLACK_LIST_TIMEOUT = 7
LOL_SWITCH_REDIS_PREFIX = "cache_"

# SQL校验Api相关配置
SQL_VERIFY_CACHE_KEY = "data_manage:data_model:verifier:scope:{table}"
VALID_NUMERIC_TYPE_LIST = ["int", "float", "double", "long"]

# 按照不同版本加载版本定制化的配置
try:
    module_str = "{app_name}.extend.versions.{run_ver}.settings".format(app_name=APP_NAME, run_ver=RUN_VERSION)  # noqa
    _module = __import__(module_str, globals(), locals(), ["*"])
    for _setting in dir(_module):
        if _setting in ("DATABASES", "DB_SETTINGS", "REDIS_CONFIG"):
            locals()[_setting].update(getattr(_module, _setting))
        elif _setting == _setting.upper():
            locals()[_setting] = getattr(_module, _setting)
except ImportError as e:
    logging.error("Could not import config '{}' (Is it on sys.path?): {}".format(module_str, str(e)))

ES_SETTINGS = {}
ES_TIMEOUT = getattr(dataapi_settings, "ES_TIMEOUT", "")
ES_MAX_RESULT_WINDOW = getattr(dataapi_settings, "ES_MAX_RESULT_WINDOW", "")

if DATAMANAGEAPI_SERVICE_VERSION == "pro":
    try:
        module_str = "{app_name}.pro.pizza_settings".format(app_name=APP_NAME)
        _module = __import__(module_str, globals(), locals(), ["*"])
        for _setting in dir(_module):
            if _setting in ("DATABASES", "DB_SETTINGS", "REDIS_CONFIG", "ES_SETTINGS"):
                locals()[_setting].update(getattr(_module, _setting))
            elif _setting == _setting.upper():
                locals()[_setting] = getattr(_module, _setting)
    except ImportError as e:
        raise ImportError("Could not import config '{}' (Is it on sys.path?): {}".format(module_str, str(e)))


def patch_hash_dict_in_deepdiff():
    import deepdiff.contenthash

    def __hash_dict(self, obj, parents_ids=frozenset({})):
        if "object_id" in obj and "object_type" in obj:
            return hash(obj["object_id"])
        result = []
        obj_keys = set(obj.keys())

        for key in obj_keys:
            key_hash = self.__hash(key)
            item = obj[key]
            item_id = id(item)
            if parents_ids and item_id in parents_ids:
                continue
            parents_ids_added = self.__add_to_frozen_set(parents_ids, item_id)
            hashed = self.__hash(item, parents_ids_added)
            hashed = "{}:{}".format(key_hash, hashed)
            result.append(hashed)

        result.sort()
        result = ";".join(result)
        result = "dict:{%s}" % result

        return result

    deepdiff.contenthash.DeepHash._DeepHash__hash_dict = __hash_dict


patch_hash_dict_in_deepdiff()
