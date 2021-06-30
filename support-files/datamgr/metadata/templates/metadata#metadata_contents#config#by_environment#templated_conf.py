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


import inspect
import logging
from os import path
from urllib.parse import quote

from metadata.util.os import dir_create
from metadata.vendor.simple_json_log_formatter import SimpleJsonFormatter
from metadata_biz.db_models.bkdata import basic as basic_models
from metadata_contents.config.by_environment.default_conf import DBConfig as DC
from metadata_contents.config.by_environment.default_conf import (
    DgraphBackendConfig as AC,
)
from metadata_contents.config.by_environment.default_conf import InteractorConfig as IC
from metadata_contents.config.by_environment.default_conf import IntroConfig as InC
from metadata_contents.config.by_environment.default_conf import LoggingConfig as LC
from metadata_contents.config.by_environment.default_conf import NormalConfig as NC
from metadata_contents.config.by_environment.default_conf import TaskConfig as TC

RUN_MODE = "prod"
RUN_VERSION = "__BKDATA_VERSION_TYPE__"

try:
    import socket

    hostname = socket.gethostname()
except Exception:
    hostname = ""


class LogRecordWithResultCode(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        self.result_code = 0o7000
        self.hostname = hostname
        super(LogRecordWithResultCode, self).__init__(*args, **kwargs)


logging.LogRecord = LogRecordWithResultCode


class IntroConfig(InC):
    pass


class InteractorConfig(IC):
    INTERACTOR_TIMEOUT = __BKDATA_METADATA_INTERACTOR_TIMEOUT__  # noqa


class NormalConfig(NC):
    ACCESS_RPC_SERVER_PROCESS_NUM = __BKDATA_METADATA_PROCESS_CNT__  # noqa
    ACCESS_RPC_SERVER_HOST = "__BKDATA_METADATA_HOST__"
    ACCESS_RPC_SERVER_PORT = __BKDATA_METADATA_PORT__  # noqa
    ACCESS_RPC_WSGI_CONCURRENT = __BKDATA_METADATA_RPC_WSGI_CONCURRENT__  # noqa
    ACCESS_RPC_WORKER_CONCURRENT = __BKDATA_METADATA_RPC_WORKER_CONCURRENT__  # noqa
    SERVICE_HA = __BKDATA_METADATA_SERVICE_HA__  # noqa
    BACKEND_HA = __BKDATA_METADATA_BACKEND_HA__  # noqa
    STATE_MODE = "__BKDATA_METADATA_STATE_MODE__"

    LANGUAGE = "__BKDATA_METADATA_LC__"

    AVAILABLE_BACKENDS = {
        "mysql": True,  # mysql后端不允许False。
        "dgraph": True,
    }

    AVAILABLE_BACKEND_INSTANCES = {
        "mysql": {"state": "main"},
        "dgraph": {
            "8085": {
                "ha_state": "master",
                "config": {
                    "SERVERS": [
                        "http://__DGRAPH_HOST_1__:__DGRAPH_SERVICE_PORT__",
                        "http://__DGRAPH_HOST_2__:__DGRAPH_SERVICE_PORT__",
                        "http://__DGRAPH_HOST_3__:__DGRAPH_SERVICE_PORT__",
                    ]
                },
            },
            "8086": {
                "ha_state": "slave",
                "config": {
                    "SERVERS": [
                        "http://__DGRAPH_BACKUP_HOST_1__:__DGRAPH_SERVICE_BACKUP_PORT__",
                        "http://__DGRAPH_BACKUP_HOST_2__:__DGRAPH_SERVICE_BACKUP_PORT__",
                        "http://__DGRAPH_BACKUP_HOST_3__:__DGRAPH_SERVICE_BACKUP_PORT__",
                    ]
                },
            },
        },
    }
    COLD_BACKEND_INSTANCE = {
        "dgraph": {
            "cold": {
                "config": {
                    "SERVERS": [
                        "http://__DGRAPH_BACKUP_HOST_1__:__DGRAPH_SERVICE_COLD_PORT__",
                        "http://__DGRAPH_BACKUP_HOST_1__:__DGRAPH_SERVICE_COLD_PORT__",
                        "http://__DGRAPH_BACKUP_HOST_1__:__DGRAPH_SERVICE_COLD_PORT__",
                    ]
                }
            }
        }
    }

    SYNC_TIMEOUT = __BKDATA_METADATA_SYNC_TIMEOUT__  # noqa
    STATE_WAIT_TIMEOUT = __BKDATA_METADATA_STATE_WAIT_TIMEOUT__  # noqa
    BACKEND_NODE_PENDING_QUERIES_CNT = __BKDATA_METADATA_PENDING_QUERIES_CNT__  # noqa
    BACKEND_NODE_RECOVERY_PENDING_QUERIES_CNT = __BKDATA_METADATA_RECOVERY_PENDING_QUERIES_CNT__  # noqa
    BACKEND_NODE_CONTINUOUS_UNHEALTHY_CNT = __BKDATA_METADATA_QUERY_DROP_PERCENT__  # noqa
    BACKEND_QUERY_DROP_PERCENT = __BKDATA_METADATA_CONTINUOUS_UNHEALTHY_CNT__  # noqa
    DATA_API_URL = "http://__BKDATA_API_HOST__/"

    # 功能开关
    SERVICE_SWITCHES = __BKDATA_METADATA_SERVICE_SWITCHES__  # noqa

    # 分流相关配置
    SHUNT_TOKEN_CONFIG = __BKDATA_METADATA_SHUNT_TOKEN_CONFIG__  # noqa
    TOKEN_PUBLIC_KEY = "__BKDATA_METADATA_SHUNT_TOKEN_PUBLIC_KEY__"
    TOKEN_DICT = __BKDATA_METADATA_SHUNT_TOKEN_PRIVATE_KEY__  # noqa
    # token秘钥填充占位符
    TOKEN_FILLING_CHR = "#"
    # token等级定义
    TOKEN_LEVEL_NONE = 0
    TOKEN_LEVEL_SIMPLE = 1
    TOKEN_LEVEL_COMPLEX = 2
    TOKEN_LEVEL_OFFLINE = 3

    # 过滤降级相关配置
    SUMMARIZE_METHOD = {
        "query": [
            "asset_complex_search",
            "asset_query_via_erp",
            "entity_complex_search",
            "entity_query_via_erp",
            "entity_query_lineage",
            "tag_search_targets",
        ],
        "mutate": [
            "asset_edit",
            "entity_edit",
            "bridge_sync",
        ],
    }
    # 过滤降级相关指标告警指标配置
    FILTER_METRIC_WARNING_CONF = {
        "query": {
            "enable_cnt": __BKDATA_METADATA_FILTER_QUERY_ENABLE_CNT__,  # noqa
            "fail_cnt": __BKDATA_METADATA_FILTER_QUERY_FAIL_CNT__,  # noqa
            "fail_rate": __BKDATA_METADATA_FILTER_QUERY_FAIL_RATE__,  # noqa
            "rate_factor": __BKDATA_METADATA_FILTER_QUERY_RATE_FACTOR__,  # noqa
            "cost_time_ms": __BKDATA_METADATA_FILTER_QUERY_COST_MS__,  # noqa
            "filter_limit": __BKDATA_METADATA_FILTER_QUERY_FILTER_LIMIT__,  # noqa
        },
        "mutate": {
            "enable_cnt": __BKDATA_METADATA_FILTER_MUTATE_ENABLE_CNT__,  # noqa
            "fail_cnt": __BKDATA_METADATA_FILTER_MUTATE_FAIL_CNT__,  # noqa
            "fail_rate": __BKDATA_METADATA_FILTER_MUTATE_FAIL_RATE__,  # noqa
            "rate_factor": __BKDATA_METADATA_FILTER_MUTATE_RATE_FACTOR__,  # noqa
            "cost_time_ms": __BKDATA_METADATA_FILTER_MUTATE_COST_MS__,  # noqa
            "filter_limit": __BKDATA_METADATA_FILTER_MUTATE_FILTER_LIMIT__,  # noqa
        },
    }

    # 时区配置
    TIME_ZONE = "__GEOG_AREA_TIMEZONE__"

    # 扩展配置
    ENABLED_TDW = __BKDATA_METADATA_ENABLED_TDW__  # noqa


class DBConfig(DC):
    BKDATA_META_DB_URL = "mysql+pymysql://{}:{}@{}:{}/bkdata_meta?charset=utf8".format(
        *[
            quote(item)
            for item in [
                "__MYSQL_DATAHUB_USER__",
                "__MYSQL_DATAHUB_PASS__",
                "__MYSQL_DATAHUB_IP0__",
                "__MYSQL_DATAHUB_PORT__",
            ]
        ]
    )
    BKDATA_BASIC_DB_URL = "mysql+pymysql://{}:{}@{}:{}/bkdata_basic?charset=utf8".format(
        *[
            quote(item)
            for item in [
                "__MYSQL_DATAHUB_USER__",
                "__MYSQL_DATAHUB_PASS__",
                "__MYSQL_DATAHUB_IP0__",
                "__MYSQL_DATAHUB_PORT__",
            ]
        ]
    )
    BKDATA_FLOW_DB_URL = "mysql+pymysql://{}:{}@{}:{}/bkdata_flow?charset=utf8".format(
        *[
            quote(item)
            for item in [
                "__MYSQL_DATAFLOW_USER__",
                "__MYSQL_DATAFLOW_PASS__",
                "__MYSQL_DATAFLOW_IP0__",
                "__MYSQL_DATAFLOW_PORT__",
            ]
        ]
    )
    BKDATA_MODELING_DB_URL = "mysql+pymysql://{}:{}@{}:{}/bkdata_modeling?charset=utf8".format(
        *[
            quote(item)
            for item in [
                "__MYSQL_DATAFLOW_USER__",
                "__MYSQL_DATAFLOW_PASS__",
                "__MYSQL_DATAFLOW_PASS__",
                "__MYSQL_DATAFLOW_PORT__",
            ]
        ]
    )
    BKDATA_LOG_DB_URL = "mysql+pymysql://{}:{}@{}:{}/bkdata_log?charset=utf8".format(
        *[
            quote(item)
            for item in [
                "__MYSQL_DATAHUB_USER__",
                "__MYSQL_DATAHUB_PASS__",
                "__MYSQL_DATAHUB_IP0__",
                "__MYSQL_DATAHUB_PORT__",
            ]
        ]
    )

    LOCAL_REDIS_URL = "redis://localhost:6379/0"
    REDIS_URL = "redis://[:__REDIS_PASS__]@__REDIS_HOST__:__REDIS_PORT__/"
    ZK_ADDR = "__ZK_META_HOST__:__ZK_META_PORT__"
    KAFKA_ADDR = ["__KAFKA_META_HOST__:__KAFKA_META_PORT__"]
    biz_db_urls = {
        "bkdata_basic": BKDATA_BASIC_DB_URL,
        "bkdata_flow": BKDATA_FLOW_DB_URL,
        "bkdata_modeling": BKDATA_MODELING_DB_URL,
    }

    # rabbitmq
    RABBITMQ_ADDR = "amqp://{rb_user}:{rb_pass}@{rb_host}:{rb_port}/{rb_vhost}".format(
        rb_user="__BKDATA_META_EVENT_RABBITMQ_USER__",
        rb_pass="__BKDATA_META_EVENT_RABBITMQ_PASS__",
        rb_host="__BKDATA_META_EVENT_RABBITMQ_HOST__",
        rb_port="__BKDATA_META_EVENT_RABBITMQ_PORT__",
        rb_vhost="__BKDATA_META_EVENT_RABBITMQ_VHOST__",
    )

    # crypt
    CRYPT_ROOT_KEY = "__CRYPT_KEY__"
    CRYPT_ROOT_IV = "__CRYPT_VECTOR__"
    CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"


class DgraphBackendConfig(AC):
    SERVERS = [
        "http://__DGRAPH_HOST_1__:__DGRAPH_SERVICE_PORT__",
        "http://__DGRAPH_HOST_2__:__DGRAPH_SERVICE_PORT__",
        "http://__DGRAPH_HOST_3__:__DGRAPH_SERVICE_PORT__",
    ]


class LoggingConfig(LC):
    STREAM_LEVEL = logging.WARNING
    FILE_LEVEL = logging.INFO
    MAJOR_FILE_NAME = "metadata"
    RELATIVE_PATH = "log"
    FORMAT = SimpleJsonFormatter()

    def __init__(self):
        self.suffix_postfix = None

    @property
    def file_name(self):
        return (
            "_".join((self.MAJOR_FILE_NAME, self.suffix_postfix)) if self.suffix_postfix else self.MAJOR_FILE_NAME
        ) + ".common.log"

    @property
    def file_path(self):
        return path.abspath(path.join("__LOGS_HOME__/metadata/", self.file_name))

    def setup(self):
        for attr in ["file_path"]:
            dir_create(getattr(self, attr), is_file=True)


class TaskConfig(TC):

    AVAILABLE_MODELS = {
        v.__tablename__: v
        for k, v in vars(basic_models).items()
        if inspect.isclass(v) and issubclass(v, basic_models.Base) and v is not basic_models.Base
    }
    MODEL_NAMES_TO_SYNC = []
    TABLE_NAMES_TO_SYNC = []
    MODELS_TO_SYNC = [AVAILABLE_MODELS[name] for name in MODEL_NAMES_TO_SYNC]
    TO_BATCH_RDFS_TABLE_NAMES = []
    META_API_URL = "http://__BKDATA_METAAPI_HOST__:__BKDATA_METAAPI_PORT__/"

    TABLE_NAMES_TO_GET = __TABLE_NAMES_TO_GET__  # noqa
    TABLE_NAMES_TO_META_SYNC = []
    TO_COMMON_RDFS_TABLE_NAMES = __TO_COMMON_RDFS_TABLE_NAMES__  # noqa
    TO_CUSTOM_RDFS_TABLE_NAMES = __TO_CUSTOM_RDFS_TABLE_NAMES__  # noqa
