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
from os import path

from metadata.util.conf import BaseConfig
from metadata.util.os import dir_create
from metadata.vendor.simple_json_log_formatter import SimpleJsonFormatter


class IntroConfig(BaseConfig):
    DATA_PATH = path.abspath('var')

    def setup(self):
        for attr in ['DATA_PATH']:
            dir_create(getattr(self, attr))


class NormalConfig(BaseConfig):
    ACCESS_RPC_SERVER_PROCESS_NUM = 4
    ACCESS_RPC_SERVER_HOST = "0.0.0.0"
    ACCESS_RPC_SERVER_PORT = 5000
    ACCESS_RPC_SERVER_AUTH = [{'StephenKing': 'Shine'}]
    ACCESS_RPC_WSGI_CONCURRENT = 500
    ACCESS_RPC_WORKER_CONCURRENT = 500
    HTTP_POOL_SIZE = 50
    HTTP_POOL_MAX_SIZE = 100

    LANGUAGE = 'zh_CN'
    AVAILABLE_BACKENDS = {
        'mysql': True,  # mysql后端不允许False。
        'dgraph': True,
    }

    SERVICE_HA = False
    BACKEND_HA = False
    STATE_MODE = 'local'
    STATE_WAIT_TIMEOUT = 10

    BACKEND_NODE_PENDING_QUERIES_CNT = 1000
    BACKEND_NODE_RECOVERY_PENDING_QUERIES_CNT = 100
    BACKEND_NODE_CONTINUOUS_UNHEALTHY_CNT = 3
    BACKEND_STATUS_CACHE_TIME = 300
    BACKEND_STATUS_CACHE_LENGTH = 120
    BACKEND_STATUS_DETECT_RANGE = 5
    BACKEND_QUERY_DROP_PERCENT = 0.7

    AVAILABLE_BACKEND_INSTANCES = {
        'mysql': {'state': 'main'},
        'dgraph': {
            '8085': {'ha_state': 'master', 'config': {'SERVERS': ["http://127.0.0.1:8085"]}},
            '8086': {'ha_state': 'slave', 'config': {'SERVERS': ["http://127.0.0.1:8086"]}},
        },
    }
    COLD_BACKEND_INSTANCE = {'dgraph': {'cold': {'config': {'SERVERS': ["http://127.0.0.1:8085"]}}}}

    DATA_API_URL = 'http://127.0.0.1/'

    # 功能开关
    SERVICE_SWITCHES = {}

    # 分流相关配置
    SHUNT_TOKEN_CONFIG = {}
    TOKEN_DICT = {}
    TOKEN_FILLING_CHR = '#'

    # 过滤降级相关配置
    SUMMARIZE_METHOD = {}
    FILTER_METRIC_WARNING_CONF = {}

    TIME_ZONE = ''

    # 批量导入预置，单事务同步写入超过该数目自动开启BATCH模式
    BATCH_AUTO_ENABLE = 50

    # 扩展配置
    ENABLED_TDW = False

    @property
    def auth_api_url(self):
        return self.DATA_API_URL + 'v3/auth/'

    @property
    def meta_api_url(self):
        return self.DATA_API_URL + 'v3/meta/'


class InteractorConfig(BaseConfig):
    RELATIVE_PATH = 'interactor'
    COMMIT_TIMEOUT = 5
    KAFKA_FAIL_RESET_CNT = 5
    INTERACTOR_TIMEOUT = 360
    KAFKA_TRANSACTIONAL_OPERATION_TOPIC = 'transactional_operation_records'

    @property
    def persistence_file_path(self):
        return path.abspath(
            path.join(
                self.config_collection.intro_config.DATA_PATH,
                self.RELATIVE_PATH,
                'persistence',
                'operate_records_persistence.txt',
            )
        )

    @property
    def local_persistence_file_path(self):
        return path.abspath(
            path.join(
                self.config_collection.intro_config.DATA_PATH,
                self.RELATIVE_PATH,
                'persistence',
                'operate_records_local_persistence.txt',
            )
        )

    @property
    def persistence_offset(self):
        return path.abspath(
            path.join(self.config_collection.intro_config.DATA_PATH, self.RELATIVE_PATH, 'persistence_offset')
        )

    def setup(self):
        for attr in ['persistence_file_path', 'persistence_offset']:
            dir_create(getattr(self, attr), is_file=True)


class DBConfig(BaseConfig):
    MAPLE_LEAF_DB_URL = ''
    BKDATA_META_DB_URL = ''
    BKDATA_LOG_DB_URL = ''
    DB_POOL_RECYCLE = 3600
    DB_POOL_SIZE = 100

    KAFKA_ADDR = ['127.0.0.1:9092']
    LOCAL_REDIS_URL = 'redis://localhost:6379/0'
    REDIS_URL = 'redis://localhost:6379/0'
    REDIS_TIMEOUT = 3600 * 96
    REDIS_TTL = 60 * 5

    ZK_ADDR = 'localhost:2181'
    ZK_PATH = '/metadata_access_service'

    biz_db_urls = {'bkdata_basic': ''}

    # rabbitmq
    RABBITMQ_CONN = 'amqp://guest:guest@localhost:5672/metadata'

    # elasticsearch
    ELASTICSEARCH_ADDR = 'http://localhost:8080'
    ELASTICSEARCH_USER = ''
    ELASTICSEARCH_PASS_TOKEN = ''
    CRYPT_ROOT_KEY = ''
    CRYPT_ROOT_IV = ''
    CRYPT_INSTANCE_KEY = ''

    @property
    def redis_lite_db_path(self):
        return path.abspath(
            path.join(
                self.config_collection.intro_config.DATA_PATH,
                'metadata_redis_lite.rdb',
            )
        )

    @property
    def redis_lite_socket_path(self):
        return path.abspath(
            path.join(
                self.config_collection.intro_config.DATA_PATH,
                'metadata_redis_lite.socket',
            )
        )


class DgraphBackendConfig(BaseConfig):
    SERVERS = [
        "http://127.0.0.1:8080",
    ]
    POOL_SIZE = 50
    POOL_MAX_SIZE = 100
    MUTATE_CHUNKED_SIZE = 500


class LoggingConfig(BaseConfig):
    STREAM_LEVEL = logging.DEBUG
    FILE_LEVEL = logging.DEBUG
    MAJOR_FILE_NAME = 'metadata'
    RELATIVE_PATH = 'log'
    FORMAT = SimpleJsonFormatter()

    def __init__(self):
        self.suffix_postfix = None

    @property
    def file_name(self):
        return (
            '_'.join((self.MAJOR_FILE_NAME, self.suffix_postfix)) if self.suffix_postfix else self.MAJOR_FILE_NAME
        ) + '.common.log'

    @property
    def file_path(self):
        return path.abspath(
            path.join(self.config_collection.intro_config.DATA_PATH, self.RELATIVE_PATH, self.file_name)
        )

    def setup(self):
        for attr in ['file_path']:
            dir_create(getattr(self, attr), is_file=True)


class TaskConfig(BaseConfig):
    TABLE_NAMES_TO_SYNC = []
    TABLE_NAMES_TO_GET = []
    TABLE_NAMES_TO_META_SYNC = []
    TO_COMMON_RDFS_TABLE_NAMES = []
    TO_CUSTOM_RDFS_TABLE_NAMES = []
