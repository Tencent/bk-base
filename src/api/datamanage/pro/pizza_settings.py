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
import os

from conf import dataapi_settings_pro
from pizza.settings_default import *  # noqa


CELERY_TIMEZONE = getattr(dataapi_settings_pro, 'CELERY_TIMEZONE', 'Asia/Shanghai')

STANDARD_PROJECT_ID = getattr(dataapi_settings_pro, 'STANDARD_PROJECT_ID', 0)

INSTALLED_APPS += (  # noqa
    'datamanage.pro.lifecycle',
    'datamanage.pro.dataquality',
    'datamanage.pro.datamodel',
)

LOCALE_PATHS += (os.path.join(PIZZA_ROOT, APP_NAME, 'pro', 'locale'),)  # noqa  # noqa

BROKER_URL = "redis://{}:{}@{}:{}/0".format(
    getattr(dataapi_settings_pro, 'REDIS_NAME'),
    getattr(dataapi_settings_pro, 'REDIS_PASS'),
    getattr(dataapi_settings_pro, 'REDIS_HOST'),
    getattr(dataapi_settings_pro, 'REDIS_PORT'),
)
CELERY_BROKER_URL = "redis://{}:{}@{}:{}/0".format(
    getattr(dataapi_settings_pro, 'REDIS_NAME'),
    getattr(dataapi_settings_pro, 'REDIS_PASS'),
    getattr(dataapi_settings_pro, 'REDIS_HOST'),
    getattr(dataapi_settings_pro, 'REDIS_PORT'),
)
RABBITMQ_USER = getattr(dataapi_settings_pro, 'RABBITMQ_USER', '')
RABBITMQ_PASS = getattr(dataapi_settings_pro, 'RABBITMQ_PASS', '')
RABBITMQ_HOST = getattr(dataapi_settings_pro, 'RABBITMQ_HOST', '')
RABBITMQ_PORT = getattr(dataapi_settings_pro, 'RABBITMQ_PORT', '')
RABBITMQ_VHOST = getattr(dataapi_settings_pro, 'RABBITMQ_VHOST', '')

CELERY_BROKER_URL = 'amqp://{user}:{password}@{host}:{port}/{vhost}'.format(
    user=RABBITMQ_USER, password=RABBITMQ_PASS, host=RABBITMQ_HOST, port=RABBITMQ_PORT, vhost=RABBITMQ_VHOST
)
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
CELERY_ENABLE_UTC = True


LOGGER_FORMATTER = 'verbose'
# 自定义日志
LOGGING['handlers'].update(
    {
        'range_task': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_range_task.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
        'heat_task': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_heat_task.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
        'score_sort_task': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_score_sort_task.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
        'data_inventory_task': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_data_inventory_task.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
        'app_code_task': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_app_code_task.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
        'data_model_sync': {
            'class': LOG_CLASS,
            'formatter': LOGGER_FORMATTER,
            'filename': os.path.join(LOG_DIR, 'sys_data_model_sync.log'),
            'maxBytes': LOG_MAX_BYTES,
            'backupCount': LOG_BACKUP_COUNT,
        },
    }
)

LOGGING['loggers'].update(
    {
        'range_task': {'handlers': ['range_task'], 'level': LOGGER_LEVEL, 'propagate': False},
        'heat_task': {'handlers': ['heat_task'], 'level': LOGGER_LEVEL, 'propagate': False},
        'score_sort_task': {'handlers': ['score_sort_task'], 'level': LOGGER_LEVEL, 'propagate': False},
        'data_inventory_task': {'handlers': ['data_inventory_task'], 'level': LOGGER_LEVEL, 'propagate': False},
        'app_code_task': {'handlers': ['app_code_task'], 'level': LOGGER_LEVEL, 'propagate': False},
        'data_model_sync': {'handlers': ['data_model_sync'], 'level': LOGGER_LEVEL, 'propagate': False},
    }
)

RANGE_METRIC_MEASUREMENT = 'range_metrics'
HEAT_METRIC_MEASUREMENT = 'heat_metrics'
HEAT_RELATED_METRIC_MEASUREMENT = 'heat_related_metric'

ZK_ADDR = getattr(dataapi_settings_pro, 'ZK_ADDR', '127.0.0.1:8000')
ZK_PATH = getattr(dataapi_settings_pro, 'ZK_PATH', '/metadata_access_service')
META_ACCESS_RPC_ENDPOINT = getattr(
    dataapi_settings_pro, 'META_ACCESS_RPC_ENDPOINT', 'http://127.0.0.1:5000/jsonrpc/2.0/'
)

RULE_AUDIT_TASK_QUEUE = getattr(dataapi_settings_pro, 'RULE_AUDIT_TASK_QUEUE', 'data_set_audit_tasks')

CACHES['redis'] = {
    "BACKEND": "django_redis.cache.RedisCache",
    "LOCATION": "redis://{}:{}@{}:{}/0".format(
        getattr(dataapi_settings_pro, 'REDIS_NAME'),
        getattr(dataapi_settings_pro, 'REDIS_PASS'),
        getattr(dataapi_settings_pro, 'REDIS_HOST'),
        getattr(dataapi_settings_pro, 'REDIS_PORT'),  # redis的地址
    ),
    "OPTIONS": {
        "CLIENT_CLASS": "django_redis.client.DefaultClient",
        "CONNECTION_POOL_KWARGS": {"max_connections": 10},  # 池的个数
    },
}

CACHES['default'] = CACHES['redis']

ES_USER = getattr(dataapi_settings_pro, 'ES_USER', '')
ES_PASS = getattr(dataapi_settings_pro, 'ES_PASS', '')
ES_HOST = getattr(dataapi_settings_pro, 'ES_HOST', '')
ES_PORT = getattr(dataapi_settings_pro, 'ES_PORT', '')
DATA_TRACE_ES_TOKEN = getattr(dataapi_settings_pro, 'DATA_TRACE_ES_PASS_TOKEN', '')
DATA_TRACE_ES_API_HOST = getattr(dataapi_settings_pro, 'DATA_TRACE_ES_API_HOST', '')
DATA_TRACE_ES_API_PORT = getattr(dataapi_settings_pro, 'DATA_TRACE_ES_API_PORT', '')
DATA_TRACE_ES_API_ADDR = 'http://{}:{}'.format(DATA_TRACE_ES_API_HOST, DATA_TRACE_ES_API_PORT)

ES_SETTINGS = getattr(dataapi_settings_pro, 'ES_SETTINGS', '')

# 数据修正、数据剖析Session环境变量
CORRECTING_DEBUG_SESSION_KEY = 'data_correct_debug_session'
DATA_PROFILING_SESSION_KEY = 'data_profiling_session'
SESSION_GEOG_AREA_CODE = 'inland'
DEBUG_VIRTUAL_TABLE = getattr(dataapi_settings_pro, 'DEBUG_VIRTUAL_TABLE')
PARQUET_FILE_TMP_FOLDER = getattr(dataapi_settings_pro, 'PARQUET_FILE_TMP_FOLDER')

ASSET_VALUE_SCORE = getattr(dataapi_settings_pro, 'ASSET_VALUE_SCORE', 0)
