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
from environs import Env

from pizza.settings_default import *  # noqa
from pizza.settings_default import LOG_CLASS  # noqa
from pizza.settings_default import (
    APP_NAME,
    DATABASES,
    INSTALLED_APPS,
    LOG_BACKUP_COUNT,
    LOG_DIR,
    LOG_MAX_BYTES,
    LOGGING,
)

env = Env()

APP_ID = env.str("APP_ID")
APP_TOKEN = env.str("APP_TOKEN")
DATAWEB_SAAS_FQDN = env.str("DATAWEB_SAAS_FQDN")
BKIAM_HOST = env.str("BKIAM_HOST")
BKIAM_PORT = env.int("BKIAM_PORT")
PAAS_HOST = env.str("PAAS_HOST")
PAAS_HTTP_PORT = env.int("PAAS_HTTP_PORT")
DATAHUB_API_HOST = env.str("DATAHUB_API_HOST")
DATAHUB_API_PORT = env.int("DATAHUB_API_PORT")
DATAFLOW_API_HOST = env.str("DATAFLOW_API_HOST")
DATAFLOW_API_PORT = env.str("DATAFLOW_API_PORT")

ACCESS_API_URL = f"http://{DATAHUB_API_HOST}:{DATAHUB_API_PORT}/v3/access/"
DATABUS_API_URL = f"http://{DATAHUB_API_HOST}:{DATAHUB_API_PORT}/v3/databus/"
STOREKIT_API_URL = f"http://{DATAHUB_API_HOST}:{DATAHUB_API_PORT}/v3/storekit/"
DATAFLOW_API_URL = f"http://{DATAFLOW_API_HOST}:{DATAFLOW_API_PORT}/v3/dataflow/"
TOF_API_URL = ""
ISTM_API_URL = ""
ESB_API_URL = f"http://{PAAS_HOST}:{PAAS_HTTP_PORT}/api/c/compapi/v2/esb/"

INSTALLED_APPS += (APP_NAME,)

DATABASES.update(
    {
        "basic": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_basic",
            "USER": env.str("CONFIG_DB_USER"),
            "PASSWORD": env.str("CONFIG_DB_PASSWORD"),
            "HOST": env.str("CONFIG_DB_HOST"),
            "PORT": env.int("CONFIG_DB_PORT"),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {"NAME": "test_bkdata_auth"},
        },
        "flow": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_flow",
            "USER": env.str("DATAFLOW_DB_USER"),
            "PASSWORD": env.str("DATAFLOW_DB_PASSWORD"),
            "HOST": env.str("DATAFLOW_DB_HOST"),
            "PORT": env.int("DATAFLOW_DB_PORT"),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {"NAME": "test_bkdata_auth"},
        },
        "modeling": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_modeling",
            "USER": env.str("DATAFLOW_DB_USER"),
            "PASSWORD": env.str("DATAFLOW_DB_PASSWORD"),
            "HOST": env.str("DATAFLOW_DB_HOST"),
            "PORT": env.int("DATAFLOW_DB_PORT"),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {"NAME": "test_bkdata_auth"},
        },
        "public": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_meta",
            "USER": env.str("CONFIG_DB_USER"),
            "PASSWORD": env.str("CONFIG_DB_PASSWORD"),
            "HOST": env.str("CONFIG_DB_HOST"),
            "PORT": env.int("CONFIG_DB_PORT"),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {"NAME": "test_bkdata_auth"},
        },
        "superset": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_superset",
            "USER": env.str("DATALAB_DB_USER"),
            "PASSWORD": env.str("DATALAB_DB_PASSWORD"),
            "HOST": env.str("DATALAB_DB_HOST"),
            "PORT": env.int("DATALAB_DB_PORT"),
            "ENCRYPTED": False,
            "CONN_MAX_AGE": 7200,
            "TEST": {"NAME": "test_bkdata_auth"},
        },
    }
)

DATABASE_ROUTERS = ["auth.db_router.DBRouter"]


REDIS_CONFIG = {
    "default": {
        "sentinel": False,
        "port": env.str("REDIS_PORT"),
        "host": env.str("REDIS_HOST"),
        "sentinel_name": env.str("REDIS_NAME"),
        "password": env.str("REDIS_PASS"),
    }
}

AUTH_SYNC_QUEUE = "bkdata_auth_sync_queue"
AUTH_SYNC_FAILED_QUEUE = "bkdata_auth_sync_failed_queue"

# ==============================================================================
# CELERY相关配置
# ==============================================================================

# CELERY 配置，申明任务的文件路径，即包含有 @task 装饰器的函数文件
CELERY_IMPORTS = (
    "auth.tasks.sync_queue_auth",
    "auth.tasks.sync_itsm_ticket_status",
)


if os.path.exists(os.sep.join([os.path.abspath(os.curdir), "auth", "extend", "tasks", "sync_tdw_auth.py"])):
    CELERY_IMPORTS += ("auth.extend.tasks.sync_tdw_auth",)

CELERY_ENABLE_UTC = False
CELERY_BROKER_URL = "amqp://{user}:{password}@{host}:{port}/{vhost}".format(
    user=APP_ID,
    password=APP_TOKEN,
    host=env.str("RABBITMQ_HOST"),
    port=env.str("RABBITMQ_PORT"),
    vhost=APP_ID,
)

LOGGER_LEVEL = env.str("LOGGER_LEVEL", "INFO")

# 增加iam heandler, 落地日志到日志目录, 文件名 iam.log, 并且按固定大小rotate, 保留固定个数
LOGGING["handlers"]["iam"] = {
    "class": LOG_CLASS,
    "formatter": "verbose",
    "filename": os.path.join(LOG_DIR, "iam.log"),
    "maxBytes": LOG_MAX_BYTES,
    "backupCount": LOG_BACKUP_COUNT,
}

LOGGING["loggers"]["iam"] = {
    "handlers": ["iam"],
    "level": LOGGER_LEVEL,
    "propagate": True,
}

AUHT_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SUPPORT_SENSITIVITIES = ["public", "private", "confidential"]

# 是否发送微信审核消息,默认False
SMCS_WEIXIN_SWITCH = True

# 测试环境，使用开发者代替实际用户接受信息
TEST_NOTICE_RECEIVERS = []

# 申请单据处理URL
TICKET_HANDLE_URL = f"{DATAWEB_SAAS_FQDN}/#/auth-center/todos"

DEFAULT_TDW_BIZ_ID = 0

# 是否使用新版资源中心
USE_RESOURCE_CENTER = True

# BKIAM 配置
SUPPORT_IAM = True
IAM_REGISTERED_SYSTEM = "bk_bkdata"
IAM_REGISTERED_APP_CODE = APP_ID
IAM_REGISTERED_APP_SECRET = APP_TOKEN
IAM_API_HOST = f"http://{BKIAM_HOST}:{BKIAM_PORT}"
IAM_API_PAAS_HOST = f"http://{PAAS_HOST}:{PAAS_HTTP_PORT}/"

# APIGW 网关认证信息
APIGW_PUBLIC_KEY_FROM_ESB = True

try:
    module = __import__("auth.extend.extend_pizza_settings", globals(), locals(), ["*"])
    for setting in dir(module):
        if setting == setting.upper():
            locals()[setting] = getattr(module, setting)
except ImportError:
    print("[Auth] 不存在模块自定义 extend 配置")
