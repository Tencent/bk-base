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
    ACCESS_API_HOST,
    ACCESS_API_PORT,
    AUTH_API_HOST,
    AUTH_API_PORT,
    DATABUS_API_HOST,
    DATABUS_API_PORT,
    DATAMANAGE_API_HOST,
    DATAMANAGE_API_PORT,
    META_API_HOST,
    META_API_PORT,
)
from pizza.settings import DATABASES, INSTALLED_APPS

AUTH_API_URL = "http://%s:%d/v3/auth/" % (AUTH_API_HOST, AUTH_API_PORT)
ACCESS_API_URL = "http://%s:%d/v3/access/" % (ACCESS_API_HOST, ACCESS_API_PORT)
DATABUS_API_URL = "http://%s:%d/v3/databus/" % (DATABUS_API_HOST, DATABUS_API_PORT)
META_API_URL = "http://%s:%d/v3/meta/" % (META_API_HOST, META_API_PORT)
DATAMANAGE_API_URL = "http://%s:%d/v3/datamanage/" % (DATAMANAGE_API_HOST, DATAMANAGE_API_PORT)

DEBUG = False
ALLOWED_HOSTS = ["*"]
APP_NAME = "datahub"
USED_APPS = ["datahub"]

DATABASE_ROUTERS = ["datahub.dbrouter.DbRouter"]
DATABASES.update(
    {
        "mapleleaf": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_basic",
            "USER": getattr(dataapi_settings, "CONFIG_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_DB_PASSWORD", "bkdata"),
            "HOST": getattr(dataapi_settings, "CONFIG_DB_HOST", "localhost"),
            "PORT": getattr(dataapi_settings, "CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "queue_db": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": getattr(dataapi_settings, "CONFIG_QUEUE_DB_NAME", "bkdata_basic"),
            "USER": getattr(dataapi_settings, "CONFIG_QUEUE_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "CONFIG_QUEUE_DB_PASSWORD", "bkdata"),
            "HOST": getattr(dataapi_settings, "CONFIG_QUEUE_DB_HOST", "localhost"),
            "PORT": getattr(dataapi_settings, "CONFIG_QUEUE_DB_PORT", 3306),
            "ENCRYPTED": False,
        },
    }
)

CELERY_IMPORTS = ("datahub.storekit.maintain_task", "datahub.access.collectors.file_collector.periodic_tasks")
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = "Asia/Shanghai"
CELERY_BROKER_URL = "amqp://{user}:{password}@{host}:{port}/{vhost}".format(
    user=dataapi_settings.RABBITMQ_USER,
    password=dataapi_settings.RABBITMQ_PASS,
    host=dataapi_settings.RABBITMQ_HOST,
    port=dataapi_settings.RABBITMQ_PORT,
    vhost=dataapi_settings.RABBITMQ_VHOST,
)

TAG_RELATED_MODELS = {
    "AccessRawData": {"target_type": "raw_data", "table_primary_key": "access_raw_data.id"},
    "StorageClusterConfig": {"target_type": "storage_cluster", "table_primary_key": "storage_cluster_config.id"},
    "DatabusCluster": {"target_type": "connector_cluster", "table_primary_key": "databus_connector_cluster_config.id"},
    "DatabusChannel": {"target_type": "channel_cluster", "table_primary_key": "databus_channel_cluster_config.id"},
}

ROOT_URLCONF = "datahub.urls"
INSTALLED_APPS += ("datahub",)
