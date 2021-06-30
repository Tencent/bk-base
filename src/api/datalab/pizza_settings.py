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
from pizza.settings import *  # noqa

DATABASES.update(
    {  # noqa
        "bkdata_lab": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_lab",
            "USER": getattr(dataapi_settings, "ANALYSIS_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "ANALYSIS_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "ANALYSIS_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "ANALYSIS_DB_PORT", 3306),
            "ENCRYPTED": False,
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
        "bkdata_jupyterhub": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_jupyterhub",
            "USER": getattr(dataapi_settings, "ANALYSIS_DB_USER", "root"),
            "PASSWORD": getattr(dataapi_settings, "ANALYSIS_DB_PASSWORD", ""),
            "HOST": getattr(dataapi_settings, "ANALYSIS_DB_HOST", "127.0.0.1"),
            "PORT": getattr(dataapi_settings, "ANALYSIS_DB_PORT", 3306),
            "ENCRYPTED": False,
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
            "TEST": {
                "NAME": "bkdata_test",
            },
        },
    }
)

QUERYENGINE_API_ROOT = "http://{host}:{port}/v3/queryengine".format(
    host=dataapi_settings.QUERYENGINE_API_HOST, port=dataapi_settings.QUERYENGINE_API_PORT
)
JUPYTERHUB_API_ROOT = "{http_schema}://{host}:{port}".format(
    http_schema=dataapi_settings.HTTP_SCHEMA,
    host=dataapi_settings.JUPYTERHUB_API_HOST,
    port=dataapi_settings.JUPYTERHUB_API_PORT,
)
META_API_ROOT = "http://{host}:{port}/v3/meta".format(
    host=dataapi_settings.META_API_HOST, port=dataapi_settings.META_API_PORT
)
STOREKIT_API_ROOT = "http://{host}:{port}/v3/storekit".format(
    host=dataapi_settings.STOREKIT_API_HOST, port=dataapi_settings.STOREKIT_API_PORT
)
DATAFLOW_API_ROOT = "http://{host}:{port}/v3/dataflow".format(
    host=dataapi_settings.DATAFLOW_API_HOST, port=dataapi_settings.DATAFLOW_API_PORT
)

OUTPUT_TYPES = dataapi_settings.OUTPUT_TYPES
DATABASE_ROUTERS = ["datalab.dbrouter.DbRouter"]
