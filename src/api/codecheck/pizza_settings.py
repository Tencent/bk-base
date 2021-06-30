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
# flake8: noqa
from conf.dataapi_settings import *
from pizza.settings_default import MIDDLEWARE, REST_FRAMEWORK

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_codecheck",
        "USER": CODECHECK_DB_USER,
        "PASSWORD": CODECHECK_DB_PASSWORD,
        "HOST": CODECHECK_DB_HOST,
        "PORT": CODECHECK_DB_PORT,
        "ENCRYPTED": False,
    }
}

APP_NAME = "codecheck"

CODE_CHECK_APPS = ["codecheck"]
