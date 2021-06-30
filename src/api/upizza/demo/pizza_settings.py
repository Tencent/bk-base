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
from environs import Env

from pizza.settings_default import *  # noqa

env = Env()

APP_NAME = env.str("APP_NAME", "demo")

INSTALLED_APPS += (APP_NAME,)

DATABASES.update(
    {
        "demo": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "bkdata_demo",
            "USER": env.str("CONFIG_DB_USER", "root"),
            "PASSWORD": env.str("CONFIG_DB_PASSWORD", ""),
            "HOST": env.str("CONFIG_DB_HOST", "127.0.0.1"),
            "PORT": env.int("CONFIG_DB_PORT", 3306),
            "ENCRYPTED": False,
        }
    }
)

DATABASE_ROUTERS = ["demo.db_router.DBRouter"]
