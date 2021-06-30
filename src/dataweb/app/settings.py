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
import os

from environs import Env

# 本地存在环境变量文件优先加载
env = Env()
env.read_env()

# V3判断环境的环境变量为BKPAAS_ENVIRONMENT
if "BKPAAS_ENVIRONMENT" in os.environ:
    ENVIRONMENT = os.getenv("BKPAAS_ENVIRONMENT", "dev")
# V2判断环境的环境变量为BK_ENV
else:
    PAAS_V2_ENVIRONMENT = os.environ.get("BK_ENV", "development")
    ENVIRONMENT = {"development": "dev", "testing": "stag", "production": "prod"}.get(PAAS_V2_ENVIRONMENT)
DJANGO_CONF_MODULE = "config.{env}".format(env=ENVIRONMENT)

try:
    _module = __import__(DJANGO_CONF_MODULE, globals(), locals(), ["*"])
except ImportError as err:
    raise ImportError("Could not import config '{}' (Is it on sys.path?): {}".format(DJANGO_CONF_MODULE, err))

for _setting in dir(_module):
    if _setting == _setting.upper():
        locals()[_setting] = getattr(_module, _setting)


locals()["SENSITIVE_PARAMS"].extend(list(locals()["OAUTH_COOKIES_PARAMS"].keys()))
