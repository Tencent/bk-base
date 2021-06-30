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
import json

from django.conf import settings
from django.utils import translation

from apps.utils import build_auth_args
from apps.utils.local import get_request


def add_esb_info_before_request(params):
    """
    通过 params 参数控制是否检查 request
    """
    # 规范后的参数
    params["bk_app_code"] = settings.APP_CODE
    params["bk_app_secret"] = settings.SECRET_KEY
    params["bkdata_authentication_method"] = "user"
    params["appenv"] = settings.RUN_VER

    if "no_request" in params and params["no_request"]:
        params["bk_username"] = "no_user"
        params["operator"] = "no_user"
    else:
        req = get_request()
        auth_info = build_auth_args(req)
        params.update(auth_info)

        params["auth_info"] = json.dumps(auth_info)
        params.update({"blueking_language": translation.get_language()})
        params["bk_username"] = req.user.username

        if "operator" not in params:
            params["operator"] = req.user.username

    # 兼容参数
    params["submitted_by"] = params["bk_username"]
    params["app_code"] = settings.APP_CODE
    params["app_secret"] = settings.SECRET_KEY
    return params
