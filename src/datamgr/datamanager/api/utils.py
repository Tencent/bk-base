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
from __future__ import absolute_import, print_function, unicode_literals

import json

from conf.settings import APP_ID, APP_TOKEN


def add_dataapi_inner_header():
    """
    调用 DataAPI 时，使用内部授权机制，则使用这个 header
    """
    return {
        "x-bkdata-authorization": json.dumps(
            {
                "bkdata_authentication_method": "inner",
                "bk_app_code": APP_ID,
                "bk_app_secret": APP_TOKEN,
                "bk_username": "admin",
            }
        )
    }


def add_esb_header():
    return {
        "x-bkapi-authorization": json.dumps(
            {"bk_app_code": APP_ID, "bk_app_secret": APP_TOKEN, "bk_username": "admin"}
        )
    }


def add_esb_common_params(params):
    params["bk_app_code"] = APP_ID
    params["bk_app_secret"] = APP_TOKEN
    params["bk_username"] = "admin"
    return params
