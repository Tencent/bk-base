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
from __future__ import absolute_import, unicode_literals

from api.base import DataAPI
from conf.settings import APP_ID, APP_TOKEN, TOF_API_URL


def base_before_request(params):
    params["app_code"] = APP_ID
    params["app_secret"] = APP_TOKEN
    return params


class TOFApi(object):
    MODULE = "TOF"

    def __init__(self):
        self.get_staff_info = DataAPI(
            method="POST",
            url=TOF_API_URL + "get_staff_info/",
            module=self.MODULE,
            description="获取员工信息",
            before_request=base_before_request,
        )
        self.get_dept_staffs_with_level = DataAPI(
            method="POST",
            url=TOF_API_URL + "get_dept_staffs_with_level/",
            module=self.MODULE,
            description="获取部门员工信息",
            before_request=base_before_request,
        )
        self.get_staff_direct_leader = DataAPI(
            method="POST",
            url=TOF_API_URL + "get_staff_direct_leader/",
            module=self.MODULE,
            description="获取员工直属leader信息",
            before_request=base_before_request,
        )
