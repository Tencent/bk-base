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
import json

from django.conf import settings
from rest_framework.test import APIClient

from common.api.base import DataResponse


class UnittestClient(APIClient):
    def _add_auth_field(self, params=None):
        params = params or {}
        if "bk_username" not in params:
            params["bk_username"] = "bk_{}_unittest".format(settings.APP_NAME)
        if "bk_app_code" not in params:
            params["bk_app_code"] = "bk_{}_unittest".format(settings.APP_NAME)
        return params

    def get(self, url, data=None, **kwargs):
        data = self._add_auth_field(data)
        response = super(UnittestClient, self).get(url, data, **kwargs)
        return DataResponse(json.loads(response.content))

    def post(self, url, data=None, format="json", **kwargs):
        data = self._add_auth_field(data)
        response = super(UnittestClient, self).post(url, data, format, **kwargs)
        return DataResponse(json.loads(response.content))

    def put(self, url, data=None, format="json", **kwargs):
        data = self._add_auth_field(data)
        response = super(UnittestClient, self).put(url, data, format, **kwargs)
        return DataResponse(json.loads(response.content))

    def patch(self, url, data=None, format="json", **kwargs):
        data = self._add_auth_field(data)
        response = super(UnittestClient, self).patch(url, data, format, **kwargs)
        return DataResponse(json.loads(response.content))

    def delete(self, url, data=None, format="json", **kwargs):
        data = self._add_auth_field(data)
        response = super(UnittestClient, self).delete(url, data, format, **kwargs)
        return DataResponse(json.loads(response.content))
