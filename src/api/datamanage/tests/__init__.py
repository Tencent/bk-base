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

from rest_framework.test import APITestCase
from common.local import set_local_param


class BaseTestCase(APITestCase):
    databases = "__all__"

    def setUp(self):
        set_local_param('bk_username', 'jtest')

    def get(self, url, params=None):
        params = params if params is not None else {}
        if 'bk_username' not in params:
            params['bk_username'] = 'jtest'
        if 'bk_app_code' not in params:
            params['bk_app_code'] = 'data'

        return self.client.get(url, params)

    def post(self, url, params=None):
        params = params if params is not None else {}
        if 'bk_username' not in params:
            params['bk_username'] = 'jtest'
        if 'bk_app_code' not in params:
            params['bk_app_code'] = 'data'
        return self.client.post(url, json.dumps(params), content_type='application/json')

    def put(self, url, params=None):
        params = params if params is not None else {}
        if 'bk_username' not in params:
            params['bk_username'] = 'jtest'
        if 'bk_app_code' not in params:
            params['bk_app_code'] = 'data'
        return self.client.put(url, json.dumps(params), content_type='application/json')

    def delete(self, url, params=None):
        params = params if params is not None else {}
        if 'bk_username' not in params:
            params['bk_username'] = 'jtest'
        if 'bk_app_code' not in params:
            params['bk_app_code'] = 'data'
        return self.client.delete(url, json.dumps(params), content_type='application/json')

    def is_api_success(self, resp):
        self.assertEqual(resp.status_code, 200)
        content = json.loads(resp.content)
        self.assertEqual(content['result'], True)

        return content['data']

    def is_api_failure(self, resp):
        self.assertEqual(resp.status_code, 200)
        content = json.loads(resp.content)
        self.assertEqual(content['result'], False)

        return content
