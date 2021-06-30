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
from functools import wraps

from werkzeug.wrappers import Request, Response


class WSGIHttpBasicAuth(object):
    """
    WSGI App的HttpBasic中间件，使用装饰器部署。
    """

    def __init__(self, users, realm='login required'):
        self.users = users
        self.realm = realm

    def check_auth(self, username, password):
        return username in self.users and self.users[username] == password

    def auth_required(self, request):
        return Response(
            json.dumps(
                'Could not verify your access level for that URL.\n' 'You have to login with proper credentials'
            ),
            401,
            {'WWW-Authenticate': 'Basic realm="%s"' % self.realm},
        )

    def requires_auth(self, f):
        @wraps(f)
        def decorated(app, environ, start_response):
            request = Request(environ)
            auth = request.authorization
            if not auth or not self.check_auth(auth.username, auth.password):
                response = self.auth_required(request)
                return response(environ, start_response)
            return f(app, environ, start_response)

        return decorated
