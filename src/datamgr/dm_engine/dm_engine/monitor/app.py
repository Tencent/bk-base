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

from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)


def make_wsgi_app(registry=None):
    """Create a WSGI app which serves the metrics from a registry."""

    def app(environ, start_response):
        data = generate_latest(registry)
        status = "200 OK"
        response_headers = [("Content-type", CONTENT_TYPE_LATEST), ("Content-Length", str(len(data)))]
        start_response(status, response_headers)
        return iter([data])

    return app


class _SilentHandler(WSGIRequestHandler):
    """WSGI handler that does not log requests."""

    def log_message(self, format, *args):
        """Log nothing."""


def start_wsgi_server(port: int, addr: str = "127.0.0.1"):
    """Starts a WSGI server for prometheus metrics in a process."""
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)

    app = make_wsgi_app(registry)

    httpd = make_server(addr, port, app, WSGIServer, handler_class=_SilentHandler)
    httpd.serve_forever()
