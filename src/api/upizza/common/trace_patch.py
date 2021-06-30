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


import gevent
from future import standard_library
from opentracing_instrumentation import get_current_span
from opentracing_instrumentation.client_hooks import (
    mysqldb,
    requests,
    sqlalchemy,
    strict_redis,
    tornado_http,
    urllib,
    urllib2,
)
from opentracing_instrumentation.client_hooks._singleton import singleton
from opentracing_instrumentation.http_client import (
    AbstractRequestWrapper,
    before_http_request,
)
from opentracing_instrumentation.utils import start_child_span

standard_library.install_aliases()


def bk_trace_patch_all():

    try:
        patch_httplib2()
    except Exception:
        pass

    try:
        patch_influxdb()
    except Exception:
        pass

    try:
        mysqldb.install_patches()
    except Exception:
        pass

    try:
        strict_redis.install_patches()
    except Exception:
        pass

    try:
        sqlalchemy.install_patches()
    except Exception:
        pass

    try:
        tornado_http.install_patches()
    except Exception:
        pass

    try:
        urllib.install_patches()
    except Exception:
        pass

    try:
        urllib2.install_patches()
    except Exception:
        pass

    try:
        requests.install_patches()
    except Exception:
        pass


@singleton
def patch_jaeger_span():
    from jaeger_client import Span, thrift
    from opentracing.ext import tags as ext_tags

    def set_tag(self, key, value):
        """
        :param key:
        :param value:
        """
        try:
            if key == ext_tags.SAMPLING_PRIORITY:
                self._set_sampling_priority(value)
            elif self.is_sampled():
                try:
                    tag_value = str(value)
                except UnicodeEncodeError:
                    tag_value = value.encode("UTF-8")
                except UnicodeDecodeError:
                    tag_value = value.decode("UTF-8")

                tag = thrift.make_string_tag(key, tag_value)
                with self.update_lock:
                    self.tags.append(tag)
        except Exception:
            return self
        return self

    Span.set_tag = set_tag


@singleton
def patch_httplib2():
    try:
        import httplib2
    except ImportError:
        return False

    _HTTPLIB2_request = httplib2.Http.request

    def wrapper(
        self,
        uri,
        method="GET",
        body=None,
        headers=None,
        redirections=httplib2.DEFAULT_MAX_REDIRECTS,
        connection_type=None,
    ):
        """Wraps httplib2.Http.request"""
        if headers is None:
            headers = {}
        request_wrapper = RequestWrapper(uri, method, body, headers)
        span = before_http_request(request=request_wrapper, current_span_extractor=get_current_span)
        for key in headers:
            span.set_tag("header_%s" % key, headers[key])

        with span:
            resp, content = _HTTPLIB2_request(self, uri, method, body, headers, redirections, connection_type)
            if hasattr(resp, "status") and resp.status is not None:
                span.set_tag("http.status_code", resp.status)
        return resp, content

    class RequestWrapper(AbstractRequestWrapper):
        def __init__(self, uri, method, body, headers):
            self.method_name = method
            self.uri = uri
            self.body = body
            self.headers = headers

        def add_header(self, key, value):
            self.headers[key] = value

        @property
        def method(self):
            return self.method_name

        @property
        def full_url(self):
            return self.uri

        @property
        def _headers(self):
            return self.headers

        @property
        def operation(self):
            return "HTTP_%s" % self.method_name.upper()

        @property
        def service_name(self):
            return None

    httplib2.Http.request = wrapper


@singleton
def patch_influxdb():
    try:
        from influxdb import InfluxDBClient
        from influxdb.resultset import ResultSet
    except ImportError:
        return False

    real_action = InfluxDBClient.query

    def wrapper(self, query, **kwargs):
        tags = {"query": query}
        tags.update(kwargs)
        span = start_child_span(operation_name="INFLUXDB_QUERY", parent=get_current_span(), tags=tags)

        with span:
            resp = real_action(self, query, **kwargs)
            try:
                if type(resp) in [list, ResultSet]:
                    span.set_tag("result.length", len(resp))
            except Exception:
                pass
        return resp

    InfluxDBClient.query = wrapper


def gevent_spawn_with_span(*args, **kwargs):
    span = get_current_span()
    kwargs["__span"] = span
    return gevent.spawn(*args, **kwargs)
