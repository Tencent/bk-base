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
import django_opentracing
import pytz
from common.bklanguage import BkLanguage
from common.local import activate_request
from common.trace_patch import bk_trace_patch_all
from django.conf import settings
from django.http import HttpResponseRedirect
from django.middleware.locale import LocaleMiddleware
from django.utils import timezone
from django.utils.deprecation import MiddlewareMixin
from django_opentracing import OpenTracingMiddleware
from jaeger_client.constants import SAMPLER_TYPE_PROBABILISTIC
from opentracing.ext import tags
from opentracing_instrumentation import span_in_context
from pizza.settings_default import BK_LANGUAGE_HEADER, BK_TIMEZONE_HEADER


class BkLocaleMiddleware(LocaleMiddleware):
    """
    此中间件处理http头中的blueking_language
    根据blueking_language取值控制api返回相应语言的信息
    """

    response_redirect_class = HttpResponseRedirect

    def process_request(self, request):
        """
        请求之前根据http header决定bk用户使用哪种语言
        如果没有指定， 则根据django默认的国际化规则(使用http header accept_language)
        :param request:
        :return:
        """
        language_key = request.META.get(BK_LANGUAGE_HEADER, False)
        if not language_key:
            return super(BkLocaleMiddleware, self).process_request(request)

        # 设置BkLanguage语言
        BkLanguage.set_language(language_key, request)


class PizzaOpenTracingMiddleware(OpenTracingMiddleware):
    """
    初始化API Tracing组件
    """

    def __init__(self, get_response=None):
        self.get_response = get_response
        # 从配置中取得Trace开关
        if not settings.DO_TRACE:
            return

        # 获取采样比率
        from jaeger_client import Config

        config = Config(
            config={
                "sampler": {"type": SAMPLER_TYPE_PROBABILISTIC, "param": settings.TRACING_SAMPLE_RATE},
                "logging": True,
            },
            service_name="{app_name}api{env}".format(
                app_name=settings.APP_NAME,
                env="({})".format(settings.RUN_MODE) if settings.RUN_MODE != "PRODUCT" else "",
            ),
        )
        tracer = config.initialize_tracer()

        settings.OPENTRACING_TRACE_ALL = True
        settings.OPENTRACING_TRACED_ATTRIBUTES = ["path", "method"]
        settings.OPENTRACING_TRACER = django_opentracing.DjangoTracing(tracer)

        super(PizzaOpenTracingMiddleware, self).__init__(get_response)

        bk_trace_patch_all()
        self._ctx_manager = False

    class OperationName:
        """
        用来描述当前Span的动作名称
        """

        def __init__(self, op_name):
            self.__name__ = op_name

    def process_view(self, request, view_func, view_args, view_kwargs):
        """
        middleware hook, 在逻辑执行之前
        :param request:
        :param view_func:
        :param view_args:
        :param view_kwargs:
        :return:
        """
        if not settings.DO_TRACE:
            return None

        if not self._tracing._trace_all:
            return None

        try:
            if hasattr(settings, "OPENTRACING_TRACED_ATTRIBUTES"):
                traced_attributes = getattr(settings, "OPENTRACING_TRACED_ATTRIBUTES")
            else:
                traced_attributes = []

            try:
                self._tracing._tracer.service_name = "{app_name}api{env}".format(
                    app_name=settings.APP_NAME,
                    env="({})".format(settings.RUN_MODE) if settings.RUN_MODE != "PRODUCT" else "",
                )
            except Exception:
                pass
            op_name = PizzaOpenTracingMiddleware.OperationName(request.path)
            span = self._tracing._apply_tracing(request, op_name, traced_attributes).span

            try:
                trace_id = "{:x}".format(span.trace_id)
                activate_request(request, request_id=trace_id)
                span.set_tag(tags.TRACE_ID, span.trace_id)
                span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)
                span.set_tag(tags.HTTP_METHOD, request.method)
                span.set_tag(tags.HTTP_URL, request.get_full_path())
                span.set_tag(tags.REQUEST_ID, trace_id)
                request.META["BK-TRACE-ID"] = trace_id
            except Exception:
                span.set_tag("request_id", "")
            self._ctx_manager = span_in_context(span=span)
            self._ctx_manager.__enter__()
        except Exception:
            return None

    def process_response(self, request, response):
        """
        middleware hook, 在逻辑执行之后，返回response之前
        :param request:
        :param response:
        :return:
        """
        if not settings.DO_TRACE:
            return response

        try:
            self._tracing._finish_tracing(request, response=response)
            if self._ctx_manager:
                self._ctx_manager.__exit__(*[])
        except Exception:
            return response
        return response


class PizzaCommonMiddleware(MiddlewareMixin):
    """
    Pizza框架公共中间件，主要处理逻辑：

    1. 当没有开启 Trace 模式的时候，记录请求对象至线程变量
    2. 设置时区
    """

    def process_request(self, request):
        self._set_tz(request)
        self._set_request(request)
        return None

    def _set_tz(self, request):
        tzname = request.META.get(BK_TIMEZONE_HEADER, None)
        if tzname:
            timezone.activate(pytz.timezone(tzname))

    def _set_request(self, request):
        if not settings.DO_TRACE:
            activate_request(request)
        return None
