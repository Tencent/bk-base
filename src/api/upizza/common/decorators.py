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
import types
from functools import partial, wraps

from django.core.handlers.wsgi import WSGIRequest
from django.utils import six
from django.utils.translation import ugettext as _
from opentracing_instrumentation.request_context import span_in_context
from rest_framework.decorators import action
from rest_framework.request import Request

from .base_utils import custom_params_valid
from .exceptions import ValidationError
from .views import APIView

detail_route = partial(action, detail=True)
list_route = partial(action, detail=False)


def api_view(http_method_names=None):
    """
    Decorator that converts a function-based view into an APIView subclass.
    Takes a list of allowed methods for the view as an argument.
    """
    http_method_names = ["GET"] if (http_method_names is None) else http_method_names

    def decorator(func):

        WrappedAPIView = type(six.PY3 and "WrappedAPIView" or b"WrappedAPIView", (APIView,), {"__doc__": func.__doc__})

        # Note, the above allows us to set the docstring.
        # It is the equivalent of:
        #
        #     class WrappedAPIView(APIView):
        #         pass
        #     WrappedAPIView.__doc__ = func.doc    <--- Not possible to do this

        # api_view applied without (method_names)
        assert not (isinstance(http_method_names, types.FunctionType)), "@api_view missing list of allowed HTTP methods"

        # api_view applied with eg. string instead of list of strings
        assert isinstance(http_method_names, (list, tuple)), (
            "@api_view expected a list of strings, received %s" % type(http_method_names).__name__
        )

        allowed_methods = set(http_method_names) | {"options"}
        WrappedAPIView.http_method_names = [method.lower() for method in allowed_methods]

        def handler(self, *args, **kwargs):
            return func(*args, **kwargs)

        for method in http_method_names:
            setattr(WrappedAPIView, method.lower(), handler)

        WrappedAPIView.__name__ = func.__name__

        WrappedAPIView.renderer_classes = getattr(func, "renderer_classes", APIView.renderer_classes)

        WrappedAPIView.parser_classes = getattr(func, "parser_classes", APIView.parser_classes)

        WrappedAPIView.authentication_classes = getattr(func, "authentication_classes", APIView.authentication_classes)

        WrappedAPIView.throttle_classes = getattr(func, "throttle_classes", APIView.throttle_classes)

        WrappedAPIView.permission_classes = getattr(func, "permission_classes", APIView.permission_classes)

        return WrappedAPIView.as_view()

    return decorator


def renderer_classes(renderer_classes):
    def decorator(func):
        func.renderer_classes = renderer_classes
        return func

    return decorator


def parser_classes(parser_classes):
    def decorator(func):
        func.parser_classes = parser_classes
        return func

    return decorator


def authentication_classes(authentication_classes):
    def decorator(func):
        func.authentication_classes = authentication_classes
        return func

    return decorator


def throttle_classes(throttle_classes):
    def decorator(func):
        func.throttle_classes = throttle_classes
        return func

    return decorator


def permission_classes(permission_classes):
    def decorator(func):
        func.permission_classes = permission_classes
        return func

    return decorator


def params_valid(serializer, add_params=True):
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            # 获得Django的request对象
            _request = kwargs.get("request")

            if not _request:
                for arg in args:
                    if isinstance(arg, (Request, WSGIRequest)):
                        _request = arg
                        break

            if not _request:
                raise ValidationError(_("该装饰器只允许用于Django的View函数(包括普通View函数和Class-base的View函数)"))

            # 校验request中的参数
            params = {}
            if _request.method in ["GET"]:
                if isinstance(_request, Request):
                    params = _request.query_params
                else:
                    params = _request.GET
            elif _request.is_ajax():
                if isinstance(_request, Request):
                    params = _request.data
                else:
                    params = _request.json()
            else:
                if isinstance(_request, Request):
                    params = _request.data
                else:
                    params = _request.POST

            cleaned_params = custom_params_valid(serializer=serializer, params=params)
            _request.cleaned_params = cleaned_params

            # 执行实际的View逻辑
            params_add = False
            try:
                # 语法糖，使用这个decorator的话可直接从view中获得参数的字典
                if "params" not in kwargs and add_params:
                    kwargs["params"] = cleaned_params
                    params_add = True
            except TypeError:
                if params_add:
                    del kwargs["params"]
            resp = view_func(*args, **kwargs)
            return resp

        return wrapper

    return decorator


def views_cache(cache_time=300):
    # @todo 通用view函数装饰器
    def decorator(view_func):
        def wrapper(*args, **kwargs):
            # 获得Django的request对象
            _request = kwargs.get("request")

            if not _request:
                for arg in args:
                    if isinstance(arg, (Request, WSGIRequest)):
                        _request = arg
                        break

            if not _request:
                raise ValidationError(_("该装饰器只允许用于Django的View函数(包括普通View函数和Class-base的View函数)"))
            resp = view_func(*args, **kwargs)
            return resp

        return wrapper

    return decorator


def trace_gevent():
    def decorator(gevent_func):
        def wrapper(*args, **kwargs):
            if "__span" not in kwargs:
                return gevent_func(*args, **kwargs)

            span = kwargs.pop("__span")
            context = span_in_context(span)
            context.__enter__()
            resp = gevent_func(*args, **kwargs)
            context.__exit__(*[])
            return resp

        return wrapper

    return decorator
