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

from common.mixins import LoggingMixin
from rest_framework import exceptions, serializers, status
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet, ModelViewSet

from dataflow.flow.utils.local import activate_request_id


class FlowMixin(LoggingMixin):
    @staticmethod
    def _extract_params(request):
        try:
            if request.method.upper == "GET":
                return request.GET
            else:
                return json.loads(request.body)
        except Exception:
            return {}

    def initialize_request(self, request, *args, **kwargs):
        if "HTTP_X_METHOD_OVERRIDE" in request.META:
            request.method = request.META["HTTP_X_METHOD_OVERRIDE"].upper()
        request_id = request.META.get("HTTP_REQUEST_ID", None)
        activate_request_id(request_id)

        # params = self._extract_params(request)
        # if 'X_HTTP_METHOD_OVERRIDE' in params:
        #    request.method = params['X_HTTP_METHOD_OVERRIDE']
        return super(FlowMixin, self).initialize_request(request, *args, **kwargs)

    def finalize_response(self, request, response, *args, **kwargs):

        # 目前仅对 Restful Response 进行处理
        if isinstance(response, Response):
            response.data = {
                "result": True,
                "data": response.data,
                "code": "1500200",
                "message": "ok",
            }
            response.status_code = status.HTTP_200_OK
        return super(FlowMixin, self).finalize_response(request, response, *args, **kwargs)


class APIViewSet(FlowMixin, GenericViewSet):
    pass


class ModelViewSet(FlowMixin, ModelViewSet):
    pass


class FlowSerializerMixin(object):
    """
    该 Mixin 类主要用于重载部分 Serializer 方法
    """

    def is_valid(self, raise_exception=False):
        if not raise_exception:
            return super(FlowSerializerMixin, self).is_valid()

        try:
            return super(FlowSerializerMixin, self).is_valid(raise_exception=True)
        except exceptions.ValidationError as exc:
            # 对于DRF默认返回的校验异常，需要额外补充 message 字段
            # 由于 ValidationError 需要返回给前端，需要把错误信息处理一下
            exc.detail = self.format_errmsg()
            raise exc

    def format_errmsg(self):
        """
        格式化 DRF serializer 序列化器返回错误信息，简化为字符串提示，错误信息形如：
            {
                "result_tables": {
                    "non_field_errors": [
                        "结果表不可为空"
                    ]
                },
                "app_code": [
                    "该字段是必填项。"
                ]
            }
        @return {String} 简化后的提示信息
        @returnExample
            结果表，结果表不可为空
        """
        errors = self.errors
        declared_fields = self.fields

        _key, _val = list(errors.items())[0]
        _whole_key = _key

        while isinstance(_val, dict):
            _key, _val = list(_val.items())[0]
            _whole_key += "." + _key

        _key_display = ""
        for _key in _whole_key.split("."):
            # 特殊KEY，表示全局字段
            if _key == "non_field_errors":
                break

            _field = declared_fields[_key]
            if hasattr(_field, "child"):
                declared_fields = _field.child

            _key_display = _field.label if _field.label else _key

        format_msg = "{}，{}".format(_key_display, _val[0]) if _key_display else _val[0]
        return format_msg


class FlowSerializer(FlowSerializerMixin, serializers.Serializer):
    pass


class FlowModelSerializer(FlowSerializerMixin, serializers.ModelSerializer):
    pass


class ModelingModelSerializer(FlowSerializerMixin, serializers.ModelSerializer):
    pass


class MLSqlStorageModelSerializer(FlowSerializerMixin, serializers.ModelSerializer):
    pass


class DataPageNumberPagination(PageNumberPagination):
    PAGE_SIZE = 10
    page_query_param = "page"
    page_size_query_param = "page_size"
    max_page_size = 1000

    def get_paginated_response(self, data):
        return Response({"count": self.page.paginator.count, "results": data})
