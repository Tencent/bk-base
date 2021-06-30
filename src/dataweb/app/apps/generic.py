# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.conf import settings
from django.http import Http404, JsonResponse
from django.utils.translation import ugettext as _
from rest_framework import exceptions, serializers, status
from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet
from rest_framework.viewsets import ModelViewSet as _ModelViewSet

from apps.common.log import logger
from apps.exceptions import DataError, ErrorCode, FormError


class CsrfExemptSessionAuthentication(SessionAuthentication):
    def enforce_csrf(self, request):
        # To not perform the csrf check previously happening
        return


class FlowMixin(object):
    """
    封装 APIViewSet 修改 ModelViewSet 默认返回内容，固定格式为
        {result: True, data: {}, code: 00, message: ''}
    """

    authentication_classes = (CsrfExemptSessionAuthentication, BasicAuthentication)

    def finalize_response(self, request, response, *args, **kwargs):

        # 目前仅对 Restful Response 进行处理
        if isinstance(response, Response):
            response.data = {"result": True, "data": response.data, "code": "00", "message": ""}
            response.status_code = status.HTTP_200_OK

        # 返回响应头禁用浏览器的类型猜测行为
        response._headers["x-content-type-options"] = ("X-Content-Type-Options", "nosniff")
        return super(FlowMixin, self).finalize_response(request, response, *args, **kwargs)

    def valid(self, form_class, filter_blank=False, filter_none=False):
        """
        校验参数是否满足组 form_class 的校验
        @param {django.form.Form} form_class 验证表单
        @param {Boolean} filter_blank 是否过滤空字符的参数
        @param {Boolean} filter_none 是否过滤 None 的参数

        @raise FormError 表单验证不通过时抛出
        """
        if self.request.method == "GET":
            _form = form_class(self.request.query_params)
        else:
            _form = form_class(self.request.data)

        if not _form.is_valid():
            raise FormError(_form.format_errmsg())
        _data = _form.cleaned_data
        if filter_blank:
            _data = {_k: _v for _k, _v in list(_data.items()) if _v != ""}
        if filter_none:
            _data = {_k: _v for _k, _v in list(_data.items()) if _v is not None}

        return _data

    def valid_serializer(self, serializer):
        """
        校验参数是否满足组 serializer 的校验
        @param {serializer} serializer 验证表单
        @return {serializer} _serializer 序列器（已进行校验清洗）
        """
        _request = self.request
        if _request.method == "GET":
            _serializer = serializer(data=_request.query_params)
        else:
            _serializer = serializer(data=_request.data)
        _serializer.is_valid(raise_exception=True)
        return _serializer

    def is_pagination(self, request):
        page = request.query_params.get("page", "")
        page_size = request.query_params.get("page_size", "")
        return page != "" and page_size != ""

    def do_paging(self, request, data):
        # 处理分页
        if self.is_pagination(request):
            page = int(request.query_params["page"])
            page_size = int(request.query_params["page_size"])

            count = len(data)
            total_page = (count + page_size - 1) / page_size
            data = data[page_size * (page - 1) : page_size * page]

            return {"total_page": total_page, "count": count, "results": data}
        else:
            # 无分页请求时返回全部
            return {"total_page": 1, "count": len(data), "results": data}


class APIViewSet(FlowMixin, GenericViewSet):
    pass


class ModelViewSet(FlowMixin, _ModelViewSet):
    pass


class DataSerializerMixin(object):
    """
    该 Mixin 类主要用于重载部分 Serializer 方法
    """

    def is_valid(self, raise_exception=False):
        if not raise_exception:
            return super(DataSerializerMixin, self).is_valid()

        try:
            return super(DataSerializerMixin, self).is_valid(raise_exception=True)
        except exceptions.ValidationError as exc:
            # 对于DRF默认返回的校验异常，需要额外补充 message 字段
            # 由于 ValidationError 需要返回给前端，需要把错误信息处理一下
            # @todo 多层serializer的报错需递归生成
            exc.message = self.format_errmsg()
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

        while type(_val) is dict:
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

        format_msg = "{}，{}".format(_key_display, _val[0])
        return format_msg


class DataSerializer(DataSerializerMixin, serializers.Serializer):
    pass


class DataModelSerializer(DataSerializerMixin, serializers.ModelSerializer):
    pass


def custom_exception_handler(exc, context):
    """
    自定义错误处理方式
    """
    # 专门处理 404 异常，直接返回前端，前端处理
    if isinstance(exc, Http404):
        return JsonResponse(_error("404", exc.message))

    # # 专门处理 403 异常，直接返回前端，前端处理
    # if isinstance(exc, exceptions.PermissionDenied):
    #     return HttpResponse(exc.detail, status='403')

    # 特殊处理 rest_framework ValidationError
    if isinstance(exc, exceptions.ValidationError):
        return JsonResponse(_error("{}".format(exc.status_code), exc.message))

    # 处理 rest_framework 的异常
    if isinstance(exc, exceptions.APIException):
        return JsonResponse(_error("{}".format(exc.status_code), exc.detail))

    # 处理 Data APP 自定义异常
    if isinstance(exc, DataError):
        _msg = _("【APP 自定义异常】{message}, code={code}, args={args}, errors={errors}").format(
            message=exc.message, code=exc.code, args=exc.args, data=exc.data, errors=exc.errors
        )
        logger.warning(_msg)
        return JsonResponse(_error(exc.code, exc.message, exc.data, errors=exc.errors))

    # 判断是否在debug模式中,
    # 在这里判断是防止阻止了用户原本主动抛出的异常
    if settings.DEBUG:
        return None

    # 非预期异常
    logger.exception(exc.message)
    return JsonResponse(_error("500", _("系统错误，请联系管理员")))


def _error(code=None, message="", data=None, errors=None):
    if len(str(code)) == 3:
        code = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, ErrorCode.BKDATA_WEB_CODE, code)
    message = "{}（{}）".format(message, code)
    return {"result": False, "code": code, "data": data, "message": message, "errors": errors}


class DataPageNumberPagination(PageNumberPagination):
    PAGE_SIZE = 10
    page_query_param = "page"
    page_size_query_param = "page_size"
    max_page_size = 1000

    def get_paginated_response(self, data):
        return Response({"count": self.page.paginator.count, "results": data})
