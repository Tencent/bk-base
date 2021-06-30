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

from django.http import JsonResponse
from django.utils.timezone import now
from rest_framework import status
from rest_framework.response import Response

from common import local

from .base_logger import APIRequestLog
from .base_utils import custom_params_valid
from .django_utils import JsonResponse as CustomJsonResponse


class LoggingMixin:
    """Mixin to log requests"""

    def initial(self, request, *args, **kwargs):
        """Set current time on request"""
        # get data dict
        try:
            if request.content_type == "application/x-www-form-urlencoded":  # 主要为了兼容原数据平台urllib调用问题
                data_dict = json.loads(request.body)
            else:
                data_dict = request.data.dict()
        except AttributeError:  # if already a dict, can't dictify
            data_dict = request.data
        except Exception:
            data_dict = {}

        # get IP
        ipaddr = request.META.get("HTTP_X_FORWARDED_FOR", None)
        request_id = request.META.get("BK-TRACE-ID", local.get_request_id())
        if ipaddr:
            # X_FORWARDED_FOR returns client1, proxy1, proxy2,...
            ipaddr = ipaddr.split(", ")[0]
        else:
            ipaddr = request.META.get("REMOTE_ADDR", "")

        # write to log
        self.request.log = APIRequestLog(
            requested_at=now(),
            path=request.path,
            remote_addr=ipaddr,
            host=request.get_host(),
            method=request.method,
            query_params=request.query_params.dict(),
            data=data_dict,
            request_id=request_id,
            http_header={key: value for key, value in list(request.META.items()) if key.startswith("HTTP_")},
        )

        super(LoggingMixin, self).initial(request, *args, **kwargs)

        # add user to log after auth
        self.request.log.user = local.get_request_username()
        self.request.log.app_code = local.get_request_app_code()

    def perform_authentication(self, request):
        pass

    def finalize_response(self, request, response, *args, **kwargs):
        # regular finalize response
        response = super(LoggingMixin, self).finalize_response(request, response, *args, **kwargs)
        response["X-Request-ID"] = self.request.log.request_id  # create request_id

        # compute response time
        response_timedelta = now() - self.request.log.requested_at
        response_ms = int(response_timedelta.total_seconds() * 1000)  # milliseconds

        # write to log
        # 用于表示是否记录返回内容，由于有些返回
        is_log_resp = getattr(response, "is_log_resp", True)
        if is_log_resp:
            try:
                if isinstance(response, Response):
                    self.request.log.response = response.rendered_content.decode("utf-8")
                elif isinstance(response, CustomJsonResponse) or isinstance(response, JsonResponse):
                    self.request.log.response = response.content.decode("utf-8")
            except Exception as e:
                self.request.log.response = "{}".format(e)
                self.request.log.result = False
                self.request.log.errors = ""
                self.request.log.code = "1500500"
            else:
                if getattr(response, "result", None) is not None:
                    self.request.log.result = response.result
                    self.request.log.code = getattr(response, "code", "")
                    self.request.log.errors = getattr(response, "errors", "")
                else:
                    try:
                        response_data = json.loads(self.request.log.response)
                    except ValueError:
                        response_data = {}
                    self.request.log.result = response_data.get("result", False)
                    self.request.log.code = response_data.get("code", "")
                    self.request.log.errors = response_data.get("errors", "")

            # 为了避免记录的response过长，这里截取前10240个字符
            self.request.log.response = self.request.log.response[:10240]
            self.request.log.errors = self.request.log.errors[:10240]

        self.request.log.status_code = response.status_code
        self.request.log.response_ms = response_ms
        self.request.log.is_log_resp = is_log_resp
        self.request.log.write()

        # return
        return response


class ValidationMixin:
    def params_valid(self, serializer, params=None):
        """
        校验参数是否满足 serializer 规定的格式
        """
        # 获得Django的request对象
        _request = self.request

        # 校验request中的参数
        if not params:
            if _request.method in ["GET"]:
                params = _request.query_params
            else:
                params = _request.data

        return custom_params_valid(serializer=serializer, params=params)


class ResponseMixin:
    def finalize_response(self, request, response, *args, **kwargs):

        # 目前仅对 Restful Response 进行处理
        if isinstance(response, Response):
            response.data = {"result": True, "data": response.data, "code": "1500200", "message": "ok", "errors": None}
            response.status_code = status.HTTP_200_OK
            response.is_log_resp = getattr(response, "is_log_resp", True)
            response.result = True
            response.code = "1500200"
            response.errors = ""
        return super(ResponseMixin, self).finalize_response(request, response, *args, **kwargs)
