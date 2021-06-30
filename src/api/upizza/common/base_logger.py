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
from .base_utils import jsonize
from .log import logger_api


class APIRequestLog:
    """
    用于记录日志Object
    """

    def __init__(self, **kwargs):
        """
        :params str request_id:  请求ID，用于表示请求
        :params datetime request_at: 请求时间
        :params str path: 请求路径
        :params str remote_addr: 请求IP
        :params str host: 请求host
        :params str status_code: 返回码
        :params str data: 请求data
        :params str method: HTTP_METHOD('GET','POST')
        :params str query_params: 请求参数
        :params str response: 返回内容
        :params int response_ms: 响应时间
        :params str request_id: 请求唯一标示,用于trace request
        :params bool result: 请求是否成功, default true(主要由于历史原因一些接口没有添加result)
        :params str code: 请求返回错误码
        :params str errors: 请求返回的错误详情
        :params bool is_log_resp: 用于标示是否需要记录返回结果
        :params str http_header: HTTP请求头部信息
        """
        self.requested_at = kwargs.get("requested_at", "")
        self.path = kwargs.get("path", "")
        self.remote_addr = kwargs.get("remote_addr", "")
        self.host = kwargs.get("host", "")
        self.method = kwargs.get("method", "")
        self.query_params = kwargs.get("query_params", {})
        self.data = kwargs.get("data", {})
        self.response = kwargs.get("response", "")
        self.status_code = kwargs.get("status_code", "")
        self.response_ms = kwargs.get("response_ms", 0)
        self.request_id = kwargs.get("request_id", "")
        self.result = kwargs.get("result", True)
        self.code = kwargs.get("code", "")
        self.errors = kwargs.get("errors", "")
        self.is_log_resp = kwargs.get("is_log_resp", True)
        self.extra = kwargs.get("extra", {})
        self.http_header = kwargs.get("http_header", {})

    def set_extra(self, extra):
        """用于记录该次请求的一些扩展字段"""
        self.extra = extra if isinstance(extra, type({})) else {}

    def write(self):
        """
        logger request log
        """
        logger_api.info(jsonize(self.__dict__))
