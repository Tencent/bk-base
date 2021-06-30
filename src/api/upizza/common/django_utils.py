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
import datetime
import decimal
import json

from django.http import HttpResponse
from rest_framework.utils import encoders


class CustomJSONEncoder(encoders.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time and decimal types.
    And process the smart place name object
    """

    DATE_FORMAT = "%Y-%m-%d"
    TIME_FORMAT = "%H:%M:%S"

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("{} {}".format(self.DATE_FORMAT, self.TIME_FORMAT))
        elif isinstance(o, datetime.date):
            return o.strftime(self.DATE_FORMAT)
        elif isinstance(o, datetime.time):
            return o.strftime(self.TIME_FORMAT)
        elif isinstance(o, decimal.Decimal):
            return float(o)
        else:
            return super(CustomJSONEncoder, self).default(o)


class JsonResponse(HttpResponse):
    def __init__(self, data, safe=True, **kwargs):
        if safe and not isinstance(data, dict):
            raise TypeError("In order to allow non-dict objects to be " "serialized set the safe parameter to False")
        kwargs.setdefault("content_type", "application/json")
        self.is_log_resp = kwargs.pop("is_log_resp", True)
        self.result = data.get("result", False)
        self.code = data.get("code", "")
        self.errors = json.dumps(data.get("errors", {}), cls=CustomJSONEncoder, ensure_ascii=True)
        data = json.dumps(data, cls=CustomJSONEncoder, ensure_ascii=True)
        super(JsonResponse, self).__init__(content=data, **kwargs)

    @property
    def is_success(self):
        return self.result


class DataResponse(JsonResponse):
    def __init__(self, result=True, data=None, message="ok", code="1500200", errors=None, **kwargs):
        super(DataResponse, self).__init__(
            {"result": result, "data": data, "code": code, "message": message, "errors": errors}, **kwargs
        )
