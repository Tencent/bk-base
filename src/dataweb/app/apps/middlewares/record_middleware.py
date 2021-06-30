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
import json

from django.core.cache import cache
from django.utils.deprecation import MiddlewareMixin
from rest_framework.viewsets import GenericViewSet

from apps.dataflow.models import UserOperateRecord
from apps.utils.local import activate_request, get_request_id


class RecordMiddleware(MiddlewareMixin):
    def __init__(self, get_response=None):
        super(RecordMiddleware, self).__init__(get_response=get_response)
        self.records = {}

    def process_request(self, request, **kwargs):
        activate_request(request)
        self.records.update({request: UserOperateRecord()})
        _record = self.records[request]
        _record.url = request.path
        _record.method = request.method
        if _record.method == "GET":
            _record.operate_params = json.dumps(request.GET)
        else:
            _record.operate_params = request.body
        return None

    def process_view(self, request, view, args, kwargs):

        # 记录用户最后一次访问的project_id
        if "project_id" in kwargs:
            cache.set("{user}_last_project".format(user=request.user.username), kwargs.get("project_id"))

        if issubclass(getattr(view, "cls", RecordMiddleware), GenericViewSet):
            try:
                _record = self.records[request]
            except KeyError:
                return
            _record.project_id = kwargs.get("project_id", None)
            _record.operate_type = "{}.{}".format(view.__name__, view.actions.get(_record.method.lower(), None))
            _record.operate_object = view.__dict__["cls"].lookup_field
            _record.operate_object_id = kwargs.get(_record.operate_object, None)
            _record.extra_data = json.dumps(kwargs)

    def process_response(self, request, response):
        try:
            _record = self.records[request]
        except KeyError:
            return response
        _record.operator = request.user.username
        try:
            _record.operate_result = response.content[:1023]
        except AttributeError:
            _record.operate_result = ""

        try:
            _record.operate_final = response.data.get("result", False)
        except AttributeError:
            _record.operate_final = False
            pass
        operate_types = [_t[0] for _t in UserOperateRecord.OPERATE_TYPES]
        if _record.operate_type in operate_types:
            _record.request_id = get_request_id()
            _record.save()
        self.records.pop(request)
        return response
