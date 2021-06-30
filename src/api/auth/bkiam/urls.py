# -*- coding: utf-8 -*
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
from django.conf.urls import url
from iam.contrib.django.dispatcher.dispatchers import (
    DjangoBasicResourceApiDispatcher,
    success_response,
)
from iam.resource.utils import get_filter_obj, get_page_obj

from auth.bkiam import IAM_REGISTERED_SYSTEM
from auth.bkiam import resources
from auth.bkiam.backend import iam
from auth.bkiam.resources import BKDataResourceProvider


class BKDataDjangoBasicResourceApiDispatcher(DjangoBasicResourceApiDispatcher):
    def _dispatch_search_instance(self, request, data, request_id):
        options = self._get_options(request)

        filter_obj = get_filter_obj(data.get("filter"), ["parent", "keyword"])
        page_obj = get_page_obj(data.get("page"))

        provider = self._provider[data["type"]]

        pre_process = getattr(provider, "pre_search_instance", None)
        if pre_process and callable(pre_process):
            pre_process(filter_obj, page_obj, **options)

        result = provider.list_instance(filter_obj, page_obj, **options)

        return success_response(result.to_dict(), request_id)


def register_resources(dispatcher, resources_module):

    for item in dir(resources):

        if not item.endswith("ResourceProvider"):
            continue

        resource_class = getattr(resources_module, item)

        if issubclass(resource_class, BKDataResourceProvider) and resource_class.resource_type is not None:
            dispatcher.register(resource_class.resource_type, resource_class())


dispatcher = BKDataDjangoBasicResourceApiDispatcher(iam, IAM_REGISTERED_SYSTEM)
register_resources(dispatcher, resources)

urlpatterns = [url(r"^resource/api/v1/$", dispatcher.as_view([]), name="iamApi")]
