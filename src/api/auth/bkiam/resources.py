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


from auth.handlers.resource.manager import AuthResourceManager, ResourceFilter
from iam.resource.provider import ListResult, ResourceProvider


class BKDataResourceProvider(ResourceProvider):
    resource_type = None

    def list_attr(self, **options):
        manager = AuthResourceManager.init(self.resource_type)
        data = manager.list_scope_attributes()
        return ListResult(results=data, count=len(data))

    def list_attr_value(self, filter, page, **options):
        kwargs = {}

        # 目前 PAGE 是必定存在的对象，调用方没传，也会有默认值
        kwargs["limit"] = page.limit
        kwargs["offset"] = page.offset
        attr = filter["attr"]

        if filter["keyword"]:
            kwargs["search"] = filter["keyword"]
        if filter["ids"]:
            kwargs["ids"] = filter["ids"]

        manager = AuthResourceManager.init(self.resource_type)
        data = manager.query_scope_attribute_value(attr, **kwargs)

        return ListResult(results=data["results"], count=data["count"])

    def list_instance(self, filter, page, **options):
        kwargs = {}

        # 目前 PAGE 是必定存在的对象，调用方没传，也会有默认值
        kwargs["limit"] = page.limit
        kwargs["offset"] = page.offset

        # 处理 filter 参数
        if filter["parent"] is not None:
            kwargs["resource_filter"] = ResourceFilter(
                resource_type=filter["parent"]["type"], resource_id=filter["parent"]["id"]
            )

        if "keyword" in filter:
            kwargs["search"] = filter["keyword"]

        manager = AuthResourceManager.init(self.resource_type)
        kwargs["only_display"] = True
        data = manager.query(**kwargs)
        return ListResult(results=data["results"], count=data["count"])

    def search_instance(self, filter, page, **options):
        return self.list_instance(filter, page, **options)

    def fetch_instance_info(self, filter, page, **options):
        return ListResult(results=[], count=0)

    def list_instance_by_policy(self, filter, page, **options):
        return ListResult(results=[], count=0)


class BizResourceProvider(BKDataResourceProvider):
    resource_type = "biz"


class ProjectResourceProvider(BKDataResourceProvider):
    resource_type = "project"


class RawDataResourceProvider(BKDataResourceProvider):
    resource_type = "raw_data"


class ResultTableResourceProvider(BKDataResourceProvider):
    resource_type = "result_table"


class FlowResourceProvider(BKDataResourceProvider):
    resource_type = "flow"


class DashboardResourceProvider(BKDataResourceProvider):
    resource_type = "dashboard"


class FunctionResourceProvider(BKDataResourceProvider):
    resource_type = "function"


class DataTokenResourceProvider(BKDataResourceProvider):
    resource_type = "data_token"


class ResourceGroupResourceProvider(BKDataResourceProvider):
    resource_type = "resource_group"


class DataModelResourceProvider(BKDataResourceProvider):
    resource_type = "datamodel"


class ModelResourceProvider(BKDataResourceProvider):
    resource_type = "model"


class SampleSetResourceProvider(BKDataResourceProvider):
    resource_type = "sample_set"


class AlgorithmResourceProvider(BKDataResourceProvider):
    resource_type = "algorithm"
