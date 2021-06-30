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

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_META_URL

from .test.test_call_meta import TestMeta


class _MetaApi(object):

    test_meta = TestMeta()

    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=BASE_META_URL + "result_tables/",
            primary_key="result_table_id",
            module="meta",
            description=_("获取结果表元信息"),
            default_return_value=self.test_meta.set_return_value("result_tables"),
            before_request=add_app_info_before_request,
            custom_config={
                "storages": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_meta.set_return_value("storages"),
                ),
                "fields": DRFActionAPI(
                    method="get",
                    default_return_value=self.test_meta.set_return_value("fields"),
                ),
            },
        )

        self.data_processings = DataDRFAPISet(
            url=BASE_META_URL + "data_processings/",
            primary_key="processing_id",
            module="meta",
            description=_("获取数据处理表元信息"),
            default_return_value=self.test_meta.set_return_value("data_processings"),
            before_request=add_app_info_before_request,
            custom_config={"bulk": DRFActionAPI(method="delete", detail=False)},
        )

        self.projects = DataDRFAPISet(
            url=BASE_META_URL + "projects/",
            primary_key="project_id",
            module="meta",
            description=_("获取项目元信息"),
            default_return_value=self.test_meta.set_return_value("retrieve_projects"),
            before_request=add_app_info_before_request,
        )

        self.data_transferrings = DataDRFAPISet(
            url=BASE_META_URL + "data_transferrings/",
            primary_key="transferring_id",
            module="meta",
            default_return_value=self.test_meta.set_return_value("data_transferrings"),
            description=_("数据传输元信息"),
            before_request=add_app_info_before_request,
        )

        self.meta_transaction = DataDRFAPISet(
            url=BASE_META_URL + "meta_transaction/",
            primary_key=None,
            module="meta",
            description=_("MetaApi集合事务接口"),
            default_return_value=self.test_meta.set_return_value("meta_transaction"),
            before_request=add_app_info_before_request,
        )

        self.lineage = DataAPI(
            url=BASE_META_URL + "lineage/",
            method="GET",
            module="meta",
            description=_("查询血缘关系"),
        )

        self.tdw_app_group = DataDRFAPISet(
            url=BASE_META_URL + "tdw/app_groups/",
            primary_key="app_group_name",
            module="meta",
            default_return_value=self.test_meta.set_return_value("data_transferrings"),
            description=_("获取tdw应用组信息"),
            custom_config={"mine": DRFActionAPI(method="get", detail=False)},
        )

        self.cluster_group_configs = DataDRFAPISet(
            url=BASE_META_URL + "cluster_group_configs/",
            primary_key="cluster_group_id",
            module="meta",
            description="获取集群组详情",
        )

        self.tag = DataDRFAPISet(
            url=BASE_META_URL + "tag/",
            primary_key=None,
            module="meta",
            description="获取集群组详情",
            custom_config={"geog_tags": DRFActionAPI(method="get", detail=False)},
        )

        self.field_type_configs = DataDRFAPISet(
            url=BASE_META_URL + "field_type_configs/",
            primary_key="field_type",
            module="meta",
            description="获取字段类型配置",
        )

        self.assets = DataDRFAPISet(
            url=BASE_META_URL + "basic/asset/",
            primary_key=None,
            module="meta",
            description="元数据 asset",
            custom_config={"query_via_erp": DRFActionAPI(method="post", detail=False)},
        )


MetaApi = _MetaApi()
