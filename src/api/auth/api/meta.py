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
from django.conf import settings

from common.api.base import DataAPI


class _MetaApi:
    def __init__(self):
        meta_api_url = settings.META_API_URL
        self.lineage = DataAPI(
            url=meta_api_url + "lineage/",
            method="GET",
            module="meta",
            description="查询血缘关系",
        )
        self.list_result_table = DataAPI(
            url=meta_api_url + "result_tables/", method="GET", module="meta", description="查询结果表列表"
        )
        self.tdw_tables = DataAPI(url=meta_api_url + "tdw/tables/", method="GET", module="meta", description="查询tdw表")
        self.get_result_table_info = DataAPI(
            url=meta_api_url + "result_tables/{result_table_id}/",
            method="GET",
            module="meta",
            url_keys=["result_table_id"],
            description="查询结果表详情",
        )
        self.update_result_table = DataAPI(
            url=meta_api_url + "result_tables/{result_table_id}/",
            method="PUT",
            module="meta",
            url_keys=["result_table_id"],
            description="更新结果表详情",
        )
        self.get_project_info = DataAPI(
            url=meta_api_url + "projects/{project_id}/",
            method="GET",
            module="meta",
            url_keys=["project_id"],
            description="获取项目对应的基本信息",
        )
        self.cluster_group_configs = DataAPI(
            url=meta_api_url + "cluster_group_configs/",
            method="GET",
            module="meta",
            description="查询集群组配置列表",
        )
        self.entity_complex_search = DataAPI(
            url=meta_api_url + "basic/entity/complex_search/", method="POST", module="meta", description="元素复杂查询"
        )
        self.query_via_erp = DataAPI(
            url=meta_api_url + "basic/asset/query_via_erp/", method="POST", module="meta", description="资源查询"
        )


MetaApi = _MetaApi()
