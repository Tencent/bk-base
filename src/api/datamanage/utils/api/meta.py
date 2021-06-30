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


from datamanage.pizza_settings import META_API_ROOT

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI


class _MetaApi(object):
    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=META_API_ROOT + "/result_tables/",
            primary_key="result_table_id",
            module="meta",
            description="获取结果表元信息",
            default_timeout=60,
            custom_config={
                "fields": DRFActionAPI(method="get", detail=True),
                "storages": DRFActionAPI(method="get", detail=True),
            },
        )

        self.data_processings = DataDRFAPISet(
            url=META_API_ROOT + "/data_processings/",
            primary_key="processing_id",
            module="meta",
            description="获取数据处理元信息",
            default_timeout=60,
        )

        self.complex_search = DataAPI(
            url=META_API_ROOT + "/basic/entity/complex_search",
            method="POST",
            module="meta",
            description="元数据后端查询语言",
            default_timeout=300,
        )

        self.search_target_tag = DataAPI(
            url=META_API_ROOT + "/tag/targets/",
            method="GET",
            module="meta",
            description="获取某个实体标签",
            default_timeout=60,
        )

        self.get_geog_area_tags = DataAPI(
            url=META_API_ROOT + "/tag/geog_tags/",
            method="GET",
            module="meta",
            description="获取所有地域标签",
        )

        self.projects = DataDRFAPISet(
            url=META_API_ROOT + "/projects/",
            primary_key="project_id",
            module="meta",
            description="获取项目信息",
        )

        self.lineage = DataDRFAPISet(
            url="%s/lineage/" % META_API_ROOT,
            primary_key="guid",
            module="meta",
            description="血缘关系",
            custom_config={},
        )

        self.query_via_erp = DataAPI(
            url=META_API_ROOT + "/basic/asset/query_via_erp/",
            method="POST",
            module="meta",
            description="erp查询",
            default_timeout=300,
        )

        self.get_bizs = DataAPI(
            method="GET",
            url=META_API_ROOT + "/bizs/",
            module="meta",
            url_keys=[],
            description="meta",
        )

        self.query_via_erp = DataAPI(
            url=META_API_ROOT + "/basic/asset/query_via_erp/",
            method="POST",
            module="meta",
            description="erp查询",
            default_timeout=300,
        )

        self.tagged = DataAPI(method="POST", url=META_API_ROOT + "/tag/targets/tagged/", module="meta")

        self.untagged = DataAPI(
            url=META_API_ROOT + "/tag/targets/untagged/",
            method="POST",
            module="meta",
            description="删除给实体打的标签",
            default_timeout=300,
        )

        self.query_tag_targets = DataAPI(method="GET", url=META_API_ROOT + "/tag/targets/", module="meta")

        self.get_target_tag = DataAPI(
            url=META_API_ROOT + "/tag/targets/filter/",
            method="POST",
            module="meta",
            description="查实体对应标签",
            default_timeout=300,
        )

        self.get_field_type_configs = DataAPI(
            url=META_API_ROOT + "/field_type_configs/",
            method="GET",
            module="meta",
            description="字段数据类型",
            default_timeout=300,
            cache_time=600,
        )

        self.data_traces = DataDRFAPISet(
            url=META_API_ROOT + "/data_traces/",
            primary_key="data_set_id",
            module="meta",
            description="获取数据集数据足迹",
            default_timeout=60,
            custom_config={},
        )


MetaApi = _MetaApi()
