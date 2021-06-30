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
from __future__ import absolute_import, unicode_literals

from api.base import DataAPI, DataDRFAPISet
from conf.settings import META_API_URL


class MetaApi(object):
    MODULE = "META"

    def __init__(self):
        self.bizs = DataDRFAPISet(
            url=META_API_URL + "bizs/",
            primary_key="bk_biz_id",
            module="meta",
            description="业务详情",
        )

        self.projects = DataDRFAPISet(
            url=META_API_URL + "projects/",
            primary_key="project_id",
            module="meta",
            description="项目详情",
        )

        self.query_tdw_tables = DataAPI(
            url=META_API_URL + "tdw/tables/",
            method="GET",
            module="meta",
            description="查询TDW表",
        )

        self.complex_search = DataAPI(
            url=META_API_URL + "basic/entity/complex_search/",
            method="POST",
            module="meta",
            description="元数据后端查询语言",
        )

        self.lineage = DataAPI(
            url=META_API_URL + "lineage/",
            method="GET",
            module="meta",
            description="血缘关系",
        )

        self.result_tables = DataDRFAPISet(
            url=META_API_URL + "result_tables/",
            primary_key="result_table_id",
            module="meta",
            description="结果表详情",
        )

        self.report_events = DataAPI(
            url=META_API_URL + "events/",
            method="POST",
            module="meta",
            description="事件上报",
        )
