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

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI


class _MetaApi:
    def __init__(self):
        meta_api_url = settings.META_API_URL

        self.sync_hook = DataAPI(
            url=meta_api_url + "sync_hook/",
            method="POST",
            module="meta",
            description="创建DB操作记录",
        )
        self.lineage = DataAPI(
            url=meta_api_url + "lineage/",
            method="GET",
            module="meta",
            description="查询血缘关系",
        )
        self.content_language_configs = DataDRFAPISet(
            url=meta_api_url + "content_language_configs/",
            primary_key="id",
            module="meta",
            custom_config={
                "translate": DRFActionAPI(method="get", detail=False),
            },
            description="语言对照配置表",
        )
        self.entity_complex_search = DataAPI(
            method="POST",
            url=meta_api_url + "basic/entity/complex_search/",
            module="meta",
            description="原生后端查询语言搜索",
        )
        self.bizs = DataDRFAPISet(
            url=meta_api_url + "bizs/",
            primary_key="bk_biz_id",
            module="meta",
            description="业务接口",
        )


MetaApi = _MetaApi()
