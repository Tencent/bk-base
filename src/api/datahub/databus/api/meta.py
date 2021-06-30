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

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from datahub.databus.settings import META_API_URL


class _MetaApi(object):
    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=META_API_URL + "result_tables/",
            primary_key="result_table_id",
            module="meta",
            description="获取结果表元信息",
            custom_config={
                "storages": DRFActionAPI(method="get"),
                "fields": DRFActionAPI(method="get"),
                "lineage": DRFActionAPI(method="get"),
            },
        )

        self.data_processings = DataDRFAPISet(
            url=META_API_URL + "data_processings/",
            primary_key="processing_id",
            module="meta",
            description="获取数据处理元信息",
            custom_config={},
        )

        self.data_transferrings = DataDRFAPISet(
            url=META_API_URL + "data_transferrings/",
            primary_key="transferring_id",
            module="meta",
            description="获取数据传输信息",
            custom_config={},
        )

        self.target = DataDRFAPISet(
            url=META_API_URL + "tag/targets/",
            primary_key=None,
            module="meta",
            description="检索实体&标签&关联信息",
            custom_config={},
        )

        self.meta_transaction = DataAPI(
            url=META_API_URL + "meta_transaction/",
            method="POST",
            module="meta",
            description="集合事务接口",
        )


MetaApi = _MetaApi()
