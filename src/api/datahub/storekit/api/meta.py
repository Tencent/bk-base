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


from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from datahub.common.const import FIELDS, META, RESULT_TABLE_ID, STORAGES
from datahub.storekit.settings import META_API_URL


class _MetaApi(object):
    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=META_API_URL + "/result_tables/",
            primary_key=RESULT_TABLE_ID,
            module=META,
            description="获取结果表元信息",
            custom_config={STORAGES: DRFActionAPI(method="get"), FIELDS: DRFActionAPI(method="get")},
            default_timeout=60,  # 设置默认超时时间
        )

        self.tag_target = DataDRFAPISet(
            url=META_API_URL + "/tag/targets/", primary_key=None, module=META, description="标签实体列表", custom_config={}
        )

        self.tag = DataDRFAPISet(
            url=META_API_URL + "/tag/geog_tags/", primary_key=None, module=META, description="区域标签列表", custom_config={}
        )

        self.tdw = DataDRFAPISet(
            url=META_API_URL + "/tdw/app_groups/mine/",
            primary_key=None,
            module=META,
            description="应用组id",
            before_request=add_app_info_before_request,
            custom_config={},
        )

        self.lineage = DataDRFAPISet(
            url=META_API_URL + "/lineage/",
            primary_key=None,
            module=META,
            description="id",
            before_request=add_app_info_before_request,
            default_timeout=300,  # 设置默认超时时间
            custom_config={},
        )


MetaApi = _MetaApi()
