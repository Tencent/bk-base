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

from common.api.base import DataDRFAPISet
from common.local import get_request_username

from datahub import pizza_settings

MEATA_API_URL = pizza_settings.META_API_URL


def add_app_info_before_request(params):
    params["bk_username"] = get_request_username()
    return params


class _MetaApi(object):

    MODULE = "meta"

    def __init__(self):
        self.meta = DataDRFAPISet(
            url=MEATA_API_URL + "dm_category_configs/",
            primary_key="",
            module=self.MODULE,
            description="分类数据数据操作",
            before_request=add_app_info_before_request,
            custom_config={},
        )
        self.targets = DataDRFAPISet(
            url=MEATA_API_URL + "tag/targets/",
            primary_key=None,
            module=self.MODULE,
            description="标签实体列表",
            before_request=add_app_info_before_request,
            custom_config={},
        )
        self.tag = DataDRFAPISet(
            url=MEATA_API_URL + "tag/geog_tags/",
            primary_key=None,
            module="meta",
            description="区域标签列表",
            custom_config={},
        )


MetaApi = _MetaApi()
