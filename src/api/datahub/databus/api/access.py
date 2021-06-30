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

from common.api.base import DataDRFAPISet, DRFActionAPI
from datahub.databus.settings import ACCESS_API_URL


class _AccessApi(object):
    def __init__(self):
        self.rawdata = DataDRFAPISet(
            url=ACCESS_API_URL + "rawdata/",
            primary_key="raw_data_id",
            module="access",
            description="获取rawdata信息",
            custom_config={
                "add_storage_channel": DRFActionAPI(method="post", url_path="add_storage_channel"),
            },
        )
        self.data_struct = DataDRFAPISet(
            url=ACCESS_API_URL + "collector/data_struct/",
            primary_key="bk_biz_id",
            module="access",
            description="获取rawdata对应的数据结构",
            custom_config={
                "detail": DRFActionAPI(method="get", url_path="detail"),
            },
            default_timeout=60,  # 设置默认超时时间
        )


AccessApi = _AccessApi()
