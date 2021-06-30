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

from common.api.base import DataAPI
from conf.dataapi_settings import CC_V3_API_URL


class _CC3Api(object):
    MODULE = "cc3"

    def __init__(self):
        self.search_business = DataAPI(
            method="POST",
            url=CC_V3_API_URL + "search_business/",
            module=self.MODULE,
            description=u"查询业务信息",
        )
        self.search_host = DataAPI(
            method="POST",
            url=CC_V3_API_URL + "search_host/",
            module=self.MODULE,
            description=u"查询主机信息",
        )
        self.get_biz_location = DataAPI(
            method="POST",
            url=CC_V3_API_URL + "get_biz_location/",
            module=self.MODULE,
            description=u"查询业务所属CC",
        )


CC3Api = _CC3Api()
