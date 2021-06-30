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
from datahub.common.const import ALTER_ID, DATAMANAGE, METRIC_ID, QUERY, REPORT
from datahub.storekit.settings import DATAMANAGE_API_URL


class _DataManageApi(object):
    def __init__(self):
        self.alerts = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "/alerts/",
            primary_key=ALTER_ID,
            module=DATAMANAGE,
            description="发送告警",
            custom_config={"send": DRFActionAPI(method="post", detail=False)},
            default_timeout=60,  # 设置默认超时时间
        )

        self.metrics = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "/metrics/",
            primary_key=METRIC_ID,
            module=DATAMANAGE,
            description="指标数据",
            custom_config={
                REPORT: DRFActionAPI(method="post", detail=False),
                QUERY: DRFActionAPI(method="post", detail=False),
            },
            default_timeout=60,  # 设置默认超时时间
        )


DataManageApi = _DataManageApi()
