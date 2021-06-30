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
from datahub.common.const import (
    CREATE_CLUSTER,
    GET_CLUSTER,
    RESOURCECENTER,
    SRC_CLUSTER_ID,
    UPDATE_CLUSTER,
)
from datahub.storekit.settings import RESOURCE_CENTER_API_URL


class _ResourceCenterApi(object):
    def __init__(self):
        self.cluster_register = DataDRFAPISet(
            url=RESOURCE_CENTER_API_URL + "/cluster_register/",
            module=RESOURCECENTER,
            primary_key=SRC_CLUSTER_ID,
            description="资源中心",
            custom_config={
                CREATE_CLUSTER: DRFActionAPI(method="post", detail=False),
                UPDATE_CLUSTER: DRFActionAPI(method="put", detail=False),
                GET_CLUSTER: DRFActionAPI(method="get", detail=False),
            },
        )


ResourceCenterApi = _ResourceCenterApi()
