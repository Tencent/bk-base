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


from datamanage.pizza_settings import DATABUS_API_ROOT

from common.api.base import DataAPI, DataDRFAPISet


class _DatabusApi(object):
    def __init__(self):
        self.get_connector_clusters = DataAPI(
            url=DATABUS_API_ROOT + "/clusters/",
            method="GET",
            module="databus",
            description="获取总线所有connector集群信息",
        )

        self.channels = DataDRFAPISet(
            url=DATABUS_API_ROOT + "/channels/",
            primary_key="channel_cluster_config_id",
            module="databus",
            description="总线channel集群接口",
        )

        self.migration_tasks = DataAPI(
            url=DATABUS_API_ROOT + "/migrations/get_tasks/",
            method="GET",
            module="databus",
            description="数据迁移",
        )


DatabusApi = _DatabusApi()
