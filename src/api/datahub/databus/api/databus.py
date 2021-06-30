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
from datahub.databus.settings import DATABUS_API_URL


class _DatabusApi(object):
    def __init__(self):
        self.channels = DataDRFAPISet(
            url=DATABUS_API_URL + "channels/",
            primary_key="channel_name",
            module="databus",
            description="databus channel调用",
            custom_config={},
        )

        self.clusters = DataDRFAPISet(
            url=DATABUS_API_URL + "clusters/",
            primary_key="cluster_name",
            module="databus",
            description="databus cluster调用",
            custom_config={},
        )

        self.tasks = DataDRFAPISet(
            url=DATABUS_API_URL + "tasks/",
            primary_key="result_table_id",
            module="databus",
            description="清洗分发任务",
            custom_config={
                "task_component_list": DRFActionAPI(method="get", url_path="component"),
            },
            default_timeout=60,  # 设置默认超时时间
        )

        self.cleans = DataDRFAPISet(
            url=DATABUS_API_URL + "cleans/",
            primary_key="raw_data_id",
            module="databus",
            description="清洗配置",
            custom_config={},
            default_timeout=60,  # 设置默认超时时间
        )

        self.scenarios = DataDRFAPISet(
            url=DATABUS_API_URL + "scenarios/",
            primary_key=None,
            module="databus",
            description="分发任务",
            default_return_value=None,
            after_request=None,
            custom_config={"setup_shipper": DRFActionAPI(method="post", detail=False)},
        )

        self.rawdatas = DataDRFAPISet(
            url=DATABUS_API_URL + "rawdatas/",
            primary_key="raw_data_id",
            module="databus",
            description="数据源",
            custom_config={"set_partitions": DRFActionAPI(method="post")},
            default_timeout=60,  # 设置默认超时时间
        )

        self.result_tables = DataDRFAPISet(
            url=DATABUS_API_URL + "result_tables/",
            primary_key="result_table_id",
            module="databus",
            description="结果表",
            custom_config={"set_partitions": DRFActionAPI(method="post")},
            default_timeout=60,  # 设置默认超时时间
        )


DatabusApi = _DatabusApi()
