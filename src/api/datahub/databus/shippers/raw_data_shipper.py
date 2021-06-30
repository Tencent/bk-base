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
from datahub.databus.shippers.base_shipper import BaseShipper
from datahub.databus.task import task_utils

from datahub.databus import model_manager, rawdata


class RawDataShipper(BaseShipper):
    """RawDataShipper代表该shipper不从rt表的结果数据读取数据，而是从raw_data读取原始数据"""

    def _builder_source_info(self):
        # 当前cluster_type都为kafka/pulsar
        raw_data = model_manager.get_raw_data_by_id(self.rt_info["data.id"], False)
        # eslog从source中读取数据
        self.source_channel_topic = rawdata.get_topic_name(raw_data)
        self.channel_info = task_utils.get_channel_info_by_id(raw_data.storage_channel_id)
        self.source_type = self.channel_info.cluster_type
        self.source_storage = self.channel_info.cluster_name
