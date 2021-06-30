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

import json

from common.local import get_request_username
from datahub.databus.channel import create_multi_partition_kafka_topic

from ...common.const import CLEAN, RT_ID
from ..settings import CLEAN_TABLE_TOPIC_TEMPLATE
from ..shippers.factory import ShipperFactory


def process_clean_task_in_kafka(rt_info, bk_username=""):
    """
    根据rt_info启动kafka类型清洗任务
    :param rt_info: result_table信息
    :param bk_username: 启动任务的用户
    :return: 成功启动列表["connector_name in cluster_name"]
    """
    if bk_username == "":
        bk_username = get_request_username()
    storage_params = json.dumps({"rt_id": rt_info["rt.id"], "storages": [CLEAN]})
    shipper_task = ShipperFactory.get_shipper_factory().get_shipper(CLEAN)(
        rt_info["rt.id"], bk_username, storage_params
    )
    shipper_task.start()


def _check_clean_task_partition(rt_info, raw_data, source_channel):
    """
    根据rt_info和rawdata 获取清洗分区情况，动态扩容
    :param rt_info: result_table信息
    :param raw_data: rawdata
    :param source_channel: outer集群的channel
    """
    # 获取该dataid对应的所有清洗任务中，获取最大分区数
    kafka_bs = rt_info["bootstrap.servers"]
    max_partition_nums = raw_data.storage_partitions
    rt_id = rt_info[RT_ID]
    # 如果其他清洗任务，分区数大于1，显式创建分区topic
    if max_partition_nums > 1:
        create_multi_partition_kafka_topic(kafka_bs, CLEAN_TABLE_TOPIC_TEMPLATE % rt_id, max_partition_nums)
