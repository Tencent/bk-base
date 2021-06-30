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
import logging

from dataquality.sample.task import SamplingConsumeTaskGreenlet
from gevent import monkey

monkey.patch_all()


def sampling_consume_datasets(params):
    logging.info("Start to execute sampling consume kafka task")

    sampling_task_config = {
        "consume_count": 100,  # 每次采样数据量
        "consume_interval": 60,  # 每次采样间隔时间
        "consume_hash_values": params.get("consume_hash_values", [0]),  # 采样结果表的hash值
        "consume_task_count": params.get("consume_task_count", 1),  # 采样任务总数量
        "produce_bk_biz_id": params.get("produce_bk_biz_id", 2),  # 生产数据的业务ID
        "produce_raw_data_name": params.get(
            "produce_raw_data_name", "bkdata_sampled_datasets"
        ),
        "produce_partition_count": params.get(
            "produce_partition_count", 1
        ),  # 生产数据的分区数量
        "filtered_data_sets": params.get("filtered_data_sets", []),  # 需要进行过滤的数据集
        "task_pool_size": 300,  # 采样任务协程池大小
        "heat_score": None,  # 采样数据集热度阈值
        "heat_rate": None,  # 采样数据集热度比例
        "recent_data_time": 3600,  # 采样数据集最近有数据的时间
        "multiple_partitions": False,
    }

    try:
        task = SamplingConsumeTaskGreenlet(sampling_task_config)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init sampling consume task".format(error=e),
            exc_info=True,
        )
