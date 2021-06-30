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
from datahub.common.const import CLEAN
from datahub.databus.settings import MODULE_CLEAN, TYPE_PULSAR
from datahub.databus.shippers.raw_data_shipper import RawDataShipper
from datahub.databus.task.kafka import config as kafka_config
from datahub.databus.task.pulsar import config as pulsar_config
from datahub.databus.task.status import set_clean_status

from datahub.databus import common_helper, model_manager, rawdata


class CleanShipper(RawDataShipper):
    storage_type = "kafka"
    component = CLEAN
    module = MODULE_CLEAN

    def _get_source_channel_topic(self):
        """clean从上游raw_data读取数据"""
        self.raw_data_id = self.rt_info["data.id"]
        self.raw_data = model_manager.get_raw_data_by_id(self.raw_data_id, False)
        return rawdata.get_topic_name(self.raw_data)

    def _before_add_task(self):
        source_channel = model_manager.get_channel_by_id(self.raw_data.storage_channel_id, False)
        # 启动任务时，检查任务分区情况，并创建/扩建分区
        from datahub.databus.task.clean import _check_clean_task_partition

        _check_clean_task_partition(self.rt_info, self.raw_data, source_channel)

    def _get_shipper_task_conf(self, cluster_name):
        data_id = self.rt_info["data.id"]
        target_kafka = self.rt_info["bootstrap.servers"]
        if self.source_type == TYPE_PULSAR:
            return pulsar_config.build_clean_kafka_conf(
                self.connector_name,
                self.source_channel_topic,
                self.rt_id,
                self.raw_data_id,
                self.task_nums,
                target_kafka,
            )
        else:
            return kafka_config.build_clean_kafka_conf(
                self.rt_id,
                cluster_name,
                data_id,
                self.source_channel_topic,
                self.task_nums,
                target_kafka,
            )

    def set_module_status(self, status, conf=None):
        set_clean_status(self.rt_id, self.connector_name, status)

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            ["rt.id", "topics", "tasks.max", "db.dataid", "producer.bootstrap.servers"],
        )
