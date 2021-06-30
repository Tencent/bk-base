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

from datahub.databus.exceptions import (
    CapacityInsufficient,
    TaskDruidZKEmpty,
    TaskStorageConfigErr,
)
from datahub.databus.settings import MODULE_SHIPPER
from datahub.databus.shippers.base_shipper import BaseShipper

from datahub.databus import common_helper


class DruidShipper(BaseShipper):
    storage_type = "druid"
    module = MODULE_SHIPPER

    def _before_add_task(self):
        from datahub.databus.task.task_utils import check_druid_capactiy_aviavlible

        if not check_druid_capactiy_aviavlible(self.rt_info):
            raise CapacityInsufficient(message_kv={"cluster_name": self.rt_info["druid"]["cluster_name"]})

    def _get_shipper_task_conf(self, cluster_name):
        zk_addr = self.sink_storage_conn["zookeeper.connect"] if "zookeeper.connect" in self.sink_storage_conn else ""
        if not zk_addr:
            raise TaskDruidZKEmpty()

        ts_strategy = "data_time"
        try:
            druid_storage_args = json.loads(self.rt_info[self.storage_type]["storage_config"])
        except Exception:
            raise TaskStorageConfigErr()

        if "timestamp.strategy" in druid_storage_args:
            ts_strategy = druid_storage_args["timestamp.strategy"]

        version = self.rt_info["druid"]["version"] if "version" in self.rt_info["druid"] else "0.11"
        return self.config_generator.build_druid_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            self.source_channel_topic,
            self.task_nums,
            zk_addr,
            ts_strategy,
            version,
            self.physical_table_name,
        )

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            ["rt.id", "topics", "tasks.max", "zookeeper.connect", "timestamp.strategy"],
        )
