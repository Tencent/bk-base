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
from datahub.databus.exceptions import AddAuthErr
from datahub.databus.settings import MODULE_SHIPPER
from datahub.databus.shippers.base_shipper import BaseShipper
from datahub.databus.task.task_utils import queue_task_process

from datahub.databus import common_helper


class QueueShipper(BaseShipper):
    storage_type = "queue"
    module = MODULE_SHIPPER

    def _before_add_task(self):
        if queue_task_process(self.storage_type, self.rt_info) is False:
            raise AddAuthErr(message_kv={"task": self.connector_name, "type": self.storage_type})

    def _get_shipper_task_conf(self, cluster_name):
        dest_kafka = "{}:{}".format(
            self.sink_storage_conn["host"],
            self.sink_storage_conn["port"],
        )
        topic_prefix = self.sink_storage_conn["topic_prefix"] if "topic_prefix" in self.sink_storage_conn else "queue_"
        sasl_config = {
            "use.sasl": self.sink_storage_conn.get("use.sasl", True),
            "sasl.user": self.sink_storage_conn.get("sasl.user", ""),
            "sasl.pass": self.sink_storage_conn.get("sasl.pass", ""),
        }
        return self.config_generator.build_queue_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            self.source_channel_topic,
            self.task_nums,
            dest_kafka,
            topic_prefix,
            sasl_config,
        )

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            [
                "rt.id",
                "topics",
                "tasks.max",
                "producer.bootstrap.servers",
                "dest.topic.prefix",
            ],
        )
