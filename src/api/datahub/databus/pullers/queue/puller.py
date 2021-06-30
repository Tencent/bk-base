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

from datahub.common.const import AUTO_OFFSET_RESET, KAFKA, LATEST
from datahub.databus.exceptions import TaskDataScenarioNotSupport
from datahub.databus.pullers.base_puller import BasePuller
from datahub.databus.settings import MODULE_PULLER, TYPE_PULSAR
from datahub.databus.task.pulsar import config as pulsar_config

from datahub.databus import common_helper


class QueuePuller(BasePuller):
    component = "db"
    module = MODULE_PULLER

    def _before_add_task(self):
        resource_type = self.resource_json.get("type", "unknown_type")
        if resource_type != "kafka":
            raise TaskDataScenarioNotSupport(message_kv={"Scenario": self.raw_data.data_scenario + "_" + resource_type})
        super(QueuePuller, self)._before_add_task()

    def _get_puller_task_conf(self):
        if self.storage_channel.cluster_type == TYPE_PULSAR:
            return self.__builder_pulsar_conf()
        else:
            return self.__builder_kafka_conf()

    def _get_source_id(self):
        return self.sink_topic

    def __builder_pulsar_conf(self):
        if self.resource_json.get("use_sasl", False):
            return pulsar_config.build_puller_kafka_config_with_sasl(
                self.data_id,
                self.connector_name,
                self.resource_json["master"],
                self.resource_json["group"],
                self.resource_json["topic"],
                self.sink_topic,
                self.resource_json.get("tasks", 1),
                self.resource_json["security_protocol"],
                self.resource_json["sasl_mechanism"],
                self.resource_json["user"],
                self.resource_json["password"],
                self.resource_json.get(AUTO_OFFSET_RESET, LATEST),
            )
        else:
            return pulsar_config.build_puller_kafka_config_param(
                self.data_id,
                self.connector_name,
                self.resource_json["master"],
                self.resource_json["group"],
                self.resource_json["topic"],
                self.sink_topic,
                self.resource_json.get("tasks", 1),
                self.resource_json.get(AUTO_OFFSET_RESET, LATEST),
            )

    def __builder_kafka_conf(self):
        kafka = self.storage_channel
        kafka_bs = "{}:{}".format(kafka.cluster_domain, kafka.cluster_port)
        param = {
            "data_id": self.data_id,
            "cluster_name": self.cluster_name,
            "group": self.resource_json["group"],
            "max_tasks": self.resource_json.get("tasks", "1"),
            "src_topic": self.resource_json["topic"],
            "src_server": self.resource_json["master"],
            "dest_topic": self.sink_topic,
            "dest_server": kafka_bs,
            AUTO_OFFSET_RESET: self.resource_json.get(AUTO_OFFSET_RESET, LATEST),
        }
        if self.resource_json.get("use_sasl", False):
            param["security_protocol"] = self.resource_json["security_protocol"]
            param["sasl_mechanism"] = self.resource_json["sasl_mechanism"]
            param["user"] = self.resource_json["user"]
            param["password"] = self.resource_json["password"]
        return self.config_factory[KAFKA].build_puller_kafka_config_param(param)

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            [
                "rt.id",
                "topics",
                "tasks.max",
            ],
        )
