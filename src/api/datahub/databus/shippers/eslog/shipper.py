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

from datahub.databus.exceptions import TaskStorageConnConfigErr
from datahub.databus.settings import ES_DEFAULT_VERSION, MODULE_SHIPPER
from datahub.databus.shippers.es.shipper import EsShipper

from datahub.databus import common_helper


class EslogShipper(EsShipper):
    component = "eslog"
    storage_type = "es"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        try:
            es_conf = json.loads(self.rt_info["es"]["connection_info"])
        except Exception:
            raise TaskStorageConnConfigErr()
        es_cluster_name = es_conf["es_cluster"] if "es_cluster" in es_conf else self.rt_info["es"]["cluster_name"]
        version = self.rt_info["es"]["version"] if "version" in self.rt_info["es"] else ES_DEFAULT_VERSION
        from datahub.databus.task.task_utils import config_factory

        return config_factory[self.source_type].build_eslog_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            self.task_nums,
            self.source_channel_topic,
            self.rt_info["table_name"],
            es_conf["host"],
            es_conf["port"],
            es_conf["transport"],
            es_cluster_name,
            version,
            es_conf["enable_auth"],
            es_conf.get("user", ""),
            es_conf.get("password", ""),
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
                "es.cluster.name",
                "es.hosts",
                "es.transport.port",
            ],
        )
