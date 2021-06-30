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

from datahub.databus.settings import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_THIN_CLIENT_THRESHOLD,
    MODULE_SHIPPER,
)
from datahub.databus.shippers.base_shipper import BaseShipper


class IgniteShipper(BaseShipper):
    storage_type = "ignite"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        storage_config = json.loads(self.rt_info[self.storage_type]["storage_config"])
        key_fields = ",".join(storage_config["storage_keys"])
        key_separator = storage_config.get("key_separator", "")
        max_records = storage_config.get("max_records", 100000)
        threshold = self.sink_storage_conn.get("thin.client.threshold", DEFAULT_THIN_CLIENT_THRESHOLD)
        use_thin_client = int(max_records) < int(threshold)
        cache_name = self.rt_info[self.storage_type]["physical_table_name"].split(".")[1]
        cluster_name = self.rt_info[self.storage_type]["cluster_name"]
        host = self.sink_storage_conn["host"]
        password = self.sink_storage_conn["password"]
        port = self.sink_storage_conn["port"]
        user = self.sink_storage_conn["user"]

        return self.config_generator.build_ignite_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            key_fields,
            key_separator,
            max_records,
            cache_name,
            cluster_name,
            host,
            password,
            port,
            user,
            use_thin_client,
            self.task_nums,
        )

    @classmethod
    def _get_storage_config(cls, params, storage_params):
        return json.dumps(
            {
                "indexed_fields": storage_params.indexed_fields,
                "storage_keys": storage_params.key_fields,
                "scenario_type": "join",
                "max_records": params["config"].get("max_records", DEFAULT_MAX_RECORDS)
                if params.get("config")
                else DEFAULT_MAX_RECORDS,
            }
        )
