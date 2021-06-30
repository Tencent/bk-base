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

from datahub.databus.exceptions import NotJsonFiledError
from datahub.databus.settings import ES_DEFAULT_VERSION, MODULE_SHIPPER
from datahub.databus.shippers.base_shipper import BaseShipper

from datahub.databus import common_helper


class EsShipper(BaseShipper):
    component = "es"
    storage_type = "es"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        cluster_name = (
            self.sink_storage_conn["es_cluster"]
            if "es_cluster" in self.sink_storage_conn
            else self.rt_info["es"]["cluster_name"]
        )
        version = self.rt_info["es"]["version"] if "version" in self.rt_info["es"] else ES_DEFAULT_VERSION

        return self.config_generator.build_es_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            self.task_nums,
            self.source_storage,
            self.rt_info["table_name"],
            self.physical_table_name,
            self.sink_storage_conn["host"],
            self.sink_storage_conn["port"],
            self.sink_storage_conn["transport"],
            cluster_name,
            version,
            self.sink_storage_conn["enable_auth"],
            self.sink_storage_conn.get("user", ""),
            self.sink_storage_conn.get("password", ""),
        )

    @classmethod
    def _field_handler(cls, field, storage_params):
        if field.get("is_analyzed"):
            storage_params.analyzed_fields.append(field["physical_field"])
        if field.get("is_doc_values"):
            storage_params.doc_values_fields.append(field["physical_field"])
        if field.get("is_json"):
            if field["physical_field"] not in storage_params.can_json_fields:
                raise NotJsonFiledError(message_kv={"field": field["physical_field"]})

            storage_params.json_fields.append(field["physical_field"])

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

    @classmethod
    def _get_storage_config(cls, params, storage_params):
        return json.dumps(
            {
                "analyzed_fields": storage_params.analyzed_fields,
                "doc_values_fields": storage_params.doc_values_fields,
                "json_fields": storage_params.json_fields,
                "has_replica": params["config"].get("has_replica", False) if params.get("config") else False,
            }
        )
