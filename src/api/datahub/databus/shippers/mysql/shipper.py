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

from datahub.databus.settings import MODULE_SHIPPER
from datahub.databus.shippers.base_shipper import BaseShipper


class MysqlShipper(BaseShipper):
    storage_type = "mysql"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        # physical_table_name 格式 "dbname_123.table_name"
        arr = self.physical_table_name.split(".")
        if len(arr) == 1:
            db_name = "mapleleaf_%s" % (self.rt_info["bk_biz_id"])
            table_name = self.physical_table_name
        else:
            db_name = arr[0]
            table_name = arr[1]
        conn_url = (
            "jdbc:mysql://{}:{}/{}?autoReconnect=true&useServerPrepStmts=false&rewriteBatchedStatements=true".format(
                self.sink_storage_conn["host"],
                self.sink_storage_conn["port"],
                db_name,
            )
        )
        return self.config_generator.build_tspider_config_param(
            cluster_name,
            self.connector_name,
            self.rt_id,
            self.source_channel_topic,
            self.task_nums,
            conn_url,
            self.sink_storage_conn["user"],
            self.sink_storage_conn["password"],
            table_name,
        )

    @classmethod
    def _field_handler(cls, field, storage_params):
        if field.get("is_index"):
            storage_params.indexed_fields.append(field["physical_field"])

    @classmethod
    def _get_storage_config(cls, params, storage_params):
        return json.dumps(
            {
                "indexed_fields": storage_params.indexed_fields,
            }
        )
