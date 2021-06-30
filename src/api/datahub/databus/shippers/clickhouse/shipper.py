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

from clickhouse_driver import Client
from datahub.common.const import (
    CONNECTION_INFO,
    DEFAULT,
    INDEX,
    INDEX_FIELDS,
    PHYSICAL_FIELD,
    PHYSICAL_TABLE_NAME,
    TCP_TGW,
)
from datahub.databus.settings import MODULE_SHIPPER, QUERY_FIELDS_SQL
from datahub.databus.shippers.base_shipper import BaseShipper

from datahub.databus import common_helper


class ClickHouseShipper(BaseShipper):
    storage_type = "clickhouse"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        physical_tn = self.rt_info[self.storage_type][PHYSICAL_TABLE_NAME]
        db_name, replicated_table = (
            physical_tn.split(".")[0],
            "%s_local" % physical_tn.split(".")[1],
        )
        clickhouse_properties = self.rt_info[self.storage_type][CONNECTION_INFO]
        ck_conn = json.loads(clickhouse_properties)
        host, port = ck_conn[TCP_TGW].split(":")[0], int(ck_conn[TCP_TGW].split(":")[1])
        client = Client(host=host, port=port, database=DEFAULT, user=DEFAULT)
        sql = QUERY_FIELDS_SQL.format(db_name, replicated_table)
        clickhouse_col_order = ",".join("{}:{}".format(cell[0], cell[1]) for cell in client.execute(sql))
        return self.config_generator.build_clickhouse_config_param(
            cluster_name,
            self.connector_name,
            self.source_channel_topic,
            self.task_nums,
            self.rt_id,
            db_name,
            replicated_table,
            clickhouse_properties,
            clickhouse_col_order,
        )

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            [
                "group.id",
                "rt.id",
                "connector.class",
                "tasks.max",
                "topics",
                "connector",
                "db.name",
                "replicated.table",
                "clickhouse.properties",
                "clickhouse.column.order",
            ],
        )

    @classmethod
    def _get_storage_config(cls, params, storage_params):
        index_fields = params.get(INDEX_FIELDS)
        order_by = list()
        if index_fields:
            index_fields.sort(key=lambda e: e[INDEX])
            for index_field in index_fields:
                order_by.append(index_field[PHYSICAL_FIELD])
        return json.dumps({"order_by": order_by})
