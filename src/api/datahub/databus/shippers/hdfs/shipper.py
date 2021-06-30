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
from datahub.common.const import (
    BK_BIZ_ID,
    DATA_TYPE,
    GEOG_AREA,
    HDFS,
    ICEBERG,
    STORAGES,
)
from datahub.databus.settings import MODULE_SHIPPER
from datahub.databus.shippers.base_shipper import BaseShipper
from datahub.storekit.iceberg import construct_hdfs_conf

from datahub.databus import common_helper, rt


class HdfsShipper(BaseShipper):
    storage_type = "hdfs"
    module = MODULE_SHIPPER

    def _get_shipper_task_conf(self, cluster_name):
        data_type = rt.get_rt_fields_storages(self.rt_id)[STORAGES][HDFS][DATA_TYPE]
        # 根据hdfs的connection info构建hdfs的配置参数
        custom_conf = construct_hdfs_conf(self.sink_conn_info, self.rt_info[GEOG_AREA], data_type)
        if data_type == ICEBERG:
            return self.config_generator.build_hdfs_iceberg_config_param(
                cluster_name,
                self.rt_id,
                self.source_channel_topic,
                self.task_nums,
                self.physical_table_name,
                custom_conf,
            )
        else:
            # 这里physical_table_name可能是完整的hdfs上的路径，需要截取最后一段作为table_name
            return self.config_generator.build_hdfs_config_param(
                cluster_name,
                self.rt_id,
                self.task_nums,
                self.source_channel_topic,
                custom_conf,
                self.rt_info[BK_BIZ_ID],
                self.physical_table_name.split("/")[-1],
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
                "topics.dir",
                "logs.dir",
                "hdfs.url",
                "hadoop.conf.dir",
                "flush.size",
                "table.name",
            ],
        )
