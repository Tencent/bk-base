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

from datahub.databus.pullers.base_puller import BasePuller
from datahub.databus.settings import MODULE_PULLER


class DbPuller(BasePuller):
    component = "jdbc"
    module = MODULE_PULLER

    def _get_puller_task_conf(self):
        channel_host = "{}:{}".format(
            self.storage_channel.cluster_domain,
            self.storage_channel.cluster_port,
        )
        encoding = self.raw_data.data_encoding.upper()
        return self.config_factory.build_puller_jdbc_config_param(
            self.data_id,
            self.cluster_name,
            self.connector_name,
            self.resource_info,
            self.sink_topic,
            channel_host,
            encoding,
        )

    def _get_source_id(self):
        return self.resource_json["table_name"]
