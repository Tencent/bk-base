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

from datahub.databus.exceptions import NotSupportErr
from datahub.databus.pullers.base_puller import BasePuller
from datahub.databus.settings import MODULE_PULLER, TYPE_PULSAR

from datahub.databus import common_helper


class HttpPuller(BasePuller):
    component = "http"
    module = MODULE_PULLER

    def _get_puller_task_conf(self):
        method = str(self.resource_json["method"])
        period = int(self.resource_info.period) * 60  # minutes to sends
        start, end, url = common_helper.split_url(str(self.resource_json["url"]))
        time_format = ""
        if start or end:
            time_format = str(self.resource_info.time_format)

        if self.storage_channel.cluster_type == TYPE_PULSAR:
            return self.config_factory[TYPE_PULSAR].build_puller_http_config_param(
                self.connector_name,
                self.sink_topic,
                self.data_id,
                method,
                url,
                time_format,
                start,
                end,
                period,
            )
        else:
            raise NotSupportErr()

    def _get_source_id(self):
        return self.component
