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
import math
import time

import jobnavi.http_util as http_util
import jobnavi.settings as settings
from jobnavi.jobnavi_logging import get_logger

logger = get_logger()


class HAProxy(object):
    def init(self, config):
        self.config = config
        self.max_retry = int(config.get(settings.JOBNAVI_HA_FAILOVER_RETRY, settings.JOBNAVI_HA_FAILOVER_RETRY_DEFAULT))
        url_string = str(config.get(settings.JOBNAVI_SCHEDULER_ADDRESS))
        if not url_string:
            raise Exception("Param " + settings.JOBNAVI_SCHEDULER_ADDRESS + " must config.")
        self.urls = url_string.split(",")
        self.active_url_index = 0

    def send_request(self, url, method, param, header, retry_times):
        if not self.config:
            raise Exception("HAProxy may not init.")
        max_retry_interval = 60
        max_retry_times = retry_times
        if not retry_times:
            max_retry_times = self.max_retry
        for i in range(1, max_retry_times + 1):
            if "GET" == method:
                content = http_util.http_get(self.urls[self.active_url_index] + url)
            else:
                content = http_util.http_post(self.urls[self.active_url_index] + url, param, header)
            if not content:
                logger.error("http request error.")
                if self.active_url_index < len(self.urls) - 1:
                    self.active_url_index = self.active_url_index + 1
                else:
                    self.active_url_index = 0
                logger.info("Failover to " + self.urls[self.active_url_index])
                retry = math.pow(2, i) / 10
                if retry > max_retry_interval:
                    retry = max_retry_interval
                logger.info("job retry after " + str(retry) + "s....")
                time.sleep(retry)
            else:
                return content


ha_proxy = HAProxy()


def init_ha_proxy(config):
    ha_proxy.init(config)


def get_ha_proxy():
    return ha_proxy
