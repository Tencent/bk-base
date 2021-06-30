# -*- coding: utf-8 -*
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

import random
import threading
import time

from common.log import logger
from django.apps import AppConfig

from datahub.databus import settings


class DatabusConfig(AppConfig):
    # 初始化时注册连接信号
    name = "databus"
    verbose_name = "databus.config"

    def ready(self):
        if settings.RUN_VERSION == "tencent":
            # 对于tencent版，通过其他的方式，比如检查脚本或者connector manager定期触发检查逻辑
            logger.info("not start databus cluster checking for this version")
            return
        # 这里在启动server时，会被调用一次，由于gunicorn起了多个进程，这里会被多次触发
        check_thread = threading.Thread(target=self.check_loop, name="DatabusCheckThread")
        check_thread.setDaemon(True)
        check_thread.start()

    def check_loop(self):
        from datahub.databus.cluster import check_clusters
        from datahub.databus.models import DatabusCluster

        # 触发所有总线集群的检查，等待一个随机时间，将多个进程之间的检查错开
        first_sleep_time = random.random() * 600
        time.sleep(first_sleep_time)
        while True:
            try:
                clusters = DatabusCluster.objects.all()
                result = check_clusters(clusters)
                logger.info("databus cluster checking result: %s" % result)
            except Exception as e:
                logger.exception(e, "databus cluster checking failed!")
            time.sleep(1800)  # 等待一段时间再次检查
