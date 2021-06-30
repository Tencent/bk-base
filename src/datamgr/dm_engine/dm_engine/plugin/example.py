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

import logging
import multiprocessing
import time
from datetime import datetime

from dm_engine.tasks.imports import ImportTaskCls, import_task_deco

logger = logging.getLogger(__name__)


@import_task_deco
def hello():
    info = """Hello DMEngine"""
    logger.info(info)


class FlyTask(ImportTaskCls):
    def run(self):
        info = """
           ___
       _,-' ______
     .'  .-'  ____7
    /   /   ___7
  _|   /  ___7
>(')\\ | ___7
  \\/     \\_______
  '        _======>
  `'----\\`
        """
        logger.info(info)


class SleepTask(ImportTaskCls):
    def run(self):
        while 1:
            logger.info("Current time: {}".format(datetime.now()))
            time.sleep(2)


class ParamsTask(ImportTaskCls):
    def run(self, params):
        a = params["aa"]
        b = params["bb"]

        print("Param A:{}, B:{}".format(a, b))
        logger.info(f"[ParamsTask][{multiprocessing.current_process().pid}] Succeed to execute ...")
