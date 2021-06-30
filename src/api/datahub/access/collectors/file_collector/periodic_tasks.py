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
import os
import time

from celery.schedules import crontab
from celery.task import periodic_task
from common.log import logger
from datahub.access.settings import UPLOAD_MEDIA_ROOT


@periodic_task(run_every=crontab(hour=2, minute=30, day_of_week=1))
def periodic_task_remove_file():
    logger.info("start cron remove dir file,dir : %s" % UPLOAD_MEDIA_ROOT)
    try:
        files = os.listdir(UPLOAD_MEDIA_ROOT)
        if files:
            for _file in files:
                file_path = os.path.join(UPLOAD_MEDIA_ROOT, _file)
                logger.info("remove file,file_path : %s" % file_path)
                # 判断是否是文件
                if os.path.isfile(file_path):
                    # 最后一次修改的时间
                    last = int(os.stat(file_path).st_mtime)
                    now = int(time.time())
                    if now - last >= (7 * 24 * 60 * 60):
                        os.remove(file_path)
                        logger.logger(u"cron remove file, file:%s" % file_path)
        else:
            logger.logger(u"dir:%s is empty" % dir)
    except Exception as e:
        logger.error(u"cron remove file happend exception" % str(e))
