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

summary:
    Configure task.task_loader=command, dm_task decorator can get some information from environment,

    dm_task(delete_extra_heat_entity)
"""

import logging
import os
from functools import wraps


def command_task(raw_func):
    """
    Decorator for presenting the dm_task, make dm_engine know it and initial task in the command task.
    """
    from dm_engine.config import settings
    from dm_engine.env import TASK_CODE_ENVIRONMENT_VARIABLE
    from dm_engine.utils.log import configure_logging

    executing_task_code = os.environ.get(TASK_CODE_ENVIRONMENT_VARIABLE)

    def deco_func(*args, **kwargs):
        configure_logging(
            log_level=settings.LOG_LEVEL,
            log_module=executing_task_code,
            log_dir=settings.LOG_DIR,
            add_root_logger=True,
            report_sentry=settings.REPORT_SENTRY,
            sentry_DSN=settings.SENTRY_DSN,
        )

        logger = logging.getLogger()
        try:
            raw_func(*args, **kwargs)
        except Exception as err:
            logger.exception("DMTask raise exception by itself: {}".format(err))
            raise

    return wraps(raw_func)(deco_func)
