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
import logging.config
import os
from datetime import datetime
from functools import wraps

from command.api.datalab import get_notebook_id
from command.constants import (
    CONTENT,
    DATA,
    DATALAB_PROJECT,
    EXEC_MESSAGE,
    MESSAGE,
    RESULT,
    RESULT_TABLE_IDS,
)
from command.exceptions import MagicException
from command.settings import JUPYTERHUB_USER
from command.utils import get_bk_username
from pythonjsonlogger import jsonlogger  # noqa


class BaseLogger(object):
    def __init__(self, **kwargs):
        self.log_items = kwargs.get("log_items")
        self.log_items["response_ms"] = int((datetime.now() - self.log_items["requested_at"]).total_seconds() * 1000)
        self.log_items["requested_at"] = self.log_items["requested_at"].strftime("%Y-%m-%d %H:%M:%S")

    def write(self):
        logger = self.get_logger()
        logger.info("magic log", extra=self.log_items)

    def get_logger(self):
        conf = {
            "version": 1,
            "formatters": {
                "json": {
                    "format": """%(content)s: %(bk_username)s: %(content_type)s: %(result_table_ids)s: %(project_id)s:
                                %(notebook_id)s: %(requested_at)s: %(result)s: %(exec_message)s: %(response_ms)s:
                                %(message)s""",
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                }
            },
            "handlers": {
                "magic_handler": {
                    "level": "INFO",
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "json",
                    "filename": "/home/datalab/notebooks/magic.log",
                    "maxBytes": 1024 * 1024 * 10,
                    "backupCount": 1,
                    "encoding": "utf8",
                },
            },
            "loggers": {"magic_logger": {"handlers": ["magic_handler"], "level": "INFO", "propagate": True}},
        }
        logging.config.dictConfig(conf)
        logger = logging.getLogger("magic_logger")
        return logger

    @staticmethod
    def save_log_deco(content_type):
        """
        日志保存装饰器

        :param content_type: 语句类型
        """

        def _deco(func):
            @wraps(func)
            def _wrap(*args, **kwargs):
                requested_at = datetime.now()
                project_id = JUPYTERHUB_USER if JUPYTERHUB_USER.isdigit() else os.environ.get(DATALAB_PROJECT.upper())
                content = func.__qualname__ + (
                    str(args) if args else "(%s)" % ", ".join("{}={}".format(k, v) for k, v in kwargs.items())
                )
                log_items = dict(
                    content=content,
                    content_type=content_type,
                    requested_at=requested_at,
                    bk_username=get_bk_username(),
                    project_id=project_id,
                    notebook_id=get_notebook_id(),
                )
                try:
                    _f = func(*args, **kwargs)
                    log_items.update(
                        {
                            RESULT: _f.get(RESULT, True),
                            EXEC_MESSAGE: _f.get(MESSAGE),
                            CONTENT: _f.get(CONTENT, content),
                            RESULT_TABLE_IDS: _f.get(RESULT_TABLE_IDS),
                        }
                    )
                    return _f[MESSAGE] if MESSAGE in _f else _f[DATA]
                except MagicException as e:
                    log_items.update({RESULT: False, EXEC_MESSAGE: str(e)})
                    return str(e)
                finally:
                    BaseLogger(log_items=log_items).write()

            return _wrap

        return _deco
