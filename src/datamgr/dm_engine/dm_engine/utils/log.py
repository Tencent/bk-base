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
import logging.config  # needed when logging_config doesn't start with logging.config
import os


def configure_logging(
    log_level="DEBUG", log_module="master", log_dir=None, add_root_logger=False, report_sentry=False, sentry_DSN=None
):

    if log_dir is None:
        log_dir = os.path.join(os.getcwd(), "logs")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    _logging = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {"()": "dm_engine.utils.formatter.LogstashFormatterV1"},
            "simple": {"format": "%(levelname)s [%(asctime)s] [%(process)d]\t %(message)s \n"},
        },
        "handlers": {
            "null": {
                "level": "DEBUG",
                "class": "logging.NullHandler",
            },
            "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "simple"},
            "dm_engine_json": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": os.path.join(log_dir, "{}.json.log".format(log_module)),
                "maxBytes": 1024 * 1024 * 10,
                "backupCount": 5,
                "formatter": "verbose",
            },
            "dm_engine_console": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": os.path.join(log_dir, "{}.console.log".format(log_module)),
                "maxBytes": 1024 * 1024 * 10,
                "backupCount": 5,
                "formatter": "simple",
            },
        },
        "loggers": {"dm_engine": {"handlers": ["console", "dm_engine_json", "dm_engine_console"], "level": log_level}},
    }

    if report_sentry and sentry_DSN:
        _logging["handlers"]["sentry"] = {
            "level": "ERROR",
            "class": "raven.handlers.logging.SentryHandler",
            "dsn": sentry_DSN,
        }
        _logging["loggers"]["dm_engine"]["handlers"].append("sentry")

    if add_root_logger:
        _logging["root"] = {"handlers": ["console", "dm_engine_json", "dm_engine_console"], "level": log_level}
        if "sentry" in _logging["handlers"]:
            _logging["root"]["handlers"].append("sentry")

    logging.config.dictConfig(_logging)
