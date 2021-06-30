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
from __future__ import absolute_import, print_function, unicode_literals

import logging
import logging.config  # needed when logging_config doesn't start with logging.config
import os


class BaseTestCase(object):
    @classmethod
    def setup_class(cls):
        """
        这是一个class级别的setup函数，它会在这个测试类TestSohu里
        所有test执行之前，被调用一次
        """
        cls.init_logging()

    @classmethod
    def teardown_class(cls):
        pass

    @classmethod
    def init_logging(cls):
        log_dir = os.path.join(os.getcwd(), "logs")

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        _logging = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {
                    "format": "%(levelname)s [%(asctime)s] [%(process)d]\t %(message)s \n"
                }
            },
            "handlers": {
                "testcase": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": os.path.join(log_dir, "testcase.log"),
                    "maxBytes": 1024 * 1024 * 10,
                    "backupCount": 5,
                    "formatter": "simple",
                }
            },
            "root": {"handlers": ["testcase"], "level": "INFO"},
        }

        logging.config.dictConfig(_logging)
