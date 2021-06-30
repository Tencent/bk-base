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
import datetime
import json
import logging
import sys
import traceback
from inspect import istraceback

PYTHON_MAJOR_VERSION = sys.version_info[0]

if PYTHON_MAJOR_VERSION == 2:
    str = str

CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

_levelToName = {
    CRITICAL: 'CRITICAL',
    ERROR: 'ERROR',
    WARNING: 'WARNING',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
    NOTSET: 'NOTSET',
}

DEFAULT_LOG_RECORD_FIELDS = {
    'name',
    'msg',
    'args',
    'levelname',
    'levelno',
    'pathname',
    'filename',
    'module',
    'exc_info',
    'exc_class',
    'exc_msg',
    'exc_traceback',
    'exc_text',
    'stack_info',
    'lineno',
    'funcName',
    'created',
    'msecs',
    'relativeCreated',
    'thread',
    'threadName',
    'processName',
    'process',
}


class SimpleJsonFormatter(logging.Formatter):
    level_to_name_mapping = _levelToName

    def __init__(self, fmt=None, datefmt=None, style='%', serializer=json.dumps):
        super(SimpleJsonFormatter, self).__init__()
        self.serializer = serializer

    @staticmethod
    def _default_json_handler(obj):
        if isinstance(obj, (datetime.date, datetime.time)):
            return str(obj.isoformat())
        elif istraceback(obj):
            tb = ''.join(traceback.format_tb(obj))
            return tb.strip()
        elif isinstance(obj, Exception):
            return "Exception: {}".format(str(obj))
        return str(obj)

    def format(self, record):
        msg = {
            'name': str(record.name),
            'timestamp': str(datetime.datetime.now().isoformat()),
            'line_number': record.lineno,
            'function': str(record.funcName),
            'module': str(record.module),
            'level': str(self.level_to_name_mapping[record.levelno]),
            'path': str(record.pathname),
            'thread_id': str(record.thread),
            'process_id': str(record.process),
            'thread_name': str(record.threadName),
            'process_name': str(record.processName),
        }

        for field, value in list(record.__dict__.items()):
            if field not in DEFAULT_LOG_RECORD_FIELDS:
                msg[field] = str(value)

        if isinstance(record.msg, dict):
            msg.update(record.msg)
        elif '%' in record.msg and len(record.args) > 0:
            try:
                msg['msg'] = record.msg % record.args
            except ValueError:
                msg['msg'] = record.msg
        else:
            msg['msg'] = record.msg

        if record.exc_info:
            msg['exc_class'], msg['exc_msg'], msg['exc_traceback'] = record.exc_info

        return str(self.serializer(msg, default=self._default_json_handler))
