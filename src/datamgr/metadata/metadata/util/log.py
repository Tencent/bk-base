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
from logging import (
    Filter,
    StreamHandler,
    _acquireLock,
    _releaseLock,
    captureWarnings,
    root,
)

from concurrent_log_handler import ConcurrentRotatingFileHandler


def init_logging(log_conf, re_init=False, with_report=True):
    """
    初始化日志系统。添加Stream和按日期分割的File Handler。
    :param log_conf: 日志配置
    :param re_init: 是否更新设置
    :return:
    """
    try:
        _acquireLock()
        captureWarnings(True)
        if (not root.handlers) or re_init:
            root.handlers = []
            root.setLevel(log_conf.FILE_LEVEL)
            stream_handler = StreamHandler()
            stream_handler.setLevel(log_conf.STREAM_LEVEL)
            stream_handler.setFormatter(log_conf.FORMAT)
            file_handler = ConcurrentRotatingFileHandler(
                log_conf.file_path, maxBytes=1024 * 1024 * 1024 * 2, backupCount=5, encoding='utf8'
            )
            file_handler.setFormatter(log_conf.FORMAT)
            file_handler.setLevel(log_conf.FILE_LEVEL)
            if with_report:
                stream_handler.addFilter(ExcludeReportFilter())
                file_handler.addFilter(ExcludeReportFilter())
            root.addHandler(stream_handler)
            root.addHandler(file_handler)
    finally:
        _releaseLock()


def update_logging(handlers, logger=None):
    """
    更新logging。
    :param handlers: handlers
    :param logger: 需要更新的logger。默认为root logger。
    :return:
    """
    try:
        _acquireLock()
        if logger is None:
            logger = logging.getLogger()
        for handler in handlers:
            logger.addHandler(handler)
    finally:
        _releaseLock()


def update_report_logging(log_conf):
    try:
        _acquireLock()
        metric_handler = ConcurrentRotatingFileHandler(
            log_conf.file_path.split('.common.log')[0] + '.metric.log',
            maxBytes=1024 * 1024 * 1024,
            backupCount=2,
            encoding='utf8',
        )
        error_handler = ConcurrentRotatingFileHandler(
            log_conf.file_path.split('.common.log')[0] + '.error.log',
            maxBytes=1024 * 1024 * 1024,
            backupCount=2,
            encoding='utf8',
        )
        metric_handler.setFormatter(log_conf.FORMAT)
        metric_handler.addFilter(MetricReportFilter())
        error_handler.setFormatter(log_conf.FORMAT)
        error_handler.addFilter(ErrorReportFilter())
        update_logging([metric_handler, error_handler])
    finally:
        _releaseLock()


def update_record_handler(log_conf):
    try:
        _acquireLock()
        record_handler = ConcurrentRotatingFileHandler(
            log_conf.file_path.split('.common.log')[0] + '.operate_records.log',
            maxBytes=1024 * 1024 * 1024,
            backupCount=2,
            encoding='utf8',
        )
        record_handler.setFormatter(log_conf.FORMAT)
        record_handler.addFilter(RecordFilter())
        update_logging([record_handler])
    finally:
        _releaseLock()


class RecordFilter(Filter):
    def filter(self, record):
        ret = super(RecordFilter, self).filter(record)
        metric_ret = getattr(record, 'recording', False)
        return ret and metric_ret


class ExcludeReportFilter(Filter):
    def filter(self, record):
        ret = super(ExcludeReportFilter, self).filter(record)
        report_ret = getattr(record, 'metric', False) or getattr(record, 'recording', False)
        return ret and (not report_ret)


class MetricReportFilter(Filter):
    def filter(self, record):
        ret = super(MetricReportFilter, self).filter(record)
        metric_ret = getattr(record, 'metric', False) or getattr(record, 'output_metric', False)
        return ret and metric_ret


class ErrorReportFilter(Filter):
    def filter(self, record):
        ret = super(ErrorReportFilter, self).filter(record)
        error_ret = record.levelno >= logging.ERROR
        return ret and error_ret
