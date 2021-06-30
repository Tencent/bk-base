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
"""
将后台任务需要的指标上报和读取接口进行统一管理
"""
import json
import logging
import time
from typing import Dict

from api import datamanage_api

logger = logging.getLogger(__name__)


def report_metrics(
    measurement: str,
    fields_mapping: Dict,
    tags_mapping: Dict,
    database: str = "monitor_custom_metrics",
    raise_exception: bool = False,
):
    """

    :param database: 指标所在数据库
    :param measurement: 指标所在表
    :param fields_mapping: 指标中的数值字段
    :param tags_mapping: 指标中的维度字段
    :param raise_exception: 大部分上报都期望不要抛出异常，影响主流程，所以这里默认设置为 False
    """
    content = {
        "database": database,
        measurement: {
            "tags": tags_mapping,
        },
        "time": time.time(),
    }
    content[measurement].update(fields_mapping)
    message = json.dumps(content)
    try:
        datamanage_api.influx_report(
            {"message": message, "kafka_topic": "bkdata_monitor_metrics591"},
            retry_times=3,
            raise_exception=True,
        )
    except Exception as err:
        if not raise_exception:
            logger.exception(f"report_metrics error: {err}")
            return False
        raise err

    return True
