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

import json
import time

from django.utils.translation import ugettext as _

from dataflow.component.exceptions.base_exception import CommonException
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import component_logger

PARQUET_READ_SCHEDULE = "parquet_reader"


def read_line(geog_area_code, cluster_id, file_name):
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    valid_parquet_read_schedule(jobnavi)
    exec_id = valid_parquet_read_execute(jobnavi)
    event_id = send_read_line_event(jobnavi, exec_id, file_name)
    return get_result(jobnavi, event_id)


def valid_parquet_read_schedule(jobnavi):
    """
    检查parquet read schedule是否存在，不存在就创建
    :param jobnavi:
    :return:
    """
    schedule_info = jobnavi.get_schedule_info(PARQUET_READ_SCHEDULE)
    if not schedule_info:
        args = {"schedule_id": PARQUET_READ_SCHEDULE, "type_id": "parquet_reader"}
        jobnavi.create_schedule_info(args)


def valid_parquet_read_execute(jobnavi):
    """
    检查parquet read schedule是否存在，不存在就创建
    :param jobnavi:
    :return:
    """
    data = jobnavi.get_execute_result_by_status(PARQUET_READ_SCHEDULE, "running")
    if data:
        exec_id = data[0]["execute_info"]["id"]
    else:
        args = {
            "schedule_id": PARQUET_READ_SCHEDULE,
        }
        exec_id = jobnavi.execute_schedule(args)
    wait_for_deployed(jobnavi, exec_id)
    return exec_id


def wait_for_deployed(jobnavi, execute_id):
    """
    根据execute id轮询执行状态, 等待部署完成

    :param execute_id: execute id
    :return: running
    """
    try:
        for i in range(10):
            data = jobnavi.get_execute_status(execute_id)
            if not data:
                time.sleep(1)
            elif data["status"] == "running":
                return data
            elif data["status"] != "preparing" and data["status"] != "none":
                raise CommonException("Jobnavi get execute status failed. %s" % data)
        raise CommonException(_("获取调度(%s)任务状态失败，请联系管理员" % PARQUET_READ_SCHEDULE))
    except Exception as e:
        component_logger.exception(e)
        raise CommonException(_("获取调度(%s)任务状态失败，请联系管理员" % PARQUET_READ_SCHEDULE))


def send_read_line_event(jobnavi, exec_id, path):
    """
    发送事件，获取最新一条数据
    :param jobnavi:
    :param exec_id:
    :param path:
    :return:
    """
    event_info = {
        "path": path,
    }
    args = {
        "event_name": "last_line",
        "execute_id": exec_id,
        "event_info": str(json.dumps(event_info)),
    }
    try:
        data = jobnavi.send_event(args)
        return data
    except Exception as e:
        component_logger.exception(e)
        raise CommonException(_("获取最新数据失败，请联系管理员"))


def get_result(jobnavi, event_id):
    """
    获取事件结果
    :param jobnavi:
    :param event_id:
    :return:
    """
    try:
        for i in range(10):
            data = jobnavi.get_event(event_id)

            if data["is_processed"]:
                if data["process_success"]:
                    return data["process_info"]
                else:
                    raise CommonException(_("获取最新数据失败，请联系管理员"))
            time.sleep(1)
        raise CommonException(_("获取最新数据失败，请联系管理员"))
    except Exception as e:
        component_logger.exception(e)
        raise CommonException(_("获取最新数据失败，请联系管理员"))
