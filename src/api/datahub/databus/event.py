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

from common.log import logger
from datahub.databus.api.dataflow import DataFlowApi
from datahub.databus.models import DatabusStorageEvent

from datahub.databus import rt


def check_hdfs_event_and_notify(result_table_id, event_type, event_value):
    """
    获取当前事件触发次数、判断是否需要调用计算模块
    :param result_table_id: result_table表的result_table_id，结果表标识
    :param event_type: 事件类型
    :param event_value: 事件值
    :return: 无返回
    """
    try:
        event_count = DatabusStorageEvent.objects.filter(
            result_table_id=result_table_id,
            event_type=event_type,
            event_value=event_value,
        ).count()
        par_num = rt.get_partitions(result_table_id)
        process_finished = event_count > 0 and par_num > 0 and (event_count % par_num == 0)
        if process_finished:
            if validate_date_time(event_value):
                return call_dataflow_batch_job(result_table_id, event_value)
            else:
                warn_msg = "%s not a valid date time for offline calculate" % event_value
                send_warn_info(warn_msg)
                return False, warn_msg
        else:
            return False, "process_not_finished"
    except Exception as e:
        logger.error(
            "failed to check and notify event with result_table_id:%s event_type:%s event_value:%s e:%s"
            % (result_table_id, event_type, event_value, e)
        )
        return (
            False,
            "failed to check and notify event with result_table_id：%s" % result_table_id,
        )


def call_dataflow_batch_job(result_table_id, date_time):
    """
    调用dataflow batch接口，触发离线计算任务
    :param result_table_id: result_table表的result_table_id，结果表标识。
    :param date_time: 数据时间 格式为YYYYMMDD或YYYYMMDDHH
    :return: 调用成功返回True，否则返回False
    """
    try:
        ret = DataFlowApi.batch_jobs.execute({"jid": result_table_id, "data_time": date_time})
        if ret.is_success():
            return True, "ok"
        else:
            error_msg = u"[bad response!] failed to call dataflow batch job result_table_id:%s" % result_table_id
            logger.error(error_msg)
            send_warn_info(error_msg)
            return False, error_msg
    except Exception as e:
        error_msg = u"failed to call dataflow batch job result_table_id:{} date_time:{} e:{}".format(
            result_table_id,
            date_time,
            e,
        )
        logger.error(error_msg)
        send_warn_info(error_msg)
        return (
            False,
            "failed to call dataflow batch job result_table_id:%s" % result_table_id,
        )


def send_warn_info(msg):
    """
    给指定人员发送告警内容，后续应该在平台处配置，这里先留个接口
    :param msg: 消息内容
    :return: 无返回
    """
    logger.error(msg)


def validate_date_time(date_time):
    """
    日期格式校验：查输入的date_time是否符合 YYYYMMDD 或者 YYYYMMDDHH 日期格式
    :param date_time: 日期字符串
    :return: 验证通过返回True，否则返回False
    """
    try:
        if len(date_time) == 8:
            date_time = int(date_time)
            day = int(date_time % 100)
            month = int((date_time / 100) % 100)
            year = int(date_time / 10000)
            return year > 2000 and month in range(1, 13) and day in range(1, 32)
        elif len(date_time) == 10:
            date_time = int(date_time)
            hour = int(date_time % 100)
            day = int((date_time / 100) % 100)
            month = int((date_time / 10000) % 100)
            year = int(date_time / 1000000)
            return year > 2000 and month in range(1, 13) and day in range(1, 32) and hour in range(0, 24)
    except Exception as e:
        logger.error("failed to validate datetime {}. {}".format(date_time, e))
    return False
