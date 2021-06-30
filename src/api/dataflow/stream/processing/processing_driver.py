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

from django.db import transaction
from django.utils.translation import ugettext_lazy as _

from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import ComponentNotSupportError, StreamWindowConfigCheckError
from dataflow.stream.handlers import processing_stream_info
from dataflow.stream.processing.processing_for_common import delete_processings
from dataflow.stream.processing.processing_for_flink import (
    create_flink_processing,
    delete_processing_redis_checkpoint,
    get_real_window_type,
    update_flink_processings,
)
from dataflow.stream.processing.processing_handler import ProcessingHandler
from dataflow.stream.settings import ACCUMULATE, NONE, SESSION, SLIDING, TUMBLING, WINDOW_TYPE, ImplementType


def create_processing(args):
    # 这里的component type是由flow传过来的，此处不带版本号
    if "implement_type" in args and args["implement_type"] == ImplementType.CODE.value:
        return ProcessingHandler.create_processing(args)
    # todo 整合代码到 ProcessingHandler
    if args["component_type"] == "storm":
        from dataflow.stream.extend.processing.processing_for_storm import create_storm_processing

        return create_storm_processing(args)
    elif args["component_type"] == "flink":
        return create_flink_processing(args)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % args["component_type"])


def update_processings(args, processing_id):
    # processing_stream_info 表中记录的 component type 是包含版本号的
    stream_info = processing_stream_info.get(processing_id)
    if "implement_type" in args and args["implement_type"] == ImplementType.CODE.value:
        return ProcessingHandler(processing_id).update_processing(args)
    # todo 整合代码到 ProcessingHandler
    if stream_info.component_type == "flink":
        return update_flink_processings(args, processing_id)
    elif stream_info.component_type == "storm":
        from dataflow.stream.extend.processing.processing_for_storm import update_storm_processings

        return update_storm_processings(args, processing_id)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % stream_info.component_type)


def delete_processing_funcs(args, processing_id):
    with_data = args.get("with_data", False)
    delete_processing_redis_checkpoint(processing_id)
    delete_processings(processing_id, with_data)


def delete_all_processing_info(remove_processing_list):
    logger.info("delete the data processing and result table " + str(remove_processing_list))
    for remove_processing in remove_processing_list:
        MetaApiHelper.delete_data_processing(remove_processing)
        processing_stream_info.delete(remove_processing)


@transaction.atomic()
def change_component_type(args):
    processings = args["processings"]
    component_type = args["component_type"]
    for processing in processings:
        processing_stream_info.update(processing, component_type=component_type)


def get_processing_udf_info(processing_id):
    udf_names = []
    result_data = []
    udf_infos = processing_udf_info.where(processing_id=processing_id)
    for udf in udf_infos:
        if udf.udf_name not in udf_names:
            udf_info = json.loads(udf.udf_info)
            result_data.append(udf_info)
        udf_names.append(udf.udf_name)
    return result_data


def check_stream_param(args):

    window_type = args["scheduling_content"]["window_type"]
    count_freq = args["scheduling_content"].get("count_freq") or 0
    waiting_time = args["scheduling_content"].get("waiting_time") or 0
    window_time = args["scheduling_content"].get("window_time") or 0
    session_gap = args["scheduling_content"].get("session_gap") or 0
    expired_time = args["scheduling_content"].get("expired_time") or 0
    # 延迟数据计算
    window_lateness = args["scheduling_content"].get("window_lateness") or {}
    allowed_lateness = window_lateness.get("allowed_lateness") or False
    lateness_time = window_lateness.get("lateness_time") or 0
    lateness_count_freq = window_lateness.get("lateness_count_freq") or 0

    # 无窗不校验
    if window_type == NONE:
        return
    flink_window_type = get_real_window_type(window_type)
    # 校验窗口类型
    __check_window_type(flink_window_type)
    # 校验窗口长度
    __check_window_length(window_time, flink_window_type)
    # 校验等待时间
    __check_waiting_time(waiting_time)
    # 校验统计频率
    __check_count_freq(count_freq, flink_window_type)
    # 校验会话间隔及过期时间
    __check_session_gap_expired_time(session_gap, expired_time, flink_window_type)
    # 校验延迟计算配置
    __check_lateness_config(allowed_lateness, lateness_time, lateness_count_freq, flink_window_type)


def __check_window_type(flink_window_type):
    """
    校验窗口配置项:窗口类型

    :param flink_window_type: 窗口类型
    """
    # 校验窗口类型
    if flink_window_type not in WINDOW_TYPE:
        raise StreamWindowConfigCheckError(_("窗口类型目前只支持 [scroll、accumulate、slide、session]"))


def __check_window_length(window_time, flink_window_type):
    """
    校验窗口配置项:窗口长度

    :param window_time: 窗口长度
    :param flink_window_type: 窗口类型
    """
    # 校验窗口长度（单位:m)
    if flink_window_type == SLIDING and window_time not in [2, 10, 30, 45, 60, 1440]:
        raise StreamWindowConfigCheckError(
            _("窗口类型[%s] 属性[%s] 目前只支持 %s") % (SLIDING, "window_time", "[2, 10, 30, 45, 60, 1440]")
        )
    elif flink_window_type == ACCUMULATE and window_time not in [10, 30, 45, 60, 1440]:
        raise StreamWindowConfigCheckError(
            _("窗口类型[%s] 属性[%s] 目前只支持 %s") % (ACCUMULATE, "window_time", "[10, 30, 45, 60, 1440]")
        )


def __check_waiting_time(waiting_time):
    """
    校验窗口配置项:等待时间

    :param waiting_time: 等待时间
    """
    # 校验等待时间（单位:s)
    if waiting_time not in [0, 10, 30, 60, 180, 300, 600]:
        raise StreamWindowConfigCheckError(_("属性[%s] 目前只支持 %s") % ("waiting_time", "[0, 10, 30, 60, 180, 300, 600]"))


def __check_count_freq(count_freq, flink_window_type):
    """
    校验窗口配置项:统计频率

    :param count_freq: 统计频率
    :param flink_window_type: 窗口类型
    """
    # 校验统计频率（单位:s)
    if flink_window_type in [TUMBLING, SLIDING, ACCUMULATE] and count_freq not in [
        30,
        60,
        180,
        300,
        600,
    ]:
        raise StreamWindowConfigCheckError(
            _("窗口类型[%s] 属性[%s] 目前只支持 %s") % ("scroll、slide、accumulate", "count_freq", "[30, 60, 180, 300, 600]")
        )


def __check_session_gap_expired_time(session_gap, expired_time, flink_window_type):
    """
    校验窗口配置项:统计频率

    :param session_gap: 间隔时间
    :param expired_time: 过期时间
    :param flink_window_type: 窗口类型
    """
    if flink_window_type == SESSION:
        # 间隔时间（单位:s)
        if session_gap not in [0, 10, 30, 60, 180, 300, 600]:
            raise StreamWindowConfigCheckError(
                _("窗口类型[%s] 属性[%s] 目前只支持 %s") % (SESSION, "session_gap", "[0, 10, 30, 60, 180, 300, 600]")
            )
        # 过期时间（单位:m)
        if expired_time not in [0, 1, 3, 5, 10, 30]:
            raise StreamWindowConfigCheckError(
                _("窗口类型[%s] 属性[%s] 目前只支持 %s") % (SESSION, "expired_time", "[0, 1, 3, 5, 10, 30]")
            )


def __check_lateness_config(allowed_lateness, lateness_time, lateness_count_freq, flink_window_type):
    """
    校验窗口配置项:计算延迟数据

    :param allowed_lateness: 是否计算延迟数据
    :param lateness_time: 延迟时间
    :param lateness_count_freq: 统计频率
    :param flink_window_type: 窗口类型
    """
    if allowed_lateness:
        # 校验延迟时间（单位:h)
        if lateness_time not in [1, 6, 12, 24, 48]:
            raise StreamWindowConfigCheckError(_("属性[%s] 目前只支持 %s") % ("lateness_time", "[1, 6, 12, 24, 48]"))
        # 校验统计频率（单位:s）
        if flink_window_type != SESSION:
            if lateness_count_freq not in [60, 180, 300, 600]:
                raise StreamWindowConfigCheckError(
                    _("属性[%s] 目前只支持 %s") % ("lateness_count_freq", "[60, 180, 300, 600]")
                )
