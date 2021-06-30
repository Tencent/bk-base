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

from common.exceptions import BaseError, ErrorCode


class DataQualityError(BaseError):
    MODULE_CODE = ErrorCode.BKDATA_MANAGER_DATAQUALITY


class DataQualityErrorCode(ErrorCode):
    COMMON_ERR = ("01", "数据质量通用异常")
    TASK_ALREADY_RUNNING_ERR = ("02", "结果表({data_set_id})的规则({rule_id})审核任务已经在运行中")
    TASK_ALREADY_FINISHED = ("03", "结果表({data_set_id})的规则({rule_id})审核任务已经结束")
    FUNC_NOT_IMPLEMENT_ERR = ("04", "审核判断函数({function_name})尚未实现")
    METRIC_NOT_SUPPORTED_ERR = ("05", "数据质量指标({metric_name})尚不支持")
    METRIC_ORIGIN_NOT_EXIST_ERR = ("06", "数据质量指标来源({metric_origin})不存在")
    METRIC_MSG_NOT_CORRECT_ERR = ("07", "指标原始信息错误，无法构建指标实例")
    TIMER_TYPE_NOT_SUPPORTED_ERR = ("08", "不支持该规则定时器类型({timer_type})")
    TIMER_HAS_BEEN_FINISHED_ERR = ("09", "定时器已经结束")
    TIME_FORMAT_NOT_SUPPORTED_ERR = ("10", "不支持时间格式({time_format})")
    TIMER_INTERVAL_FORMAT_ERR = ("11", "时间间隔格式({interval})异常")
    CONSTANT_TYPE_NOT_SUPPORTED_ERR = ("12", "不支持常数类型({constant_type})")
    PARAM_TYPE_NOT_SUPPORTED_ERR = ("13", "不支持参数类型({param_type})")
    METRIC_NOT_EXIST_ERR = ("14", "指标({metric_name})不在审核任务上下文")
    EVENT_NOT_EXIST_ERR = ("15", "事件({event_id})不在审核任务上下文")
    QUERY_TSDB_METRIC_ERR = ("16", "从TSDB查询指标({metric_name})失败，查询SQL：{sql}")
    RULE_NOT_EXIST_ERR = ("17", "结果表({data_set_id})的规则({rule_id})不存在")
    EVENT_NOT_SUPPORTED_ERR = ("18", "数据质量事件({event_id})尚不支持")
    START_TASK_TIMEOUT_ERR = ("19", "启动结果表({data_set_id})的规则({rule_id})审核任务超时")
    START_DATAQUALITY_SESSION_ERR = ("20", "启动数据质量Session失败({server_id})")
    STOP_DATAQUALITY_SESSION_ERR = ("21", "停止数据质量Session失败({server_id})")
    SESSION_SERVER_CODE_ERR = ("22", "Session({server_id})执行代码异常: {error}")


class AuditTaskAlreadyRunningError(DataQualityError):
    CODE = DataQualityErrorCode.TASK_ALREADY_RUNNING_ERR[0]
    MESSAGE = DataQualityErrorCode.TASK_ALREADY_RUNNING_ERR[1]


class AuditTaskAlreadyFinishedError(DataQualityError):
    CODE = DataQualityErrorCode.TASK_ALREADY_FINISHED[0]
    MESSAGE = DataQualityErrorCode.TASK_ALREADY_FINISHED[1]


class FunctionNotImplementError(DataQualityError):
    CODE = DataQualityErrorCode.FUNC_NOT_IMPLEMENT_ERR[0]
    MESSAGE = DataQualityErrorCode.FUNC_NOT_IMPLEMENT_ERR[1]


class MetricNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.METRIC_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.METRIC_NOT_SUPPORTED_ERR[1]


class MetricOriginNotExistError(DataQualityError):
    CODE = DataQualityErrorCode.METRIC_ORIGIN_NOT_EXIST_ERR[0]
    MESSAGE = DataQualityErrorCode.METRIC_ORIGIN_NOT_EXIST_ERR[1]


class MetricMessageNotCorrectError(DataQualityError):
    CODE = DataQualityErrorCode.METRIC_MSG_NOT_CORRECT_ERR[0]
    MESSAGE = DataQualityErrorCode.METRIC_MSG_NOT_CORRECT_ERR[1]


class TimerTypeNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.TIMER_TYPE_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.TIMER_TYPE_NOT_SUPPORTED_ERR[1]


class TimerHasBeenFinsihedError(DataQualityError):
    CODE = DataQualityErrorCode.TIMER_HAS_BEEN_FINISHED_ERR[0]
    MESSAGE = DataQualityErrorCode.TIMER_HAS_BEEN_FINISHED_ERR[1]


class TimeFormatNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.TIME_FORMAT_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.TIME_FORMAT_NOT_SUPPORTED_ERR[1]


class TimerIntervalFormatError(DataQualityError):
    CODE = DataQualityErrorCode.TIMER_INTERVAL_FORMAT_ERR[0]
    MESSAGE = DataQualityErrorCode.TIMER_INTERVAL_FORMAT_ERR[1]


class ConstantTypeNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.CONSTANT_TYPE_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.CONSTANT_TYPE_NOT_SUPPORTED_ERR[1]


class ParamTypeNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.PARAM_TYPE_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.PARAM_TYPE_NOT_SUPPORTED_ERR[1]


class MetricNotExistError(DataQualityError):
    CODE = DataQualityErrorCode.METRIC_NOT_EXIST_ERR[0]
    MESSAGE = DataQualityErrorCode.METRIC_NOT_EXIST_ERR[1]


class EventNotExistError(DataQualityError):
    CODE = DataQualityErrorCode.EVENT_NOT_EXIST_ERR[0]
    MESSAGE = DataQualityErrorCode.EVENT_NOT_EXIST_ERR[1]


class QueryTSDBMetricError(DataQualityError):
    CODE = DataQualityErrorCode.QUERY_TSDB_METRIC_ERR[0]
    MESSAGE = DataQualityErrorCode.QUERY_TSDB_METRIC_ERR[1]


class RuleNotExistError(DataQualityError):
    CODE = DataQualityErrorCode.RULE_NOT_EXIST_ERR[0]
    MESSAGE = DataQualityErrorCode.RULE_NOT_EXIST_ERR[1]


class EventNotSupportedError(DataQualityError):
    CODE = DataQualityErrorCode.EVENT_NOT_SUPPORTED_ERR[0]
    MESSAGE = DataQualityErrorCode.EVENT_NOT_SUPPORTED_ERR[1]


class StartAuditTaskTimeoutError(DataQualityError):
    CODE = DataQualityErrorCode.START_TASK_TIMEOUT_ERR[0]
    MESSAGE = DataQualityErrorCode.START_TASK_TIMEOUT_ERR[1]


class StartSessionError(DataQualityError):
    CODE = DataQualityErrorCode.START_DATAQUALITY_SESSION_ERR[0]
    MESSAGE = DataQualityErrorCode.START_DATAQUALITY_SESSION_ERR[1]


class StopSessionError(DataQualityError):
    CODE = DataQualityErrorCode.STOP_DATAQUALITY_SESSION_ERR[0]
    MESSAGE = DataQualityErrorCode.STOP_DATAQUALITY_SESSION_ERR[1]


class SessionServerCodeError(DataQualityError):
    CODE = DataQualityErrorCode.SESSION_SERVER_CODE_ERR[0]
    MESSAGE = DataQualityErrorCode.SESSION_SERVER_CODE_ERR[1]
