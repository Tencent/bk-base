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


class DmonitorError(BaseError):
    MODULE_CODE = ErrorCode.BKDATA_MANAGER_DMONITOR


class DmonitorErrorCode(ErrorCode):
    COMMON_ERR = ("01", "数据监控通用异常")
    METRIC_NOT_CORRECT_ERR = ("02", "指标结构不正确")
    DMONITOR_EVENT_REPORT_ERR = ("03", "dmonitor event report error:{error_info}")


class MetricMessageNotCorrectError(DmonitorError):
    CODE = DmonitorErrorCode.METRIC_NOT_CORRECT_ERR[0]
    MESSAGE = DmonitorErrorCode.METRIC_NOT_CORRECT_ERR[1]


class DmonitorEventReportError(DmonitorError):
    CODE = DmonitorErrorCode.DMONITOR_EVENT_REPORT_ERR[0]
    MESSAGE = DmonitorErrorCode.DMONITOR_EVENT_REPORT_ERR[1]
