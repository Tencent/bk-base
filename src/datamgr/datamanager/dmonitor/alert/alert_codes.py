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
import enum


class ListValueMixin(object):
    @classmethod
    def get_enum_value_list(cls, excludes=None):
        if excludes is None:
            excludes = []
        return [m.value for m in cls.__members__.values() if m.value not in excludes]

    @classmethod
    def get_enum_key_list(cls):
        return cls.__members__.keys()


class AlertCode(ListValueMixin, enum.Enum):
    TASK = "task"
    DATA_TREND = "data_trend"
    NO_DATA = "no_data"
    DATA_LOSS = "data_loss"
    DATA_TIME_DELAY = "data_time_delay"
    PROCESS_TIME_DELAY = "process_time_delay"
    DATA_INTERRUPT = "data_interrupt"
    DATA_DROP = "data_drop"
    BATCH_DELAY = "batch_delay"
    DELAY_TREND = "delay_trend"


class AlertType(ListValueMixin, enum.Enum):
    TASK_MONITOR = "task_monitor"
    DATA_MONITOR = "data_monitor"


class AlertLevel(ListValueMixin, enum.Enum):
    DANGER = "danger"
    WARNING = "warning"
    INFO = "info"


class AlertStatus(ListValueMixin, enum.Enum):
    INIT = "init"
    ALERTING = "alerting"
    CONVERGED = "converged"
    RECOVERED = "recovered"
    SHIELDED = "shielded"


class AlertSendStatus(ListValueMixin, enum.Enum):
    SUCC = "success"
    ERR = "error"
    INIT = "init"
    PART_ERR = "partial_error"


class NotifyWay(ListValueMixin, enum.Enum):
    SMS = "sms"
    MAIL = "mail"
    PHONE = "voice"
    WECHAT = "wechat"
    EEWECHAT = "eewechat"
    VOICE = "voice"


RECEIVER_TYPE_SUPPORT_MAPPINGS = {"list": ["voice"]}

RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS = {"user": ["voice"]}
