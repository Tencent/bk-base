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
import re
import time

from crontab import CronTab

from dataquality.exceptions import (
    TimeFormatNotSupportedError,
    TimerHasBeenFinsihedError,
    TimerIntervalFormatError,
    TimerTypeNotSupportedError,
)
from dataquality.model.base import TimerType
from dataquality.rule.base import Timer


class TimerManager(object):
    @staticmethod
    def generate_timer(timer_type, timer_config):
        if timer_type == TimerType.CRONTAB.value:
            return CrontabTimer(timer_config)
        elif timer_type == TimerType.FIX.value:
            return FixTimer(timer_config)
        elif timer_type == TimerType.INTERVAL.value:
            return IntervalTimer(timer_config)
        elif timer_type == TimerType.ONCE.value:
            return OnceTimer(timer_config)
        else:
            raise TimerTypeNotSupportedError(message_kv={"timer_type": timer_type})


class CrontabTimer(Timer):
    def __init__(self, timer_config):
        super(CrontabTimer, self).__init__(timer_config)
        self._crontab = CronTab(timer_config)

    def next(self):
        return self._crontab.next(default_utc=False)

    def tick(self):
        pass

    def finished(self):
        return False


class FixTimer(Timer):
    def __init__(self, timer_config):
        super(FixTimer, self).__init__(timer_config)

        try:
            self._dt = datetime.datetime.strptime(timer_config, "%Y-%m-%d %H:%M:%S")
        except Exception:
            if len(timer_config) == 10:
                self._dt = datetime.datetime.fromtimestamp(int(timer_config))
            elif len(timer_config) == 13:
                self._dt = datetime.datetime.fromtimestamp(int(timer_config) / 1000)
            else:
                raise TimeFormatNotSupportedError(
                    message_kv={"time_format": timer_config}
                )

    def next(self):
        now = datetime.datetime.now()
        if self._dt < now:
            raise TimerHasBeenFinsihedError()
        timedelta = self._dt - now
        return timedelta.seconds + timedelta.microseconds * 1e-6

    def tick(self):
        pass

    def finished(self):
        now = datetime.datetime.now()
        return self._dt < now


class IntervalTimer(Timer):
    INTERVAL_PATTERN = re.compile(r"(\d+)([d|h|m|s])")

    def __init__(self, timer_config):
        super(IntervalTimer, self).__init__(timer_config)

        self._last_audit_time = None
        self._interval = self.parse_interval(timer_config)

    def next(self):
        if self._last_audit_time is None:
            return 0

        now = time.time()
        remaining_second = self._last_audit_time + self._interval - now
        if remaining_second < 0:
            return 0
        return remaining_second

    def tick(self):
        self._last_audit_time = time.time()

    def finished(self):
        return False

    def parse_interval(self, timer_config):
        try:
            match_results = self.INTERVAL_PATTERN.match(timer_config).groups()
            amount = int(match_results[0])
            unit = match_results[1]
        except Exception:
            raise TimerIntervalFormatError(message_kv={"interval": timer_config})

        if unit == "d":
            return amount * 86400
        elif unit == "h":
            return amount * 3600
        elif unit == "m":
            return amount * 60
        elif unit == "s":
            return amount
        return 0


class OnceTimer(Timer):
    def next(self):
        return 0

    def tick(self):
        pass

    def finished(self):
        return True
