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

from datetime import datetime

from bkbase.dataflow.one_model.utils.periodic_time_helper import PeriodicTimeHelper
from dateutil.relativedelta import relativedelta


class InputWindowAnalyzer(object):
    current_accumulate_start_time = None

    def __init__(self, info):
        self.is_accumulate_not_in_range = False
        self.window_type = info["window_type"].lower()
        if not (self.is_accumulate() or self.is_slide() or self.is_scroll()):
            raise Exception("unsupported  window type:{}".format(self.window_type))
        if "window_size" in info:
            self.window_size = BatchTimeDelta()
            self.window_size.init_delta_from_string(info["window_size"])
        if "window_offset" in info:
            self.window_offset = BatchTimeDelta()
            self.window_offset.init_delta_from_string(info["window_offset"])
        if "window_start_offset" in info:
            self.window_start_offset = BatchTimeDelta()
            self.window_start_offset.init_delta_from_string(info["window_start_offset"])
        if "window_end_offset" in info:
            self.window_end_offset = BatchTimeDelta()
            self.window_end_offset.init_delta_from_string(info["window_end_offset"])

        if "accumulate_start_time" in info:
            self.accumulate_start_time = BatchTimeStamp(info["accumulate_start_time"] / 1000)
        if "schedule_time" in info:
            # schedule_time_str = PeriodicTimeHelper.round_schedule_timestamp_to_hour(info['schedule_time'])
            # schedule_time = datetime.strptime(schedule_time_str, '%Y%m%d%H')
            schedule_time = PeriodicTimeHelper.round_schedule_timestamp_to_hour(info["schedule_time"])
            self.schedule_time_in_hour = BatchTimeStamp(schedule_time)
        self.calculate_accumulate_start_time()
        self.calculate_start_time()
        self.calculate_end_time()

    def calculate_start_time(self):
        if self.is_slide() or self.is_scroll():
            self.start_time = self.schedule_time_in_hour.minus(self.window_offset).minus(self.window_size)
        else:
            self.start_time = self.current_accumulate_start_time.plus(self.window_start_offset)
            if (
                self.schedule_time_in_hour.minus(self.window_offset).get_time_in_mills()
                <= self.start_time.get_time_in_mills()
            ):
                self.is_accumulate_not_in_range = True

    def calculate_end_time(self):
        if self.is_slide() or self.is_scroll():
            self.end_time = self.schedule_time_in_hour.minus(self.window_offset)
        else:
            end_offset_time = self.current_accumulate_start_time.plus(self.window_end_offset)
            self.end_time = self.schedule_time_in_hour.minus(self.window_offset)
            if self.end_time.get_time_in_mills() > end_offset_time.get_time_in_mills():
                self.is_accumulate_not_in_range = True

    def calculate_accumulate_start_time(self):
        if self.is_accumulate():
            limit_time_stamp = self.schedule_time_in_hour.minus(self.window_offset)
            if self.accumulate_start_time.get_time_in_mills() >= limit_time_stamp.get_time_in_mills():
                cur_batch_time_stamp = self.accumulate_start_time
                while cur_batch_time_stamp.get_time_in_mills() >= limit_time_stamp.get_time_in_mills():
                    cur_batch_time_stamp = cur_batch_time_stamp.minus(self.window_size)
                self.current_accumulate_start_time = cur_batch_time_stamp
            else:
                next_batch_time_stamp = self.accumulate_start_time.plus(self.window_size)
                cur_batch_time_stamp = self.accumulate_start_time
                while next_batch_time_stamp.get_time_in_mills() < limit_time_stamp.get_time_in_mills():
                    cur_batch_time_stamp = next_batch_time_stamp
                    next_batch_time_stamp = next_batch_time_stamp.plus(self.window_size)
                self.current_accumulate_start_time = cur_batch_time_stamp

    def is_accumulate(self):
        return self.window_type == "accumulate"

    def is_scroll(self):
        return self.window_type == "scroll"

    def is_slide(self):
        return self.window_type == "slide"


class BatchTimeDelta(object):
    def __init__(self):
        self.month = 0
        self.week = 0
        self.day = 0
        self.hour = 0

    def init_delta_from_string(self, time_string):
        time_array = time_string.split("+")
        for item_plus in time_array:
            array_split_with_minus = item_plus.split("-")
            minus_count = 0
            for item_minus in array_split_with_minus:
                if minus_count == 0 and item_minus == "":
                    minus_count = minus_count + 1
                    continue
                elif minus_count == 0 and item_minus != "":
                    self.set_time_with_string_format(item_minus, False)
                else:
                    self.set_time_with_string_format(item_minus, True)
                minus_count = minus_count + 1

    def set_time_with_string_format(self, string, is_minus):
        if string.upper().endswith("M"):
            self.month = self.parse_sign_int_str(string[0 : len(string) - 1], is_minus)
        elif string.upper().endswith("W"):
            self.week = self.parse_sign_int_str(string[0 : len(string) - 1], is_minus)
        elif string.upper().endswith("D"):
            self.day = self.parse_sign_int_str(string[0 : len(string) - 1], is_minus)
        else:
            self.hour = self.parse_sign_int_str(string[0 : len(string) - 1], is_minus)

    def parse_sign_int_str(self, string, is_minus):
        if is_minus:
            return -int(string)
        else:
            return int(string)

    def plus(self, time_delta):
        batch_time_delta = BatchTimeDelta()
        batch_time_delta.month = self.month + time_delta.month
        batch_time_delta.week = self.week + time_delta.week
        batch_time_delta.day = self.day + time_delta.day
        batch_time_delta.hour = self.hour + time_delta.hour
        return batch_time_delta

    def minus(self, time_delta):
        batch_time_delta = BatchTimeDelta()
        batch_time_delta.month = self.month - time_delta.month
        batch_time_delta.week = self.week - time_delta.week
        batch_time_delta.day = self.day - time_delta.day
        batch_time_delta.hour = self.hour - time_delta.hour
        return batch_time_delta

    def get_hour_except_month(self):
        return self.week * 7 * 24 + self.day * 24 + self.hour


class BatchTimeStamp(object):
    def __init__(self, timestamp):
        self.date_time = datetime.fromtimestamp(timestamp)

    def init_from_timestamp(self, timestamp):
        self.date_time = datetime.fromtimestamp(timestamp)

    def plus(self, time_delta):
        date_time_add_month = self.date_time + relativedelta(months=time_delta.month)
        new_date_time_stamp = date_time_add_month.timestamp() + time_delta.get_hour_except_month() * 3600
        return BatchTimeStamp(new_date_time_stamp)

    def minus(self, time_delta):
        date_time_minus_month = self.date_time - relativedelta(months=time_delta.month)
        new_date_time_stamp = date_time_minus_month.timestamp() - time_delta.get_hour_except_month() * 3600
        return BatchTimeStamp(new_date_time_stamp)

    def get_time_in_mills(self):
        return self.date_time.timestamp() * 1000
