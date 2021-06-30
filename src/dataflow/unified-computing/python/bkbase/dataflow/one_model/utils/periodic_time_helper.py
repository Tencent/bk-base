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

from datetime import datetime, timedelta

from bkbase.dataflow.one_model.api import deeplearning_api
from bkbase.dataflow.one_model.utils import deeplearning_logger
from bkbase.dataflow.one_model.utils.deeplearning_constant import NodeType, PeriodUnit
from bkdata_datalake import tables
from dateutil.relativedelta import relativedelta


class PeriodicTimeHelper(object):
    def __init__(self, node_id, window_info, parent_node_url=None):
        self.is_managed = int(window_info.get("is_managed", "1")) == 1
        self.is_accumulate = bool(window_info.get("accumulate"))
        self.data_time_in_hour = self.round_schedule_timestamp_to_hour(window_info["schedule_time"])
        self.processing_id = node_id
        self.parent_node_url = parent_node_url
        if self.is_accumulate:
            self.start_time = int(window_info["data_start"])
            self.end_time = int(window_info["data_end"])
        else:
            window_size = int(window_info["window_size"])
            window_delay = int(window_info["window_delay"])
            self.time_unit = window_info["window_size_period"].lower()
            self.start_time = window_delay
            self.end_time = window_delay + window_size

    @classmethod
    def round_schedule_timestamp_to_hour(cls, schedule_time):
        datatime = datetime.fromtimestamp(schedule_time / 1000)
        datatime_str = datatime.strftime("%Y%m%d%H")
        datatime_hour = datetime.strptime(datatime_str, "%Y%m%d%H")
        return datatime_hour.replace(minute=0, second=0, microsecond=0).timestamp()

    def get_start_and_end_time(self):
        if self.is_accumulate:
            return self.get_accumulate_start_end_time()
        else:
            return self.get_window_input_start_end_time()

    def get_accumulate_start_end_time(self):
        data_time = datetime.fromtimestamp(self.data_time_in_hour)
        data_time.replace(minute=0, second=0)
        hour = data_time.hour
        if (hour - 1 + 24) % 24 < self.start_time or (hour - 1 + 24) % 24 > self.end_time:
            message = "illegal argument:Not in run period. Expect Range {}-{}, now is {}".format(
                (self.start_time + 1) % 24, (self.end_time + 1) % 24, hour
            )
            deeplearning_logger.error(message)
            raise Exception(message)
        if hour == 0:
            data_time = data_time - timedelta(days=1)
        data_time = data_time.replace(hour=self.start_time)
        start_time_in_mills = data_time.timestamp() * 1000
        data_time = datetime.fromtimestamp(self.data_time_in_hour)
        # 累加窗口是从start到当前时间
        data_time = data_time.replace(hour=hour)
        end_time_in_mills = data_time.timestamp() * 1000
        return start_time_in_mills, end_time_in_mills

    @staticmethod
    def format_parent_source_nodes(parent_params):
        # 根据从接口中获取的当前Processing的信息，得到其父rt相关的信息，
        parent_source_nodes = {}
        # 主要是为了可以利用已经有的get_min_window_info函数
        for parent_id in parent_params["parent_ids"]:
            parent_table = parent_params["parent_ids"][parent_id]
            parent_source = {}
            parent_source_nodes[parent_id] = parent_source
            parent_source["type"] = NodeType.DATA.value
            window_info = {}
            parent_source["window"] = window_info
            window_info["schedule_period"] = parent_params["schedule_period"]
            window_info["count_freq"] = parent_params["count_freq"]
            window_info["accumulate"] = parent_params["accumulate"]
            if not window_info["accumulate"]:
                window_info["window_size_period"] = parent_table["window_size_period"]
                window_info["window_size"] = parent_table["window_size"]
        return parent_source_nodes

    def get_window_input_start_end_time(self):
        parent_params = deeplearning_api.get_parent_info(self.processing_id, self.parent_node_url)
        parent_source_nodes = PeriodicTimeHelper.format_parent_source_nodes(parent_params)
        parent_min_window_size, _ = PeriodicTimeHelper.get_min_window_info(parent_source_nodes)
        parent_schedule_period = parent_params["schedule_period"]
        hour_to_seconds = 3600
        window_hour_to_seconds = parent_min_window_size * hour_to_seconds
        if self.time_unit == PeriodUnit.HOUR.value:
            # 当前任务为小时，则父任务肯定为小时
            start_time_seconds = self.data_time_in_hour - (self.end_time + parent_min_window_size - 1) * hour_to_seconds
            end_time_seconds = self.data_time_in_hour - (self.start_time + parent_min_window_size - 1) * hour_to_seconds
        elif self.time_unit == PeriodUnit.DAY.value:
            period_mills = (1 if parent_schedule_period == "hour" else 24) * hour_to_seconds
            format_start = self.start_time * 24 if parent_schedule_period == "hour" else self.start_time
            format_end = self.end_time * 24 if parent_schedule_period == "hour" else self.end_time
            start_time_seconds = self.data_time_in_hour - (format_end - 1) * period_mills - window_hour_to_seconds
            end_time_seconds = self.data_time_in_hour - (format_start - 1) * period_mills - window_hour_to_seconds
        elif self.time_unit == PeriodUnit.WEEK.value:
            if parent_schedule_period == PeriodUnit.HOUR.value:
                end_time_to_seconds = self.end_time * 24 * 7 + hour_to_seconds
                start_time_to_seconds = self.start_time * 24 * 7 + hour_to_seconds
                start_time_seconds = self.data_time_in_hour - end_time_to_seconds - window_hour_to_seconds
                end_time_seconds = self.data_time_in_hour - start_time_to_seconds - window_hour_to_seconds
            elif parent_schedule_period == PeriodUnit.DAY.value:
                end_time_to_seconds = self.end_time * 24 * 7 + 24 * hour_to_seconds
                start_time_to_seconds = self.start_time * 24 * 7 + 24 * hour_to_seconds
                start_time_seconds = self.data_time_in_hour - end_time_to_seconds - window_hour_to_seconds
                end_time_seconds = self.data_time_in_hour - start_time_to_seconds - window_hour_to_seconds
            elif parent_schedule_period == PeriodUnit.WEEK.value:
                end_time_to_secods = self.end_time * 24 * 7 + 24 * 7 * hour_to_seconds
                start_time_to_seconds = self.start_time * 24 * 7 + 24 * 7 * hour_to_seconds
                start_time_seconds = self.data_time_in_hour - end_time_to_secods - window_hour_to_seconds
                end_time_seconds = self.data_time_in_hour - start_time_to_seconds - window_hour_to_seconds
            else:
                raise Exception("window config is not supported:window unit is week, parent count frequency is month")
        elif self.time_unit == PeriodUnit.MONTH.value:
            if parent_schedule_period == PeriodUnit.HOUR.value:
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.end_time)
                start_time_seconds = data_time.timestamp() + hour_to_seconds - parent_min_window_size * hour_to_seconds
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.start_time)
                end_time_seconds = data_time.timestamp() + hour_to_seconds - parent_min_window_size * hour_to_seconds
            elif parent_schedule_period == PeriodUnit.DAY.value:
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.end_time)
                start_time_seconds = (
                    data_time.timestamp() + 24 * hour_to_seconds - parent_min_window_size * hour_to_seconds
                )
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.start_time)
                end_time_seconds = (
                    data_time.timestamp() + 24 * hour_to_seconds - parent_min_window_size * hour_to_seconds
                )
            elif parent_schedule_period == PeriodUnit.WEEK.value:
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.end_time)
                start_time_seconds = (
                    data_time.timestamp() + 24 * 7 * hour_to_seconds - parent_min_window_size * hour_to_seconds
                )
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=-self.start_time)
                end_time_seconds = (
                    data_time.timestamp() + 24 * 7 * hour_to_seconds - parent_min_window_size * hour_to_seconds
                )
            else:
                data_time = datetime.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=1 - self.end_time - parent_min_window_size)
                start_time_seconds = data_time.timestamp()
                data_time = data_time.fromtimestamp(self.data_time_in_hour)
                data_time = data_time + relativedelta(months=1 - self.start_time - parent_min_window_size)
                end_time_seconds = data_time.timestamp()

        return start_time_seconds * 1000, end_time_seconds * 1000

    @staticmethod
    def get_min_window_info(source_nodes):
        """
        计算得到当前任务的最小窗口，以确定最终数据落地时的时间戳等信息
        :param source_nodes: 配置中source信息
        :return: 最小窗口及单位
        """
        window_size_list = []
        has_minus_1_window_size = False
        unit = PeriodUnit.HOUR.value
        schedule_unit = PeriodUnit.HOUR.value
        count_freq = 1
        for source_name in source_nodes:
            source_node = source_nodes[source_name]
            if source_node["type"] != NodeType.DATA.value:
                continue
            window_info = source_node["window"]
            # window_info内的信息分为两部分，为了方便传输整合到一起的：
            # schedule_period, count_freq, accumuate，这三者指的是当前节点的调度信息
            # window_size,window_size_period,window_delay指的是当前节依赖此source有依赖信息
            schedule_unit = window_info["schedule_period"]
            count_freq = window_info["count_freq"]
            if window_info["accumulate"]:
                window_size = 1
            else:
                window_size_period = window_info["window_size_period"]
                if window_size_period == PeriodUnit.HOUR.value:
                    window_size = window_info["window_size"]
                elif window_size_period == PeriodUnit.DAY.value:
                    window_size = window_info["window_size"] * 24
                elif window_size_period == PeriodUnit.WEEK.value:
                    window_size = window_info["window_size"] * 24 * 7
                elif window_size_period == PeriodUnit.MONTH.value:
                    # note:如果依赖月任务，那他本身肯定是月任务了
                    unit = PeriodUnit.MONTH.value
                    window_size = window_info["window_size"]
                else:
                    window_size = 1
            if window_size == -1:
                has_minus_1_window_size = True
            else:
                window_size_list.append(window_size)
        if has_minus_1_window_size and not window_size_list:
            window_size = 1
            if schedule_unit == PeriodUnit.HOUR.value:
                window_size = count_freq
            elif schedule_unit == PeriodUnit.DAY.value:
                window_size = 24 * count_freq
            elif schedule_unit == PeriodUnit.WEEK.value:
                window_size = 24 * 7 * count_freq
            elif schedule_unit == PeriodUnit.MONTH.value:
                unit = PeriodUnit.MONTH.value
                window_size = count_freq
            window_size_list.append(window_size)
        return min(window_size_list), unit

    @staticmethod
    def get_sink_dt_event_time_in_ms(data_time_in_hour, window_size, window_unit):
        if window_unit == PeriodUnit.MONTH.value:
            data_time = datetime.fromtimestamp(data_time_in_hour)
            data_time = data_time + relativedelta(months=-window_size)
            return data_time.timestamp() * 1000
        else:
            return data_time_in_hour * 1000 - window_size * 3600 * 1000

    @staticmethod
    def get_greater_equal_less_expression(start_time, end_time):
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        start = datetime.utcfromtimestamp(start_time / 1000)
        end = datetime.utcfromtimestamp(end_time / 1000)
        left_expr = tables.build_field_condition_expr("____et", tables.STRING, ">=", start.strftime(date_format))
        right_expr = tables.build_field_condition_expr("____et", tables.STRING, "<", end.strftime(date_format))
        exp = tables.build_and_condition(left_expr, right_expr)
        return exp
