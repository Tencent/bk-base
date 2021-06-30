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
import time

from bkbase.dataflow.metrics.base_metric import BaseMetric
from bkbase.dataflow.metrics.util.constants import *


class DataPlatformMetric(BaseMetric):
    # 封装更加全面的 Builder 类
    class Builder(object):
        def __init__(self, periodic_report_interval_sec, target_url, topic, report_type):
            self._periodic_report_interval_sec = periodic_report_interval_sec
            self._target_url = target_url
            self._topic = topic
            self._report_type = report_type
            # 其它参数
            self._module = None
            self._component = None
            self._cluster = None
            self._storage_type = None
            self._storage_id = None
            self._cluster_name = None
            self._cluster_type = None
            self._storage_host = None
            self._storage_port = None
            self._name = None
            self._physical_tag_map = {}
            self._logical_tag_map = {}
            self._custom_tag_map = {}

        # ----------------------------------- 自定义设置区
        def module(self, module):
            self._module = module
            return self

        def component(self, component):
            self._component = component
            return self

        def cluster(self, cluster):
            self._cluster = cluster
            return self

        def storage_type(self, storage_type):
            self._storage_type = storage_type
            return self

        def storage_id(self, storage_id):
            self._storage_id = storage_id
            return self

        def cluster_name(self, cluster_name):
            self._cluster_name = cluster_name
            return self

        def cluster_type(self, cluster_type):
            self._cluster_type = cluster_type
            return self

        def storage_host(self, storage_host):
            self._storage_host = storage_host
            return self

        def storage_port(self, storage_port):
            self._storage_port = storage_port
            return self

        def name(self, name):
            self._name = name
            return self

        def physical_tag(self, key, value):
            self._physical_tag_map[key] = value
            return self

        def logical_tag(self, key, value):
            self._logical_tag_map[key] = value
            return self

        def custom_tag(self, key, value):
            self._custom_tag_map[key] = value
            return self

        # 移除 map
        def remove_physical_tag(self, key):
            self._physical_tag_map.pop(key)
            return self

        def clear_physical_tag(self):
            self._physical_tag_map.clear()
            return self

        def remove_logical_tag(self, key):
            self._logical_tag_map.pop(key)
            return self

        def clear_logical_tag(self):
            self._logical_tag_map.clear()
            return self

        def remove_custom_tag(self, key):
            self._custom_tag_map.pop(key)
            return self

        def clear_custom_tag(self):
            self._custom_tag_map.clear()
            return self

        # ----------------------------------- 自定义设置区

        def build(self):
            data_platform_metric = DataPlatformMetric(
                self._periodic_report_interval_sec, self._target_url, self._topic, self._report_type
            )
            # time 时间戳
            now = int(time.time())
            data_platform_metric.literal("time").set_literal(now)
            data_platform_metric.literal("version").set_literal(DEFAULT_VERSION)
            seq_counter = data_platform_metric.counter("seq_no")
            seq_counter.set_reset(False)
            seq_counter.set_auto_increment(True)
            data_platform_metric.literal("is_end").set_literal(False)
            data_platform_metric.literal("info.module").set_literal(self._module)
            data_platform_metric.literal("info.component").set_literal(self._component)
            data_platform_metric.literal("info.cluster").set_literal(self._cluster)
            if self._storage_type and self._storage_id:
                data_platform_metric.literal("info.storage.storage_type").set_literal(self._storage_type)
                data_platform_metric.literal("info.storage.storage_id").set_literal(self._storage_id)
            elif self._cluster_name and self._cluster_type:
                data_platform_metric.literal("info.storage.cluster_name").set_literal(self._cluster_name)
                data_platform_metric.literal("info.storage.cluster_type").set_literal(self._cluster_type)
            elif self._storage_host and self._storage_port:
                data_platform_metric.literal("info.storage.storage_host").set_literal(self._storage_host)
                data_platform_metric.literal("info.storage.storage_port").set_literal(self._storage_port)
            # physical_tag
            physical_tag = ""
            for key in self._physical_tag_map:
                value = self._physical_tag_map[key]
                physical_tag += str(value) + "|"
                data_platform_metric.literal("info.physical_tag.desc.%s" % key).set_literal(value)
            # [:-1] 删除最后一个字符 '|'
            data_platform_metric.literal("info.physical_tag.tag").set_literal(physical_tag[:-1])
            # logical_tag 没有 '|'
            for key in self._logical_tag_map:
                value = self._logical_tag_map[key]
                data_platform_metric.literal("info.logical_tag.desc.%s" % key).set_literal(value)
            rt_id = data_platform_metric.literal("info.logical_tag.desc.result_table_id").get_literal()
            data_platform_metric.literal("info.logical_tag.tag").set_literal(rt_id)
            # custom_tag
            for key in self._custom_tag_map:
                value = self._custom_tag_map[key]
                data_platform_metric.literal("info.custom_tag.%s" % key).set_literal(value)
            return data_platform_metric

    def __init__(self, periodic_report_interval_sec, target_url, topic, report_type):
        super(DataPlatformMetric, self).__init__(periodic_report_interval_sec, target_url, topic, report_type)

    # ------------------------------------------------------ 特定的功能函数封装

    """
    获取输入总数据量
    counter : data_monitor.data_loss.input.total_cnt
    """

    def get_input_total_cnt_counter(self):
        counter = self.counter("metrics.data_monitor.data_loss.input.total_cnt")
        counter.set_reset(False)
        return counter

    """
    获取输入增量数据量
    counter : data_monitor.data_loss.input.total_cnt_increment
    """

    def get_input_total_cnt_inc_counter(self):
        return self.counter("metrics.data_monitor.data_loss.input.total_cnt_increment")

    """
    获取输入 tag 信息
    counter : metrics.data_monitor.data_loss.input.tags.tagName
    """

    def get_input_tags_counter(self, tag_name):
        return self.counter("metrics.data_monitor.data_loss.input.tags.%s" % tag_name)

    """
    获取输入 tag 信息
    counter : metrics.data_monitor.data_loss.input.tags.tagPrefix|timeStamp
    """

    def get_input_tags_counter2(self, tag_prefix, time_stamp):
        return self.counter("metrics.data_monitor.data_loss.input.tags.{}_{}".format(tag_prefix, time_stamp))

    """
    counter: metrics.data_monitor.data_loss.input.tags.tagPrefix:timeStamp1_timeStamp2
    """

    def get_input_tags_counter3(self, tag_prefix, time_stamp1, time_stamp2):
        return self.counter(
            "metrics.data_monitor.data_loss.input.tags.{}:{}_{}".format(tag_prefix, time_stamp1, time_stamp2)
        )

    """
    批量设置输入tag信息
    counter: metrics.data_monitor.data_loss.input.tags.{key = value}
    """

    def set_input_tags_counter(self, tags):
        for key in tags:
            value = tags[key]
            self.counter("metrics.data_monitor.data_loss.input.tags.%s" % key).inc(value)

    """
    获取输入segment信息
    counter: metrics.data_monitor.data_loss.input.segments.segmentName
    """

    def get_input_segments_counter(self, segment_name):
        return self.counter("metrics.data_monitor.data_loss.input.segments.%s" % segment_name)

    """
    批量设置输入segment信息
    counter: metrics.data_monitor.data_loss.input.segments.{key = value}
    """

    def set_input_segments_counter(self, segments):
        for key in segments:
            value = segments[key]
            self.counter("metrics.data_monitor.data_loss.input.segments.%s" % key).inc(value)

    """
    data_monitor / data_loss / output
    获取输出总数据量 counter: metrics.data_monitor.data_loss.output.total_cnt
    """

    def get_output_total_cnt_counter(self):
        counter = self.counter("metrics.data_monitor.data_loss.output.total_cnt")
        counter.set_reset(False)
        return counter

    """
    获取输出增量数据量 counter: metrics.data_monitor.data_loss.output.total_cnt_increment
    """

    def get_output_total_cnt_inc_counter(self):
        return self.counter("metrics.data_monitor.data_loss.output.total_cnt_increment")

    """
    获取输出tag信息
    counter: metrics.data_monitor.data_loss.output.tags.tagName
    """

    def get_output_tags_counter(self, tag_name):
        return self.counter("metrics.data_monitor.data_loss.output.tags.%s" % tag_name)

    """
    获取输出tag信息
    counter: metrics.data_monitor.data_loss.output.tags.tagPrefix_timeStamp
    """

    def get_output_tags_counter2(self, tag_prefix, time_stamp):
        return self.counter("metrics.data_monitor.data_loss.output.tags.{}_{}".format(tag_prefix, time_stamp))

    """
    获取输出tag信息
    counter: metrics.data_monitor.data_loss.output.tags.tagPrefix:timeStamp1_timeStamp2
    """

    def get_output_tags_counter3(self, tag_prefix, time_stamp1, time_stamp2):
        return self.counter(
            "metrics.data_monitor.data_loss.output.tags.{}:{}|{}".format(tag_prefix, time_stamp1, time_stamp2)
        )

    """
    批量设置输出tag信息
    counter: metrics.data_monitor.data_loss.output.tags.key.{key = value}
    """

    def set_output_tags_counter(self, tags):
        for key in tags:
            value = tags[key]
            self.counter("metrics.data_monitor.data_loss.output.tags.%s" % key).inc(value)

    """
    获取输出checkpoint丢弃
    counter: metrics.data_monitor.data_loss.output.ckp_drop
    """

    def get_output_ckp_drop_counter(self):
        return self.counter("metrics.data_monitor.data_loss.output.ckp_drop")

    """
    获取输出checkpoint丢弃tag相关
    counter: metrics.data_monitor.data_loss.output.ckp_drop_tags.tagName
    """

    def get_output_ckp_drop_tags_counter(self, tag_name):
        return self.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags.%s" % tag_name)

    """
    获取输出checkpoint丢弃tag相关
    counter: metrics.data_monitor.data_loss.output.ckp_drop_tags.tagPrefix_timeStamp
    """

    def get_output_ckp_drop_tags_counter2(self, tag_prefix, time_stamp):
        return self.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags.{}_{}".format(tag_prefix, time_stamp))

    """
    批量设置输出checkpoint丢弃tag相关
    counter: metrics.data_monitor.data_loss.output.ckp_drop_tags.key.{key = value}
    """

    def set_output_ckp_drop_tags_counter(self, tags):
        for key in tags:
            value = tags[key]
            self.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags.%s" % key).inc(value)

    """
    获取输出segment相关
    counter: metrics.data_monitor.data_loss.output.segments.segmentName
    """

    def get_output_segments_counter(self, segment_name):
        return self.counter("metrics.data_monitor.data_loss.output.segments.%s" % segment_name)

    """
    批量设置输出segment信息
    counter: metrics.data_monitor.data_loss.output.segments.{key = value}
    """

    def set_output_segments_counter(self, segments):
        for key in segments:
            value = segments[key]
            self.counter("metrics.data_monitor.data_loss.output.segments.%s" % key).inc(value)

    """
    data_monitor / data_loss / drop
    获取输出丢弃
    counter: metrics.data_monitor.data_loss.data_drop.dropCode.cnt
    """

    def get_data_drop_tags_counter(self, drop_code, drop_reason):
        counter = self.counter("metrics.data_monitor.data_loss.data_drop.%s.cnt" % drop_code)
        self.literal("metrics.data_monitor.data_loss.data_drop.%.reason" % drop_code).set_literal(drop_reason)
        return counter

    """
    设置输出丢弃
    counter: metrics.data_monitor.data_loss.data_drop.dropCode.cnt
    """

    def set_data_drop_tags_counter(self, drop_code, drop_reason, drop_cnt):
        self.counter("metrics.data_monitor.data_loss.data_drop.%s.cnt" % drop_code).inc(drop_cnt)
        self.literal("metrics.data_monitor.data_loss.data_drop.%s.reason" % drop_code).set_literal(drop_reason)

    """
    data_monitor / data_delay
    获取数据延迟窗口时间
    counter: metrics.data_monitor.data_delay.window_time
    """

    def get_delay_window_time_counter(self):
        return self.counter("metrics.data_monitor.data_delay.window_time")

    """
    设置数据延迟窗口时间
    counter: metrics.data_monitor.data_delay.window_time
    """

    def set_delay_window_time_counter(self, window_time):
        self.counter("metrics.data_monitor.data_delay.window_time").inc(window_time)

    """
    获取数据延迟等待时间
    counter: metrics.data_monitor.data_delay.waiting_time
    """

    def get_delay_waiting_time_counter(self):
        return self.counter("metrics.data_monitor.data_delay.waiting_time")

    """
    设置数据延迟等待时间
    counter: metrics.data_monitor.data_delay.waiting_time
    """

    def set_delay_waiting_time_counter(self, waiting_time):
        self.counter("metrics.data_monitor.data_delay.waiting_time").inc(waiting_time)

    # '''
    # 设置最小延迟相关
    # literal: metrics.data_monitor.data_delay.min_delay.data_time
    # '''
    #
    # def setDelayMinDelayLiteral(self, dataTime):
    #     timeUtils = new
    #     TimeUtils(null)
    #     TimeDeltaUtils
    #     timeDeltaUtils = new
    #     TimeDeltaUtils(timeUtils, dataTime)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.data_time").set_literal(dataTime)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.output_time").set_literal(timeUtils)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.delay_time").set_literal(timeDeltaUtils)

    """
    设置最小延迟相关
    literal: metrics.data_monitor.data_delay.min_delay.data_time
    """

    def set_delay_min_delay_literal(self, data_time, output_time):
        self.literal("metrics.data_monitor.data_delay.min_delay.data_time").set_literal(data_time)
        self.literal("metrics.data_monitor.data_delay.min_delay.output_time").set_literal(output_time)
        self.literal("metrics.data_monitor.data_delay.min_delay.delay_time").set_literal(output_time - data_time)

    # '''
    # 设置最大延迟相关
    # literal: metrics.data_monitor.data_delay.max_delay.data_time
    # '''
    #
    # def setDelayMaxDelayLiteral(self, dataTime):
    #     TimeUtils timeUtils = new TimeUtils(null);
    #     TimeDeltaUtils timeDeltaUtils = new TimeDeltaUtils(timeUtils, dataTime)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.data_time").set_literal(dataTime)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.output_time").set_literal(timeUtils)
    #     metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.delay_time").set_literal(timeDeltaUtils)

    """
    设置最大延迟相关
    literal: metrics.data_monitor.data_delay.max_delay.data_time
    """

    def set_delay_max_delay_literal(self, data_time, output_time):
        self.literal("metrics.data_monitor.data_delay.max_delay.data_time").set_literal(data_time)
        self.literal("metrics.data_monitor.data_delay.max_delay.output_time").set_literal(output_time)
        self.literal("metrics.data_monitor.data_delay.max_delay.delay_time").set_literal(output_time - data_time)

    """
    data_profiling
    获取数据质量相关
    counter: metrics.data_profiling.data_structure.data_malformed.field.cnt
    """

    def get_data_malformed_counter(self, field, reason):
        counter = self.counter("metrics.data_profiling.data_structure.data_malformed.%s.cnt" % field)
        self.literal("metrics.data_profiling.data_structure.data_malformed.%s.reason" % field).set_literal(reason)
        return counter

    """
    设置数据质量相关
    counter: metrics.data_profiling.data_structure.data_malformed.field.cnt
    """

    def set_data_malformed_counter(self, field, reason, cnt):
        self.counter("metrics.data_profiling.data_structure.data_malformed.%s.cnt" % field).inc(cnt)
        self.literal("metrics.data_profiling.data_structure.data_malformed.%s.reason" % field).set_literal(reason)
