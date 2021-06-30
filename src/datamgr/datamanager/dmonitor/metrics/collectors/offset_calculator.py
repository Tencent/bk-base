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
import copy
import json
import logging
import time

from utils.time import strtotime, timetostr

from dmonitor.settings import DMONITOR_TOPICS


class CntCalculator(object):
    def __init__(self, task, window_size, output_measurement, retention_policy=None):
        self._task = task
        self.window_size = window_size
        self.output_measurement = output_measurement
        self.retention_policy = retention_policy
        self.window_buffer = {}
        super(CntCalculator, self).__init__()

    def clean_buffer(self, nowtime):
        deleting_setids = []
        for setid in self.window_buffer.keys():
            deleting_topics = []
            for topic in self.window_buffer[setid].keys():
                deleting_partitions = []
                for partition in self.window_buffer[setid][topic].keys():
                    deleting_time_keys = []
                    for time_key in self.window_buffer[setid][topic][partition].keys():
                        window = self.window_buffer[setid][topic][partition][time_key]
                        if window.timeout(nowtime):
                            deleting_time_keys.append(time_key)
                    for time_key in deleting_time_keys:
                        del self.window_buffer[setid][topic][partition][time_key]
                    if len(self.window_buffer[setid][topic][partition]) < 1:
                        deleting_partitions.append(partition)
                for partition in deleting_partitions:
                    del self.window_buffer[setid][topic][partition]
                if len(self.window_buffer[setid][topic]) < 1:
                    deleting_topics.append(topic)
            for topic in deleting_topics:
                del self.window_buffer[setid][topic]
            if len(self.window_buffer[setid]) < 1:
                deleting_setids.append(setid)
        for setid in deleting_setids:
            del self.window_buffer[setid]
        return True

    def add_point(self, offset_metric):
        # 规范化time, 如果是localtime的string统一转为int
        point_time = offset_metric.get("time", 0)
        try:
            if isinstance(point_time, str):
                offset_metric["time"] = strtotime(point_time)
        except Exception:
            pass
        # 维度信息
        setid = offset_metric.get("setid", "unknown")
        topic = offset_metric.get("topic", "unknown")
        partition = offset_metric.get("partition", "unknown")
        point_time = offset_metric["time"]
        is_new = offset_metric["is_new"]

        if topic == "bkdata_monitor_metrics591":
            logging.info(
                "[TRACE TOPIC] GET POINT %s, %s, %s "
                % (topic, partition, timetostr(point_time))
            )
        # 获得此维度缓存的窗口
        if not self.window_buffer.get(setid, False):
            self.window_buffer[setid] = {}
        if not self.window_buffer[setid].get(topic, False):
            self.window_buffer[setid][topic] = {}
        if not self.window_buffer[setid][topic].get(partition, False):
            self.window_buffer[setid][topic][partition] = {}
        windows = self.window_buffer[setid][topic][partition]

        # 可能此point所在的window
        effect_start_time = point_time - 5 * self.window_size
        effect_end_time = point_time + 5 * self.window_size

        # 处理时间戳0非0点的问题
        zero_addition = 28800  # 0时间戳相对于整天多出的秒数
        window_start_time = (
            (effect_start_time - zero_addition % self.window_size)
            - (
                (effect_start_time - zero_addition % self.window_size)
                % self.window_size
            )
            - (zero_addition % self.window_size)
        )

        while window_start_time < effect_end_time:
            # 如果窗口不存在， 则创建
            window = windows.get(window_start_time, False)
            if not window:
                window_kwargs = {
                    "window_size": self.window_size,
                    "window_time": window_start_time,
                    "setid": setid,
                    "topic": topic,
                    "partition": partition,
                    "measurement": self.output_measurement,
                    "retention_policy": self.retention_policy,
                    "is_new": is_new,
                }
                window = CalWindow(self._task, **window_kwargs)
                self.window_buffer[setid][topic][partition][window_start_time] = window
            window.update_point(offset_metric)
            window_start_time += self.window_size

        return True


class CalWindow(object):
    def __init__(self, monitor, **kwargs):
        super(CalWindow, self).__init__()
        self._task = monitor
        self.window_size = kwargs.get("window_size", 60)
        self.window_time = kwargs.get("window_time", time.time())
        # 维度信息
        self.setid = kwargs.get("setid", "unknown")
        self.topic = kwargs.get("topic", "unknown")
        self.partition = kwargs.get("partition", "unknown")

        # 每个窗口保存3个offset上报的point， 分别是窗口之前最后一次上报， 窗口内最后一次上报， 以及窗口之后第一次上报
        # 窗口开始之前的最后一次上报
        if kwargs.get("is_new", False):
            self.left_point = kwargs.get(
                "left_offset",
                {
                    "time": self.window_time - self.window_size,
                    "offset": 0,
                    "partition": self.partition,
                    "topic": self.topic,
                    "setid": self.setid,
                },
            )
        else:
            self.left_point = kwargs.get("left_offset", False)
        # 窗口内最后一次上报
        self.in_point = kwargs.get("in_point", False)
        # 窗口结束之后第一次上报
        self.right_point = kwargs.get("right_point", False)
        # 当前窗口的值
        self.cnt = False
        # 上一次输出的结果
        self.last_output = False

        # 此窗口的输出数据到哪个measurement
        self.output_measurement = kwargs.get("measurement", "kafka_topic_message_cnt")
        self.retention_policy = kwargs.get("retention_policy", None)
        # 窗口上一次更新point的时间，用于清理窗口
        self.last_update_point = time.time()

    def __str__(self):
        return "[%s][%s][%s](%s - %s): %s\nleft[%s]: %s\nin[%s]: %s\nright[%s]: %s" % (
            self.setid,
            self.topic,
            self.partition,
            timetostr(self.window_time),
            timetostr(self.window_time + self.window_size),
            self.cnt,
            False
            if self.left_point is False
            else timetostr(self.left_point.get("time", 0)),
            self.left_point,
            False
            if self.in_point is False
            else timetostr(self.in_point.get("time", 0)),
            self.in_point,
            False
            if self.right_point is False
            else timetostr(self.right_point.get("time", 0)),
            self.right_point,
        )

    def timeout(self, nowtime=False):
        if not nowtime:
            nowtime = time.time()
        if nowtime - self.last_update_point > self.window_size * 20:
            return True
        else:
            return False

    def window_cnt(self):
        if self.in_point is False:
            cnt = self.get_cnt_without_in_point()
        else:
            cnt = self.get_cnt_with_in_point()

        if not cnt:
            return False

        self.cnt = int(cnt)
        if (self.last_output is False) or (self.cnt != self.last_output):
            self.last_output = self.cnt
            self.output()
        return True

    def get_cnt_without_in_point(self):
        if (self.left_point is False) or (self.right_point is False):
            # 当前窗口不足2次上报， 不能计算
            return False
        # 使用right-left算平均值，再乘以size
        right_offset = int(self.right_point.get("offset") or 0)
        left_offset = int(self.left_point.get("offset") or 0)
        left_time = self.left_point.get("time", 0)
        right_time = self.right_point.get("time", 0)
        incre = right_offset - left_offset
        incre = 0 if incre < 0 else incre
        time_span = right_time - left_time
        # 计算每秒上报速率， 一定要整除
        remain = 0 if time_span == 0 else (incre % time_span)
        cnt_per_sec = 0 if time_span == 0 else ((incre - remain) / time_span)
        cnt = cnt_per_sec * self.window_size
        return cnt

    def get_cnt_with_in_point(self):
        left_half = False
        if self.left_point is not False:
            # 算left 至 in 的这一段
            start_offset = int(self.left_point.get("offset") or 0)
            end_offset = int(self.in_point.get("offset") or 0)
            start_time = self.left_point.get("time", 0)
            end_time = self.in_point.get("time", 0)
            incre = end_offset - start_offset
            incre = 0 if incre < 0 else incre
            time_span = end_time - start_time
            # 计算每秒上报速率， 一定要整除
            remain = 0 if time_span == 0 else (incre % time_span)
            cnt_per_sec = 0 if time_span == 0 else ((incre - remain) / time_span)
            left_half = cnt_per_sec * (end_time % self.window_size) + remain

        right_half = False
        if self.right_point is not False:
            # 算in 至 right的这一段
            start_offset = int(self.in_point.get("offset") or 0)
            end_offset = int(self.right_point.get("offset") or 0)
            start_time = self.in_point.get("time", 0)
            end_time = self.right_point.get("time", 0)
            incre = end_offset - start_offset
            incre = 0 if incre < 0 else incre
            time_span = end_time - start_time
            # 计算每秒上报速率， 一定要整除
            remain = 0 if time_span == 0 else (incre % time_span)
            cnt_per_sec = (incre - remain) / time_span
            right_half = cnt_per_sec * (
                self.window_size - start_time % self.window_size
            )

        if (left_half is False) and (right_half is False):
            # 当前窗口不足2次上报， 不能计算
            return False
        else:
            cnt = left_half + right_half

        return cnt

    def update_point(self, point):
        """
        在可能影响到某窗口的上报到来时， 通过此确认窗口是否受到了影响
        如果真的有更准确的点可以使用， 则重新输出窗口统计信息
        :param point:
        :return:
        """
        self.last_update_point = time.time()
        point_time = point.get("time", 0)
        left_point_time = (
            self.left_point.get("time", 0) if self.left_point is not False else 0
        )
        in_point_time = (
            self.in_point.get("time", 0)
            if self.in_point is not False
            else (self.window_time - 1)
        )
        right_point_time = (
            self.right_point.get("time", 0)
            if self.right_point is not False
            else (point_time + 1)
        )
        if left_point_time < point_time < self.window_time:
            # 当前新加入的point比left_point效果好
            self.left_point = point
        elif in_point_time < point_time < (self.window_time + self.window_size):
            self.in_point = point
        elif (self.window_time + self.window_size) <= point_time < right_point_time:
            self.right_point = point
        else:
            # 无需更新，当前已用最优上报数据计算
            return True
        # 重新计算
        return self.window_cnt()

    def output(self):
        metric_info = {
            "time": self.window_time,
            "database": "monitor_data_metrics",
            "retention_policy": self.retention_policy,
            self.output_measurement: {
                "msg_inc_cnt": self.cnt,
                "cnt": self.cnt,
                "tags": {
                    "partition": "%s" % self.partition,
                    "topic": self.topic,
                    "setid": self.setid,
                },
            },
        }

        message = json.dumps(metric_info)
        self._task.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            message,
            partition=self._task._target_partition,
        )
        self._task.produce_metric(DMONITOR_TOPICS["data_io_total"], message)

        # 先双写两个RP, 后面做到无缝切换
        if self.retention_policy == "rp_month":
            backup_metric_info = copy.copy(metric_info)
            backup_metric_info["retention_policy"] = None
            self._task.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                json.dumps(backup_metric_info),
                partition=self._task._target_partition,
            )
        return True
