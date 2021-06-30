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


def map_window_config(window_info):
    window_config = {}
    if "type" in window_info:
        window_config["type"] = window_info["type"]
    if "length" in window_info:
        window_config["length"] = window_info["length"]
    if "count_freq" in window_info:
        window_config["count_freq"] = window_info["count_freq"]
    if "waiting_time" in window_info:
        window_config["waiting_time"] = window_info["waiting_time"]
    if "period_unit" in window_info:
        window_config["period_unit"] = window_info["period_unit"]
    if "delay" in window_info:
        window_config["delay"] = window_info["delay"]
    if "session_gap" in window_info:
        window_config["session_gap"] = window_info["session_gap"]
    if "expired_time" in window_info:
        window_config["expired_time"] = window_info["expired_time"]
    if "lateness_time" in window_info:
        window_config["lateness_time"] = window_info["lateness_time"]
    if "lateness_count_freq" in window_info:
        window_config["lateness_count_freq"] = window_info["lateness_count_freq"]
    if "segment" in window_info:
        segment_object = window_info["segment"]
        segment = {"start": segment_object["start"], "end": segment_object["end"], "unit": segment_object["unit"]}
        window_config["segment"] = segment
    if "schedule_time" in window_info:
        window_config["schedule_time"] = window_info["schedule_time"]
    if "data_start" in window_info:
        window_config["data_start"] = window_info["data_start"]
    if "data_end" in window_info:
        window_config["data_end"] = window_info["data_end"]
    if "window_size" in window_info:
        window_config["window_size"] = window_info["window_size"]
    if "window_delay" in window_info:
        window_config["window_delay"] = window_info["window_delay"]
    if "window_size_period" in window_info:
        window_config["window_size_period"] = window_info["window_size_period"]
    return window_config
