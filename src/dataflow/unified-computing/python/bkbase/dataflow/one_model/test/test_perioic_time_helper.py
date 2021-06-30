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
import pytest
from bkbase.dataflow.one_model.utils import deeplearning_logger
from bkbase.dataflow.one_model.utils.deeplearning_constant import PeriodUnit
from bkbase.dataflow.one_model.utils.periodic_time_helper import PeriodicTimeHelper


@pytest.mark.usefixtures("mock_get_parent_info")
def test_round_schedule_timestamp():
    schedule_time = 1608813199951
    data_time_in_hour = PeriodicTimeHelper.round_schedule_timestamp_to_hour(schedule_time)
    assert data_time_in_hour == 1608811200


@pytest.mark.usefixtures("mock_get_parent_info")
def test_min_window_info():
    source_nodes_1 = {
        "node_1": {
            "type": "data",
            "window": {
                "accumulate": False,
                "schedule_period": "week",
                "count_freq": 1,
                "window_size": 7,
                "window_size_period": "day",
            },
        },
        "node_2": {
            "type": "data",
            "window": {
                "accumulate": False,
                "schedule_period": "week",
                "count_freq": 1,
                "window_size": 2,
                "window_size_period": "hour",
            },
        },
    }
    min_window, min_unit = PeriodicTimeHelper.get_min_window_info(source_nodes_1)
    assert min_window == 2
    assert min_unit == PeriodUnit.HOUR.value


@pytest.mark.usefixtures("mock_get_parent_info")
def test_min_window_info_2():
    source_nodes_2 = {
        "node_1": {
            "type": "data",
            "window": {
                "accumulate": False,
                "schedule_period": "day",
                "count_freq": 1,
                "window_size": 1,
                "window_size_period": "day",
            },
        },
        "node_2": {
            "type": "data",
            "window": {
                "accumulate": False,
                "schedule_period": "day",
                "count_freq": 1,
                "window_size": 1,
                "window_size_period": "month",
            },
        },
        "node_3": {
            "type": "data",
            "window": {
                "accumulate": False,
                "schedule_period": "day",
                "count_freq": 1,
                "window_size": 1,
                "window_size_period": "hour",
            },
        },
    }
    min_window, min_unit = PeriodicTimeHelper.get_min_window_info(source_nodes_2)
    assert min_window == 1
    assert min_unit == PeriodUnit.MONTH.value


@pytest.mark.usefixtures("mock_get_parent_info")
def test_accumulate_start_end_time():
    node_id = "591_test_mock_table_10"
    window_info = {
        "schedule_time": 1608813199951,
        "window_type": "accumulate",
        "count_freq": 1,
        "data_end": 23,
        "schedule_period": "hour",
        "data_start": 4,
        "delay": 0,
        "accumulate": True,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_accumulate_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1608753600000
    assert end_time_in_mills == 1608811200000


@pytest.mark.usefixtures("mock_get_parent_info")
def test_window_input_start_end_time():
    # 当前任务小时，无延迟
    # 依赖父表小时，没有延迟，窗口大小为1
    node_id = "591_test_mock_table_column"
    window_info = {
        "schedule_time": 1608813199951,
        "window_type": "fixed",
        "window_size": 1,
        "count_freq": 1,
        "data_end": -1,
        "window_delay": 0,
        "schedule_period": "hour",
        "window_size_period": "hour",
        "data_start": -1,
        "delay": 0,
        "accumulate": False,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_window_input_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1608807600000
    assert end_time_in_mills == 1608811200000


@pytest.mark.usefixtures("mock_get_parent_info")
def test_window_input_start_end_time_1():
    # 当前任务为天
    # 父表小时，没有延迟， 窗口大小为1
    node_id = "591_test_mock_table_column"
    window_info = {
        "schedule_time": 1608739200000,
        "window_type": "fixed",
        "window_size": 1,
        "count_freq": 1,
        "data_end": -1,
        "window_delay": 0,
        "schedule_period": "day",
        "window_size_period": "day",
        "data_start": -1,
        "delay": 0,
        "accumulate": False,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_window_input_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1608652800000
    assert end_time_in_mills == 1608739200000


@pytest.mark.usefixtures("mock_get_parent_info")
def test_window_input_start_end_time_2():
    # 当前任务为天
    # 父表小时，依赖延迟1天， 窗口大小为1
    node_id = "591_test_mock_table_column"
    window_info = {
        "schedule_time": 1608739200000,
        "window_type": "fixed",
        "window_size": 1,
        "count_freq": 1,
        "data_end": -1,
        "window_delay": 1,
        "schedule_period": "day",
        "window_size_period": "day",
        "data_start": -1,
        "delay": 0,
        "accumulate": False,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_window_input_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1608566400000
    assert end_time_in_mills == 1608652800000


@pytest.mark.usefixtures("mock_get_parent_info")
def test_window_input_start_end_time_3():
    # 当前任务为月
    # 父表小时，依赖无延迟， 窗口大小为1
    node_id = "591_test_mock_table_column"
    window_info = {
        "schedule_time": 1606752000000,
        "window_type": "fixed",
        "window_size": 1,
        "count_freq": 1,
        "data_end": -1,
        "window_delay": 0,
        "schedule_period": "month",
        "window_size_period": "month",
        "data_start": -1,
        "delay": 0,
        "accumulate": False,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_window_input_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1604160000000
    assert end_time_in_mills == 1606752000000


@pytest.mark.usefixtures("mock_get_parent_info")
def test_window_input_start_end_time_4():
    # 当前任务为月
    # 父表小时，依赖延迟1个月， 窗口大小为1
    node_id = "591_test_mock_table_column"
    window_info = {
        "schedule_time": 1606752000000,
        "window_type": "fixed",
        "window_size": 1,
        "count_freq": 1,
        "data_end": -1,
        "window_delay": 1,
        "schedule_period": "month",
        "window_size_period": "month",
        "data_start": -1,
        "delay": 0,
        "accumulate": False,
        "dependency_rule": "all_finished",
        "advanced": {
            "self_dependency": False,
            "start_time": None,
            "recovery_enable": False,
            "self_dependency_config": {"fields": [], "dependency_rule": "self_finished"},
            "recovery_interval": "5m",
            "recovery_times": 1,
        },
    }
    helper = PeriodicTimeHelper(node_id, window_info)
    start_time_in_mills, end_time_in_mills = helper.get_window_input_start_end_time()
    deeplearning_logger.info("start:" + str(start_time_in_mills))
    deeplearning_logger.info("end:" + str(end_time_in_mills))
    assert start_time_in_mills == 1601481600000
    assert end_time_in_mills == 1604160000000


if __name__ == "__main__":
    test_round_schedule_timestamp()
    test_min_window_info()
    test_min_window_info_2()
    test_accumulate_start_end_time()
    test_window_input_start_end_time()
    test_window_input_start_end_time_1()
    test_window_input_start_end_time_2()
    test_window_input_start_end_time_3()
    test_window_input_start_end_time_4()
