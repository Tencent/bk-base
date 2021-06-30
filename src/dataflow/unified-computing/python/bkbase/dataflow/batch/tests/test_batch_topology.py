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
from bkbase.dataflow.batch.gateway import py_gateway
from bkbase.dataflow.batch.monitor.monitor_handler import MonitorHandler
from bkbase.dataflow.batch.topo.batch_topology_builder import BatchTopoBuilder
from bkbase.dataflow.batch.utils.custom_type import WindowType
from pyspark.sql import SparkSession


@pytest.fixture(scope="function")
def mock_launch_session():
    def mock_func(*args, **kwargs):
        spark = SparkSession.builder.appName("example").master("local").getOrCreate()
        return spark

    py_gateway.launch_spark_session = mock_func


class TestTopology(object):
    def test_build_topology_tumbling(self):
        params = {
            "geog_area_code": "inland",
            "user_main_class": "com.user.test_user.UserTransform",
            "nodes": {
                "source": {
                    "591_test_input": {
                        "count_freq": -1,
                        "description": "591_test_input",
                        "schedule_period": "",
                        "id": "591_test_input",
                        "name": "591_test_input",
                        "fields": [
                            {"origin": "591_test_input", "field": "field1", "type": "string", "description": "field1"}
                        ],
                        "window": {
                            "batch_window_offset": 0,
                            "batch_window_offset_unit": "",
                            "length": 2,
                            "segment": {"start": 0, "end": 2, "unit": "day"},
                        },
                        "role": "STREAM",
                        "is_managed": 1,
                        "input": {
                            "type": "hdfs",
                            "conf": {
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "data_type": "parquet",
                                "physical_table_name": "/test/591_test_input",
                            },
                        },
                        "self_dependency_mode": "",
                    }
                },
                "transform": {
                    "window": {"type": "tumbling", "count_freq": 1, "period_unit": "day"},
                    "parents": ["591_test_input"],
                },
                "sink": {
                    "591_test_output": {
                        "output": {
                            "type": "hdfs",
                            "extra_storage": False,
                            "conf": {
                                "data_type": "parquet",
                                "rt_id": "591_test_output",
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "physical_table_name": "/test/591_test_output",
                            },
                        },
                        "description": "591_test_output",
                        "role": "BATCH",
                        "id": "591_test_output",
                        "name": "591_test_output",
                    }
                },
            },
            "job_id": "591_test_output",
            "user_package_path": "hdfs://hdfs_cluster/test.zip",
            "engine_conf": {
                "spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT": "1",
                "spark.executor.memory": "8g",
                "spark.executor.memoryOverhead": "4g",
                "spark.executor.cores": 4,
                "spark.yarn.queue": "default",
            },
            "user_args": ["1", "2", "3"],
            "job_type": "spark_python_code",
            "job_name": "591_test_output",
            "run_mode": "product",
        }
        schedule_info = {"schedule_time": 1608742800000, "schedule_id": "591_test_output", "exec_id": 123}
        builder = BatchTopoBuilder(params, schedule_info)
        topology = builder.build()

        # 对关键属性进行一一校验
        # 检查节点个数及名称
        assert len(topology.source_nodes) == 1
        assert "591_test_input" in topology.source_nodes
        assert len(topology.sink_nodes) == 1
        assert "591_test_output" in topology.sink_nodes

        # 检查source的window_type
        source_node = topology.source_nodes["591_test_input"]
        # 检查窗口信息
        window = source_node.window
        assert (
            window.batch_window_offset == 0
            and window.start == 0
            and window.end == 2
            and window.window_type == WindowType.TUMBLING
        )

        # 检查输入路径
        input_paths = source_node.input
        # tumbling窗口，应该是调度时间202012240100送去start与end，同时unit为day，所以input_path应该为
        # 2020122201，2020122202....2020122323...结合相关的前缀，这里正确的值应该为
        # ['hdfs://hdfs_cluster/test/591_test_input/2020/12/22/01',
        #  'hdfs://hdfs_cluster/test/591_test_input/2020/12/22/02'
        # ...
        #  'hdfs://hdfs_cluster/test/591_test_input/2020/12/23/23',
        #  'hdfs://hdfs_cluster/test/591_test_input/2020/12/24/00']
        assert len(input_paths) == 48
        for ind in range(0, 23):
            sub_index = str(ind + 1) if ind + 1 > 9 else "0{}".format(str(ind + 1))
            assert input_paths[ind] == "hdfs://hdfs_cluster/test/591_test_input/2020/12/22/{}".format(sub_index)
        for ind in range(23, 47):
            sub_index = str(ind - 23) if ind - 23 > 9 else "0{}".format(str(ind - 23))
            assert input_paths[ind] == "hdfs://hdfs_cluster/test/591_test_input/2020/12/23/{}".format(sub_index)
        assert input_paths[47] == "hdfs://hdfs_cluster/test/591_test_input/2020/12/24/00"

        # 检查输出路径
        sink_node = topology.sink_nodes["591_test_output"]
        # 检查时间戳：调度时间送去最小窗口（48小时）
        assert sink_node.event_time == 1608570000000
        # 检查输出路径
        assert sink_node.output == "hdfs://hdfs_cluster/test/591_test_output/2020/12/22/01"

    def test_build_topology_accumulate(self):
        params = {
            "geog_area_code": "inland",
            "user_main_class": "com.user.test_user.UserTransform",
            "nodes": {
                "source": {
                    "591_test_input": {
                        "count_freq": -1,
                        "description": "591_test_input",
                        "schedule_period": "",
                        "id": "591_test_input",
                        "name": "591_test_input",
                        "fields": [
                            {"origin": "591_test_input", "field": "field1", "type": "string", "description": "field1"}
                        ],
                        "window": {
                            "batch_window_offset": 0,
                            "batch_window_offset_unit": "",
                            "length": 2,
                            "segment": {"start": 6, "end": 14, "unit": "day"},
                        },
                        "role": "STREAM",
                        "is_managed": 1,
                        "input": {
                            "type": "hdfs",
                            "conf": {
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "data_type": "parquet",
                                "physical_table_name": "/test/591_test_input",
                            },
                        },
                        "self_dependency_mode": "",
                    }
                },
                "transform": {
                    "window": {"type": "accumulate", "count_freq": 1, "period_unit": "day"},
                    "parents": ["591_test_input"],
                },
                "sink": {
                    "591_test_output": {
                        "output": {
                            "type": "hdfs",
                            "extra_storage": False,
                            "conf": {
                                "data_type": "parquet",
                                "rt_id": "591_test_output",
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "physical_table_name": "/test/591_test_output",
                            },
                        },
                        "description": "591_test_output",
                        "role": "BATCH",
                        "id": "591_test_output",
                        "name": "591_test_output",
                    }
                },
            },
            "job_id": "591_test_output",
            "user_package_path": "hdfs://hdfs_cluster/test.zip",
            "engine_conf": {
                "spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT": "1",
                "spark.executor.memory": "8g",
                "spark.executor.memoryOverhead": "4g",
                "spark.executor.cores": 4,
                "spark.yarn.queue": "default",
            },
            "user_args": ["1", "2", "3"],
            "job_type": "spark_python_code",
            "job_name": "591_test_output",
            "run_mode": "product",
        }
        schedule_info = {"schedule_time": 1608789600000, "schedule_id": "591_test_output", "exec_id": 123}
        builder = BatchTopoBuilder(params, schedule_info)
        topology = builder.build()

        # 对关键属性进行一一校验
        # 检查节点个数及名称
        assert len(topology.source_nodes) == 1
        assert "591_test_input" in topology.source_nodes
        assert len(topology.sink_nodes) == 1
        assert "591_test_output" in topology.sink_nodes

        # 检查source的window_type
        source_node = topology.source_nodes["591_test_input"]
        # 检查窗口信息
        window = source_node.window
        assert (
            window.batch_window_offset == 0
            and window.start == 6
            and window.end == 14
            and window.window_type == WindowType.ACCUMULATION
        )

        # 检查输入路径
        input_paths = source_node.input
        # accumulate窗口，应该是调度时间202012241400与start之前的时间段
        # 2020122406，2020122407....2020122413结合相关的前缀，这里正确的值应该为
        # ['hdfs://hdfs_cluster/test/591_test_input/2020/12/24/06',
        #  'hdfs://hdfs_cluster/test/591_test_input/2020/12/24/13']
        assert len(input_paths) == 8
        for ind in range(0, 8):
            sub_index = str(ind + 6) if ind + 6 > 9 else "0{}".format(str(ind + 6))
            assert input_paths[ind] == "hdfs://hdfs_cluster/test/591_test_input/2020/12/24/{}".format(sub_index)

        # 检查输出路径
        sink_node = topology.sink_nodes["591_test_output"]
        # 检查时间戳：调度时间送去最小窗口（8*24小时）
        assert sink_node.event_time == 1608098400000
        # 检查输出路径
        assert sink_node.output == "hdfs://hdfs_cluster/test/591_test_output/2020/12/16/14"

    def test_monitor_handler(self):
        params = {
            "geog_area_code": "inland",
            "user_main_class": "com.user.test_user.UserTransform",
            "nodes": {
                "source": {
                    "591_test_input": {
                        "count_freq": -1,
                        "description": "591_test_input",
                        "schedule_period": "",
                        "id": "591_test_input",
                        "name": "591_test_input",
                        "fields": [
                            {"origin": "591_test_input", "field": "field1", "type": "string", "description": "field1"}
                        ],
                        "window": {
                            "batch_window_offset": 0,
                            "batch_window_offset_unit": "",
                            "length": 2,
                            "segment": {"start": 0, "end": 2, "unit": "day"},
                        },
                        "role": "STREAM",
                        "is_managed": 1,
                        "input": {
                            "type": "hdfs",
                            "conf": {
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "data_type": "parquet",
                                "physical_table_name": "/test/591_test_input",
                            },
                        },
                        "self_dependency_mode": "",
                    }
                },
                "transform": {
                    "window": {"type": "tumbling", "count_freq": 1, "period_unit": "day"},
                    "parents": ["591_test_input"],
                },
                "sink": {
                    "591_test_output": {
                        "output": {
                            "type": "hdfs",
                            "extra_storage": False,
                            "conf": {
                                "data_type": "parquet",
                                "rt_id": "591_test_output",
                                "cluster_name": "hdfs_cluster",
                                "name_service": "hdfs://hdfs_cluster",
                                "cluster_group": "default",
                                "physical_table_name": "/test/591_test_output",
                            },
                        },
                        "description": "591_test_output",
                        "role": "BATCH",
                        "id": "591_test_output",
                        "name": "591_test_output",
                    }
                },
            },
            "job_id": "591_test_output",
            "user_package_path": "hdfs://hdfs_cluster/test.zip",
            "engine_conf": {
                "spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT": "1",
                "spark.executor.memory": "8g",
                "spark.executor.memoryOverhead": "4g",
                "spark.executor.cores": 4,
                "spark.yarn.queue": "default",
            },
            "user_args": ["1", "2", "3"],
            "job_type": "spark_python_code",
            "job_name": "591_test_output",
            "run_mode": "product",
        }
        schedule_info = {"schedule_time": 1608742800000, "schedule_id": "591_test_output", "exec_id": 123}
        builder = BatchTopoBuilder(params, schedule_info)
        topology = builder.build()
        handler = MonitorHandler(topology, "591_test_output")
        handler.add_input_count(3, "591_test_input")
        handler.add_output_count(4, "591_test_output")
        params = handler.generate_monitor_params()
        tags = params["tags"]
        assert len(tags) == 1 and tags[0] == "inland"
        input_result = params["message"]["metrics"]["data_monitor"]["data_loss"]["input"]
        output_result = params["message"]["metrics"]["data_monitor"]["data_loss"]["output"]

        input_tag = "{}|{}_{}".format(
            "591_test_input", int(1608742800000 / 1000 - 2 * 24 * 3600), int(1608742800000 / 1000)
        )
        output_tag = "{}|{}_{}".format("591_test_output", 1608570000000, 1608742800000)
        assert input_result["tags"][input_tag] == 3
        assert output_result["tags"][output_tag] == 4
        assert input_result["total_cnt"] == 3
        assert input_result["total_cnt_increment"] == 3
        assert output_result["total_cnt"] == 4
        assert output_result["total_cnt_increment"] == 4

        # 再次增加
        handler.add_input_count(3, "591_test_input")
        handler.add_output_count(4, "591_test_output")
        params = handler.generate_monitor_params()
        input_result = params["message"]["metrics"]["data_monitor"]["data_loss"]["input"]
        output_result = params["message"]["metrics"]["data_monitor"]["data_loss"]["output"]
        assert input_result["tags"][input_tag] == 3
        assert output_result["tags"][output_tag] == 4
        assert input_result["total_cnt"] == 6
        assert input_result["total_cnt_increment"] == 6
        assert output_result["total_cnt"] == 8
        assert output_result["total_cnt_increment"] == 8
