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
from bkbase.dataflow.one_model.topo.deeplearning_topology import (
    DeepLearningTopology,
    DeepLearningTopologyBuilder,
)
from bkdata_datalake import tables


@pytest.fixture(scope="function")
def mock_load_table():
    def mock_func(*args, **kwargs):
        pass

    tables.load_table = mock_func


class TestTopology(object):
    @pytest.mark.usefixtures("mock_load_table")
    def test_build_nodes(self):
        params = {
            "schedule_time": 1608742800000,
            "bk_username": "xxxx",
            "metric": {"metric_rest_api_url": "http://xxxx.xx"},
            "deploy_config": {"engine_conf": {"worker": 2, "cpu": 1, "memory": "1.0Gi"}},
            "udf": [],
            "recovery_info": {"recovery_enable": False, "recovery_interval": "60m", "retry_times": 1},
            "resource": {"resource_group_id": "default", "queue_name": "default", "cluster_group": "default"},
            "job_id": "model_test_job",
            "time_zone": "Asia/Shanghai",
            "version": "v2",
            "batch_type": "tensorflow",
            "nodes": {
                "source": {
                    "591_model_input_table": {
                        "feature_shape": 0,
                        "parent_node_url": "http://xxxxx/",
                        "storage_conf": {
                            "data_type": "iceberg",
                            "cluster_name": "hdfs_cluster",
                            "name_service": "hdfs://xxxx",
                            "storekit_hdfs_conf": {"physical_table_name": "iceberg.591_model_input_table"},
                            "cluster_group": "default",
                            "physical_table_name": "iceberg.591_model_input_table",
                        },
                        "window_offset": "0H",
                        "storage_type": "hdfs",
                        "id": "591_model_input_table",
                        "window_type": "scroll",
                        "window_size": "1H",
                        "name": "591_model_input_table",
                        "fields": [
                            {
                                "origin": "591_model_input_table",
                                "field": "field1",
                                "type": "double",
                                "description": "field1",
                            }
                        ],
                        "label_shape": 0,
                        "role": "batch",
                        "is_managed": 1,
                    }
                },
                "transform": {
                    "591_model_transform": {
                        "processor_logic": {
                            "programming_language": "Python",
                            "user_package": "hdfs://xxxx/example.zip",
                            "user_args": "a=1",
                            "script": "script.py",
                        },
                        "processor": {"user_main_module": "script", "args": {"a": 1}, "type": "untrained-run"},
                        "id": "591_model_transform",
                        "processor_type": "tensorflow",
                        "name": "591_model_transform",
                    }
                },
                "sink": {
                    "591_model_output": {
                        "output": {
                            "format": "",
                            "hdfs_user": "root",
                            "component_url": "http://xxxx/",
                            "cluster_group": "default",
                            "path": "hdfs://xxxx",
                            "type": "hdfs",
                        },
                        "description": "591_model_output",
                        "type": "model",
                        "id": 2341,
                        "name": "591_model_output",
                    }
                },
            },
            "run_mode": "product",
            "job_name": None,
            "schedule_info": {
                "geog_area_code": "inland",
                "count_freq": 1,
                "schedule_period": "hour",
                "is_restart": False,
                "cluster_id": "default",
                "jobnavi_task_type": "tensorflow",
                "start_time": 1615392000000,
            },
        }
        builder = DeepLearningTopologyBuilder(params)
        topology = DeepLearningTopology(builder)
        assert len(topology.source_nodes) == 1
        assert "591_model_input_table" in topology.source_nodes
        assert len(topology.transform_nodes) == 1
        assert "591_model_transform" in topology.transform_nodes
        assert len(topology.sink_nodes) == 1
        assert "591_model_output" in topology.sink_nodes
