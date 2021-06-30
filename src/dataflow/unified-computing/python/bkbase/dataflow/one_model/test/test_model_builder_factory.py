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
from bkbase.dataflow.one_model.topo.deeplearning_sink_node import (
    ModelIcebergQuerySetSinkNodeBuilder,
    ModelIcebergResultSetSinkNodeBuilder,
    ModelIcebergResultSetSinkNodeBuilderV2,
    ModelSinkNodeBuilder,
)
from bkbase.dataflow.one_model.topo.deeplearning_source_node import (
    ModelIcebergDebugSourceNodeBuilder,
    ModelIceBergQuerySetSourceNodeBuilder,
    ModelIcebergResultSetSourceNodeBuilder,
    ModelIcebergResultSetSourceNodeBuilderV2,
    ModelSourceNodeBuilder,
)
from bkbase.dataflow.one_model.topo.model_builder_factory import (
    ModelSinkBuilderFactory,
    ModelSourceBuilderFactory,
)
from bkbase.dataflow.one_model.utils.deeplearning_constant import NodeType


class TestModelBuilderFactory(object):
    @pytest.mark.usefixtures("mock_get_parent_info")
    def test_model_source_builder(self):
        schedule_time = 1608739200000
        job_type = "tensorflow"
        is_debug = 0
        version = "v2"
        source_factory = ModelSourceBuilderFactory(schedule_time, job_type, is_debug, version)
        # source为模型
        source_node_info = {
            "input": {"path": "hdfs://test", "format": "parquet"},
            "id": "model_source_node",
            "name": "model_source_node",
            "type": NodeType.MODEL.value,
        }

        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelSourceNodeBuilder)

        # source为表，v2版本，icerberg + result_set + scroll
        source_node_info = {
            "schedule_time": 1608742800000,
            "accumulate_start_time": 1608742800000,
            "feature_shape": 0,
            "parent_node_url": "http://xxxx/",
            "storage_conf": {
                "data_type": "iceberg",
                "cluster_name": "hdfs_cluster",
                "name_service": "hdfs://xxxx",
                "storekit_hdfs_conf": {},
                "cluster_group": "default",
                "physical_table_name": "model_test_table",
            },
            "window_offset": "0H",
            "window_start_offset": "0H",
            "window_end_offset": "1H",
            "storage_type": "hdfs",
            "id": "model_test_table",
            "window_type": "scroll",
            "window_size": "1H",
            "name": "model_test_table",
            "fields": [{"origin": "model_test_table", "field": "field_1", "type": "double", "description": "field_1"}],
            "label_shape": 0,
            "role": "batch",
            "is_managed": 1,
        }
        factory = source_factory.get_builder(source_node_info)

        assert isinstance(factory, ModelIcebergResultSetSourceNodeBuilderV2)

        # source为表，v2版本，icerberg + result_set + accumulate
        source_node_info["window_type"] = "accumulate"
        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelIcebergResultSetSourceNodeBuilderV2)

        # source为表，v1版本，queryset
        source_node_info = {
            "input": {"table_type": "query_set"},
            "id": "model_test_table",
            "name": "model_test_table",
            "iceberg_config": {"physical_table_name": "model_test_table"},
            "fields": [{"origin": "model_test_table", "field": "field_1", "type": "double", "description": "field_1"}],
        }
        source_factory.version = "v1"
        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelIceBergQuerySetSourceNodeBuilder)

        # source为表，v1版本，result_table, not debug + accumulate
        source_node_info = {
            "input": {"table_type": "result_table"},
            "iceberg_config": {"physical_table_name": "model_test_table"},
            "id": "model_test_table",
            "name": "model_test_table",
            "parent_node_url": "http://xxxx",
            "window": {
                "accumulate": True,
                "schedule_time": 1608742800000,
                "data_start": 0,
                "data_end": 1,
                "window_size": 1,
                "window_delay": 0,
                "window_size_period": "hour",
            },
            "iceberg_table_name": "model_test_table",
            "fields": [{"origin": "model_test_table", "field": "field_1", "type": "double", "description": "field_1"}],
        }
        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelIcebergResultSetSourceNodeBuilder)

        source_node_info["window"]["accumulate"] = False
        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelIcebergResultSetSourceNodeBuilder)
        source_factory.debug_label = 1
        factory = source_factory.get_builder(source_node_info)
        assert isinstance(factory, ModelIcebergDebugSourceNodeBuilder)

    def test_model_sink_builder(self):
        source_nodes = {
            "model_test_table": {
                "type": NodeType.DATA.value,
                "input": {"table_type": "result_table"},
                "iceberg_config": {"physical_table_name": "model_test_table"},
                "id": "model_test_table",
                "name": "model_test_table",
                "parent_node_url": "http://xxxx",
                "window": {
                    "accumulate": True,
                    "schedule_time": 1608742800000,
                    "data_start": 0,
                    "data_end": 1,
                    "window_size": 1,
                    "window_delay": 0,
                    "window_size_period": "hour",
                    "schedule_period": "hour",
                    "count_freq": 2,
                },
                "iceberg_table_name": "model_test_table",
                "fields": [
                    {"origin": "model_test_table", "field": "field_1", "type": "double", "description": "field_1"}
                ],
            },
            "model_test_table_1": {
                "type": NodeType.DATA.value,
                "input": {"table_type": "result_table"},
                "iceberg_config": {"physical_table_name": "model_test_table_1"},
                "id": "model_test_table_1",
                "name": "model_test_table_1",
                "parent_node_url": "http://xxxx",
                "window": {
                    "accumulate": True,
                    "schedule_time": 1608742800000,
                    "data_start": 0,
                    "data_end": 1,
                    "window_size": 1,
                    "window_delay": 0,
                    "window_size_period": "hour",
                    "schedule_period": "hour",
                    "count_freq": 1,
                },
                "iceberg_table_name": "model_test_table_1",
                "fields": [
                    {"origin": "model_test_table_1", "field": "field_1", "type": "double", "description": "field_1"}
                ],
            },
        }
        job_type = "tensorflow"
        schedule_time = 1608742800000
        version = "v2"
        sink_factory = ModelSinkBuilderFactory(job_type, schedule_time, source_nodes, version)
        sink_node = {
            "id": "output_model",
            "name": "output_model",
            "type": NodeType.MODEL.value,
            "output": {
                "path": "hdfs://xxxx",
                "format": "parquet",
                "mode": "overwrite",
                "cluster_group": "default",
                "hdfs_user": "default",
                "component_url": "http://xxx",
            },
            "fieds": [],
        }
        factory = sink_factory.get_builder(sink_node)
        assert isinstance(factory, ModelSinkNodeBuilder)

        sink_node = {
            "type": NodeType.DATA.value,
            "output": {
                "path": "hdfs://xxxx",
                "format": "parquet",
                "mode": "overwrite",
                "table_type": "result_table",
                "iceberg_config": {"physical_table_name": "model_output_test"},
            },
            "storage_conf": {"storekit_hdfs_conf": {"physical_table_name": "model_output_test"}},
            "schedule_time": schedule_time,
            "data_time_offset": "1h",
            "id": "output_table",
            "name": "output_table",
            "fields": [],
        }
        factory = sink_factory.get_builder(sink_node)
        assert isinstance(factory, ModelIcebergResultSetSinkNodeBuilderV2)

        sink_factory.version = "v1"
        sink_node["output"]["table_type"] = "query_set"
        factory = sink_factory.get_builder(sink_node)
        assert isinstance(factory, ModelIcebergQuerySetSinkNodeBuilder)

        sink_node["output"]["table_type"] = "result_table"
        factory = sink_factory.get_builder(sink_node)
        assert isinstance(factory, ModelIcebergResultSetSinkNodeBuilder)

    def test_model_transform_builder(self):
        pass
