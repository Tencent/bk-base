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

import mock as mock
from conf.dataapi_settings import (
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    UC_TIME_ZONE,
)
from rest_framework.test import APITestCase

import dataflow
from dataflow.pizza_settings import FLINK_STATE_CHECKPOINT_DIR
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.handlers import processing_version_config
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.handlers import processing_job_info
from dataflow.stream.job.flink_job import FlinkJob
from dataflow.stream.models import ProcessingStreamInfo, ProcessingStreamJob
from dataflow.stream.utils.checkpoint_manager import CheckpointManager


class TestFlinkJob(APITestCase):
    def setUp(self):
        job_id = "123"
        self._job = FlinkJob(job_id)
        processing_version_config.save(component_type="flink", branch="master", version="0.0.1")
        processing_job_info.save(
            job_id="123",
            processing_type="stream",
            component_type="flink",
            jobserver_config='{"geog_area_code":"inland","cluster_id":"default"}',
            cluster_group="default",
            implement_type="sql",
            programming_language="java",
            code_version="default",
            cluster_name=None,
            deploy_mode=None,
            created_at="2021-06-01 00:00:01",
            deploy_config="{}",
            job_config='{"heads":"591_avg_new_new2","tails":"591_avg_new_new2_ta",'
            + '"concurrency":null,"offset":1,'
            + '"processings":["591_avg_new_new2_ta"]}',
        )
        CheckpointManager.get_connection_info = mock.Mock(
            return_value={
                "enable_sentinel": False,
                "name_sentinel": "localhost",
                "host": "localhost",
                "port": 80,
                "password": "",
            }
        )

    def test_get_code_version(self):
        assert self._job.get_code_version() == "master-0.0.1.jar"

    @mock.patch.object(dataflow.stream.handlers.processing_stream_job, "get")
    def test_check_alter_checkpoint_info(self, processing_stream_job_get):
        deploy_config = '{"checkpoint":{"checkpoint_host": "localhost","checkpoint_port": 80}}'
        processing_stream_job_get.return_value = ProcessingStreamJob(stream_id="xxx", deploy_config=deploy_config)
        assert self._job._check_alter_checkpoint_info() is None

    def test_get_state_backend(self):
        assert self._job._get_state_backend() == "rocksdb"

    def test_get_checkpoint_interval(self):
        assert self._job._get_checkpoint_interval() == 600000

    @mock.patch.object(dataflow.stream.handlers.processing_stream_info, "get")
    def test_generate_nodes(self, processing_stream_info_get):
        processing_stream_info_get.return_value = ProcessingStreamInfo(
            processing_id="123_xxx", window="{}", processor_type="", processor_logic=""
        )
        MetaApiHelper.is_result_table_exist = mock.Mock(return_value=False)
        MetaApiHelper.get_result_table = mock.Mock(
            return_value={
                "data_processing": {"processing_id": "123_xxx"},
                "storages": {"hdfs": ""},
                "processing_type": "clean",
                "result_table_id": "123_xxx",
                "result_table_name": "xxxxx",
                "bk_biz_id": "123",
                "description": "",
                "fields": [
                    {"field_name": "_path_", "field_type": "string", "origins": "", "description": ""},
                    {"field_name": "platid", "field_type": "string", "origins": "", "description": ""},
                ],
            }
        )
        MetaApiHelper.get_data_processing_via_erp = mock.Mock(
            return_value={"inputs": [{"data_set_type": "result_table", "tags": [], "data_set_id": "123_xxxx"}]}
        )
        exp = {
            "source": {},
            "transform": {
                u"591_avg_new_new2_ta": {
                    "description": "",
                    "fields": [
                        {"origin": "", "field": "dtEventTime", "type": "string", "description": "event time"},
                        {"origin": "", "field": "_path_", "type": "string", "description": ""},
                        {"origin": "", "field": "platid", "type": "string", "description": ""},
                    ],
                    "window": {},
                    "id": "123_xxx",
                    "parents_info": [{"data_set_id": "123_xxxx", "tags": []}],
                    "parents": ["123_xxxx"],
                    "processor": {"processor_type": "", "processor_args": ""},
                    "name": "xxxxx_123",
                },
                "123_xxxx": {
                    "description": "",
                    "fields": [
                        {"origin": "", "field": "dtEventTime", "type": "string", "description": "event time"},
                        {"origin": "", "field": "_path_", "type": "string", "description": ""},
                        {"origin": "", "field": "platid", "type": "string", "description": ""},
                    ],
                    "window": {},
                    "id": "123_xxx",
                    "parents_info": [{"data_set_id": "123_xxxx", "tags": []}],
                    "parents": ["123_xxxx"],
                    "processor": {"processor_type": "", "processor_args": ""},
                    "name": "xxxxx_123",
                },
            },
            "sink": {},
        }
        assert self._job._generate_nodes() == exp

    def test_generate_job_config(self):
        self._job.job_id = "debug__123"
        DatamanageHelper.get_metric_kafka_server = mock.Mock(return_value="localhost:9092")
        self._job._generate_nodes = mock.Mock(return_value=None)
        self._job._generate_udf_info = mock.Mock(return_value=None)
        self._job.is_debug = True
        debug_exp = {
            "chain": None,
            "metric": {"metric_kafka_server": "localhost:9092", "metric_kafka_topic": "bkdata_data_monitor_metrics591"},
            "job_type": "flink",
            "udf": None,
            "job_id": "debug__123",
            "time_zone": UC_TIME_ZONE,
            "checkpoint": {
                "manager": "dummy",
                "start_position": "from_head",
                "state_checkpoints_dir": FLINK_STATE_CHECKPOINT_DIR,
                "checkpoint_redis_sentinel_name": None,
                "checkpoint_redis_sentinel_port": 0,
                "checkpoint_redis_sentinel_host": None,
                "checkpoint_redis_port": 0,
                "state_backend": "rocksdb",
                "checkpoint_redis_host": None,
                "checkpoint_redis_password": None,
                "checkpoint_interval": 600000,
            },
            "debug": {
                "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
                "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
                "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
                "debug_id": "debug",
            },
            "nodes": None,
            "job_name": "debug__123",
            "run_mode": "debug",
        }
        assert self._job.generate_job_config() == debug_exp
        self._job.is_debug = False
        product_exp = {
            "chain": None,
            "metric": {"metric_kafka_server": "localhost:9092", "metric_kafka_topic": "bkdata_data_monitor_metrics591"},
            "job_type": "flink",
            "udf": None,
            "job_id": "debug__123",
            "time_zone": UC_TIME_ZONE,
            "checkpoint": {
                "manager": "redis",
                "start_position": "continue",
                "state_checkpoints_dir": FLINK_STATE_CHECKPOINT_DIR,
                "checkpoint_redis_sentinel_name": None,
                "checkpoint_redis_sentinel_port": 0,
                "checkpoint_redis_sentinel_host": None,
                "checkpoint_redis_port": 80,
                "state_backend": "rocksdb",
                "checkpoint_redis_host": "localhost",
                "checkpoint_redis_password": "",
                "checkpoint_interval": 600000,
            },
            "debug": {
                "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
                "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
                "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
                "debug_id": None,
            },
            "nodes": None,
            "job_name": "debug__123",
            "run_mode": "product",
        }
        assert self._job.generate_job_config() == product_exp
