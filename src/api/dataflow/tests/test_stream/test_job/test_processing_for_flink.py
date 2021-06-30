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
from common.api.base import DataResponse
from rest_framework.test import APITestCase

from dataflow.component.config.hdfs_config import ConfigHDFS
from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.api.api_helper import BksqlHelper, MetaApiHelper
from dataflow.stream.handlers import processing_job_info, processing_stream_info
from dataflow.stream.handlers.db_helper import RedisConnection
from dataflow.stream.models import ProcessingJobInfo, ProcessingStreamInfo
from dataflow.stream.processing.processing_for_flink import (
    add_result_table_extra_info,
    create_flink_processing,
    delete_processing_redis_checkpoint,
    update_flink_processings,
)
from dataflow.stream.utils import FtpServer


class TestProcessingForFlink(APITestCase):
    def test_add_result_table_extra_info(self):
        parsed_table = {
            "processor": {
                "processor_type": "static_join_transform",
                "processor_args": '{"storage_info": {"data_id": "123_test_static_parents"}}',
            }
        }
        parsed_table_exp = {
            "static_parents": ["123_test_static_parents"],
            "processor": {
                "processor_type": "static_join_transform",
                "processor_args": '{"storage_info": {"data_id": "123_test_static_parents"}}',
            },
        }
        assert add_result_table_extra_info(parsed_table) == parsed_table_exp

    def test_create_flink_processing(self):
        args = {
            "tags": "xxx",
            "static_data": [],
            "source_data": [],
            "input_result_tables": [
                "xxx",
            ],
            "sql": "select _path_, platid from xxx",
            "processing_id": "xxx",
            "outputs": [{"bk_biz_id": 123, "table_name": "abc"}],
            "dict": {
                "window_type": "none",
                "description": "xxx",
                "count_freq": 0,
                "waiting_time": 0,
                "window_time": 0,
                "session_gap": 0,
            },
            "project_id": 99999,
            "component_type": "flink",
        }
        MetaApiHelper.is_result_table_exist = mock.Mock(return_value=False)
        MetaApiHelper.get_result_table = mock.Mock(
            return_value={"processing_type": "clean", "fields": [{"field_name": "_path_"}, {"field_name": "platid"}]}
        )
        MetaApiHelper.set_data_processing = mock.Mock(return_value=None)
        TagHelper.get_geog_area_code_by_project = mock.Mock(return_value="default")
        ConfigHDFS.get_all_cluster = mock.Mock(return_value="/app/udf/a/v1/a.jar")
        StorekitHelper.get_default_storage = mock.Mock(return_value={"cluster_group": "default"})
        FtpServer.get_ftp_server_cluster_group = mock.Mock(return_value="default")
        processing_udf_info.delete = mock.Mock(return_value=None)
        processing_udf_info.save = mock.Mock(return_value=None)
        BksqlHelper.list_result_tables_by_sql_parser = mock.Mock(
            return_value=[
                {
                    "parents": ["xxx"],
                    "name": "xxx",
                    "id": "xxx",
                    "window": {"count_freq": 0},
                    "fields": [
                        {"field": "_path_", "type": "String", "origin": "", "description": ""},
                        {"field": "platid", "type": "String", "origin": "", "description": ""},
                    ],
                    "processor": {"processor_type": "", "processor_args": ""},
                }
            ]
        )
        BksqlHelper.convert_dimension = mock.Mock(
            return_value=[{"is_dimension": True, "field_name": ["_path_", "platid"]}]
        )
        assert create_flink_processing(args) == {
            "tails": ["xxx"],
            "heads": ["xxx"],
            "result_table_ids": ["xxx"],
            "processing_id": "xxx",
        }

    @mock.patch.object(RedisConnection, "delete")
    @mock.patch.object(processing_job_info, "get")
    @mock.patch.object(processing_stream_info, "get")
    def test_delete_processing_checkpoint(
        self, processing_stream_info_get, processing_job_info_get, redis_client_delete
    ):
        TagHelper.get_geog_area_code_by_cluster_group = mock.Mock(return_value="default")
        processing_stream_info_get.return_value = ProcessingStreamInfo(stream_id="xxx", checkpoint_type="offset")
        processing_job_info_get.return_value = ProcessingJobInfo(job_config='{"heads":"xxx","tails":"yyy"}')
        MetaApi.result_tables.storages = mock.Mock(
            return_value=DataResponse({"data": {"kafka": {"physical_table_name": "physical_rt_name"}}})
        )
        redis_client_delete.return_value = mock.Mock(return_value=None)
        delete_processing_redis_checkpoint(processing_id="xxx")

    @mock.patch.object(processing_stream_info, "get")
    def test_update_flink_processing(self, processing_stream_info_get):
        args = {
            "tags": "xxx",
            "static_data": [],
            "source_data": [],
            "input_result_tables": [
                "xxx",
            ],
            "sql": "select _path_, platid from xxx",
            "processing_id": "xxx",
            "outputs": [{"bk_biz_id": 123, "table_name": "abc"}],
            "dict": {
                "window_type": "none",
                "description": "xxx",
                "count_freq": 0,
                "waiting_time": 0,
                "window_time": 0,
                "session_gap": 0,
            },
            "project_id": 99999,
            "component_type": "flink",
        }
        MetaApiHelper.is_result_table_exist = mock.Mock(return_value=False)
        MetaApiHelper.get_result_table = mock.Mock(
            return_value={"processing_type": "clean", "fields": [{"field_name": "_path_"}, {"field_name": "platid"}]}
        )
        MetaApiHelper.set_data_processing = mock.Mock(return_value=None)
        TagHelper.get_geog_area_code_by_project = mock.Mock(return_value="default")
        ConfigHDFS.get_all_cluster = mock.Mock(return_value="/app/udf/a/v1/a.jar")
        StorekitHelper.get_default_storage = mock.Mock(return_value={"cluster_group": "default"})
        FtpServer.get_ftp_server_cluster_group = mock.Mock(return_value="default")
        processing_udf_info.delete = mock.Mock(return_value=None)
        processing_udf_info.save = mock.Mock(return_value=None)
        BksqlHelper.list_result_tables_by_sql_parser = mock.Mock(
            return_value=[
                {
                    "parents": ["xxx"],
                    "name": "xxx",
                    "id": "xxx",
                    "window": {"count_freq": 0},
                    "fields": [
                        {"field": "_path_", "type": "String", "origin": "", "description": ""},
                        {"field": "platid", "type": "String", "origin": "", "description": ""},
                    ],
                    "processor": {"processor_type": "", "processor_args": ""},
                }
            ]
        )
        BksqlHelper.convert_dimension = mock.Mock(
            return_value=[{"is_dimension": True, "field_name": ["_path_", "platid"]}]
        )
        MetaApiHelper.get_data_processing = mock.Mock(return_value={"inputs": []})
        MetaApiHelper.update_data_processing = mock.Mock(return_value=None)
        processing_stream_info_get.return_value = ProcessingStreamInfo(stream_id="xxx", checkpoint_type="offset")
        assert update_flink_processings(args, "xxx") == {
            "tails": ["xxx"],
            "heads": ["xxx"],
            "result_table_ids": ["xxx"],
            "processing_id": "xxx",
        }
