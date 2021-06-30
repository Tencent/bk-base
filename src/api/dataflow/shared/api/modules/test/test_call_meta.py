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

from .base import BaseTest


class TestMeta(BaseTest):
    def result_tables(self):
        data = {
            "is_managed": True,
            "hdfs": "xxxx",
            "sensitivity": "private",
            "storages": {
                "ignite": {
                    "id": 2,
                    "storage_cluster": {
                        "storage_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "tspider",
                        "cluster_domain": "xxx",
                        "cluster_group": "xxx",
                        "connection_info": "{}",
                        "priority": 234,
                        "version": "23423",
                        "belongs_to": "bkdata",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
                "tredis": {
                    "id": 2,
                    "storage_cluster": {
                        "storage_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "tspider",
                        "cluster_domain": "xxx",
                        "cluster_group": "xxx",
                        "connection_info": "{}",
                        "priority": 234,
                        "version": "23423",
                        "belongs_to": "bkdata",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
                "mysql": {
                    "id": 2,
                    "storage_cluster": {
                        "storage_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "tspider",
                        "cluster_domain": "xxx",
                        "cluster_group": "xxx",
                        "connection_info": "{}",
                        "priority": 234,
                        "version": "23423",
                        "belongs_to": "bkdata",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
                "hdfs": {
                    "id": 2,
                    "storage_cluster": {
                        "storage_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "hdfs",
                        "cluster_domain": "xxx",
                        "cluster_group": "xxx",
                        "connection_info": "{}",
                        "priority": 234,
                        "version": "23423",
                        "belongs_to": "bkdata",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
                "kafka": {
                    "id": 1,
                    "storage_cluster": {},
                    "storage_channel": {
                        "channel_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "kafka",
                        "cluster_role": "inner",
                        "cluster_domain": "xxx",
                        "cluster_backup_ips": "xxx",
                        "cluster_port": 2432,
                        "zk_domain": "127.0.0.1",
                        "zk_port": 3481,
                        "zk_root_path": "/abc/defg",
                        "priority": 234,
                        "attribute": "bkdata",
                        "description": "sdfdsf",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
                "tdw": {
                    "id": 1,
                    "storage_cluster": {},
                    "storage_channel": {
                        "channel_cluster_config_id": 1,
                        "cluster_name": "xxx",
                        "cluster_type": "kafka",
                        "cluster_role": "inner",
                        "cluster_domain": "xxx",
                        "cluster_backup_ips": "xxx",
                        "cluster_port": 2432,
                        "zk_domain": "127.0.0.1",
                        "zk_port": 3481,
                        "zk_root_path": "/abc/defg",
                        "priority": 234,
                        "attribute": "bkdata",
                        "description": "sdfdsf",
                    },
                    "physical_table_name": "xxx",
                    "expires": "3d",
                    "storage_config": "xxx",
                    "priority": 1,
                    "generate_type": "user",
                    "description": "xxx",
                    "created_by": None,
                    "created_at": None,
                    "updated_by": None,
                    "updated_at": None,
                },
            },
            "fields": [],
            "table_name": "table_name_01",
            "result_table_name": "table_name_01",
            "result_table_name_alias": "table_name_01",
            "bk_biz_id": 591,
            "processing_type": "stream",
            "extra": {"tdw": {"associated_lz_id": {"import": "xxx"}}},
            "tags": {"manage": {"geog_area": [{"alias": "xxx", "code": "inland"}]}},
        }
        return self.success_response(data)

    def storages(self):
        data = {"storages": {"mysql": ""}}
        return self.success_response(data)

    def fields(self):
        data = [
            {
                "field_type": "timestamp",
                "field_alias": "内部时间字段",
                "description": None,
                "origins": None,
                "created_at": "2018-11-06 20:40:05",
                "updated_at": "2018-11-06 20:40:05",
                "created_by": "xx",
                "is_dimension": False,
                "field_name": "timestamp",
                "id": 1163,
                "field_index": 0,
                "updated_by": "",
            },
            {
                "field_type": "string",
                "field_alias": "ip",
                "description": None,
                "origins": None,
                "created_at": "2018-11-06 20:40:05",
                "updated_at": "2018-11-06 20:40:05",
                "created_by": "xx",
                "is_dimension": False,
                "field_name": "ip",
                "id": 1158,
                "field_index": 1,
                "updated_by": "",
            },
        ]
        return self.success_response(data)

    def retrieve_projects(self):
        data = {
            "project_name": "default_project_name",
            "tdw_app_groups": ["test"],
            "tags": {"manage": {"geog_area": [{"alias": "xxx", "code": "inland"}]}},
        }
        return self.success_response(data)

    def meta_transaction(self):
        data = ["591_hahaha", "591_xxx", "591_new_processing"]
        return self.success_response(data)

    def data_transferrings(self):
        data = {
            "project_id": 1,
            "project_name": "测试项目",
            "transferring_id": "132_xxx",
            "transferring_alias": "数据传输1",
            "transferring_type": "shipper",
            "generate_type": "user",
            "created_by ": "xxx",
            "created_at": "xxx",
            "updated_by": "xxx",
            "updated_at": "xxx",
            "description": "xxx",
            "inputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": "639_battle_info",
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": 1,
                    "storage_type": "channel",
                }
            ],
            "outputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": "639_battle_info",
                    "storage_cluster_config_id": 1,
                    "channel_cluster_config_id": None,
                    "storage_type": "storage",
                }
            ],
        }
        return self.success_response(data)

    def data_processings(self):
        data = {
            "project_id": 1,
            "project_name": "测试项目",
            "processing_id": "",
            "processing_alias": "数据处理1",
            "processing_type": "clean",
            "generate_type": "user",
            "created_by ": "xxx",
            "created_at": "xxx",
            "updated_by": "xxx",
            "updated_at": "xxx",
            "description": "xxx",
            "inputs": [
                {
                    "data_set_type": "raw_data",
                    "data_set_id": 4666,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": 1,
                    "storage_type": "channel",
                }
            ],
            "outputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": "",
                    "storage_cluster_config_id": 1,
                    "channel_cluster_config_id": None,
                    "storage_type": "storage",
                }
            ],
        }
        return self.success_response(data)
