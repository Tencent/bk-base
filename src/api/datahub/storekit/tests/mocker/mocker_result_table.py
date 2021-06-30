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


# mock接口 /v3/meta/result_tables/:result_table_id/?related=fields&related=storages的返回结果
result_table = {
    # 结果表ID
    "result_table_id": "591_anonymous_1217_02XX",
    # 业务ID
    "bk_biz_id": 591,
    # 项目ID
    "project_id": 198,
    # 结果表名称
    "result_table_name": "anonymous_1217_02",
    "processing_type": "stream",
    "result_table_type": None,
    "sensitivity": "public",
    "result_table_name_alias": "测试 591_anonymous_1217_02",
    "generate_type": "user",
    "created_by": "anonymous",
    "project_name": "storage_test",
    "count_freq": 0,
    "description": "测试",
    "created_at": "2018-12-23 16:45:46",
    "storages": {
        "druid": {
            "active": 0,
            "created_at": "2018-12-23 18:09:14",
            "created_by": "",
            "description": "存储测试",
            "expires": "7d",
            "generate_type": "user",
            "id": 889,
            "physical_table_name": "591_anonymous_1217_02",
            "priority": 0,
            "storage_channel": {},
            "storage_cluster": {
                "belongs_to": "someone",
                "cluster_domain": "localhost",
                "cluster_group": "default",
                "cluster_name": "druid-test",
                "cluster_type": "druid",
                "connection_info": '{"coordinator_port": 8081, "zookeeper.connect": "xx.xx.xx.xx:2181", '
                '"overlord_host": "xx.xx.xx.xx", "host": "xx.xx.xx.xx", "overlord_port": 8090, '
                '"port": 8082, "coordinator_host": "xx.xx.xx.xx"}',
                "meanings": [],
                "priority": 0,
                "storage_cluster_config_id": 32,
                "version": "2.0.0",
            },
            "storage_config": "{}",
            "updated_at": "2018-12-23 21:56:31",
            "updated_by": "",
        },
        "es": {
            "active": 0,
            "created_at": "2018-12-23 16:49:36",
            "created_by": "",
            "description": "存储测试",
            "expires": "7d",
            "generate_type": "user",
            "id": 880,
            "physical_table_name": "591_anonymous_1217_02",
            "priority": 0,
            "storage_channel": {},
            "storage_cluster": {
                "belongs_to": "bkdata",
                "cluster_domain": "localhost",
                "cluster_group": "default",
                "cluster_name": "es-test",
                "cluster_type": "es",
                "connection_info": '{"enable_auth": true, "host": "xx.xx.xx.xx", "user": "user", '
                '"password": "pwd\\n", "port": 8080, "transport": 9300}',
                "meanings": [],
                "priority": 0,
                "storage_cluster_config_id": 19,
                "version": "2.0.0",
            },
            "storage_config": '{"analyzedFields": ["ip", "report_time"], "storage_expire_days": 7, '
            '"dateFields": ["dtEventTime", "dtEventTimeStamp", "localTime", "thedate"]}',
            "updated_at": "2018-12-23 21:56:26",
            "updated_by": "",
        },
        "hdfs": {
            "active": 0,
            "created_at": "2018-12-23 15:51:28",
            "created_by": "",
            "description": "",
            "expires": "7",
            "generate_type": "user",
            "id": 947,
            "physical_table_name": "/kafka/data/591/anonymous_1217_02_591",
            "priority": 0,
            "storage_channel": {},
            "storage_cluster": {
                "belongs_to": "bkdata",
                "cluster_domain": "localhost",
                "cluster_group": "default",
                "cluster_name": "hdfs-test",
                "cluster_type": "hdfs",
                "connection_info": '{"hdfs_url": "hdfs://hdfsTest", "interval": 60000, '
                '"hdfs_conf_dir": "/data/databus/conf", "flush_size": 1000000, '
                '"log_dir": "/kafka/logs", "topic_dir": "/kafka/data/"}',
                "meanings": [],
                "priority": 0,
                "storage_cluster_config_id": 24,
                "version": "1.0.0",
            },
            "storage_config": '{"storage_expire_days": 30, "indexed_fields":"field_1_anonymous"}',
            "updated_at": "2018-12-23 15:51:28",
            "updated_by": "",
        },
        "kafka": {
            "active": 0,
            "created_at": "2018-12-23 16:49:33",
            "created_by": "",
            "description": "存储测试",
            "expires": "3d",
            "generate_type": "system",
            "id": 879,
            "physical_table_name": "table_591_anonymous_1217_02",
            "priority": 5,
            "storage_channel": {
                "active": True,
                "attribute": "bkdata",
                "channel_cluster_config_id": 10,
                "cluster_backup_ips": "",
                "cluster_domain": "xx.xx.xx.xx",
                "cluster_name": "testinner",
                "cluster_port": 9092,
                "cluster_role": "inner",
                "cluster_type": "kafka",
                "description": "",
                "id": 10,
                "meanings": [],
                "priority": 5,
                "zk_domain": "xx.xx.xx.xx",
                "zk_port": 2181,
                "zk_root_path": "/kafka-test-3",
            },
            "storage_cluster": {},
            "storage_config": "{}",
            "updated_at": "2018-12-23 16:49:33",
            "updated_by": "",
        },
        "mysql": {
            "active": 0,
            "created_at": "2018-12-23 17:49:21",
            "created_by": "",
            "description": "存储测试",
            "expires": "7d",
            "generate_type": "user",
            "id": 885,
            "physical_table_name": "mapleleaf_591.anonymous_1217_02_591",
            "priority": 0,
            "storage_channel": {},
            "storage_cluster": {
                "belongs_to": "anonymous",
                "cluster_domain": "localhost",
                "cluster_group": "default",
                "cluster_name": "mysql-test",
                "cluster_type": "mysql",
                "connection_info": '{"host": "xx.xx.xx.xx", "password": "pwd\\n", "port": 3306, "user": "xxx"}',
                "meanings": [],
                "priority": 0,
                "storage_cluster_config_id": 25,
                "version": "2.0.0",
            },
            "storage_config": "{}",
            "updated_at": "2018-12-23 21:56:28",
            "updated_by": "",
        },
        "queue": {
            "active": 0,
            "created_at": "2018-12-23 20:14:46",
            "created_by": "",
            "description": "存储测试",
            "expires": "7d",
            "generate_type": "user",
            "id": 893,
            "physical_table_name": "queue_591_anonymous_1217_02",
            "priority": 0,
            "storage_channel": {},
            "storage_cluster": {
                "belongs_to": "anonymous",
                "cluster_domain": "xxx",
                "cluster_group": "default",
                "cluster_name": "queue-test",
                "cluster_type": "queue",
                "connection_info": '{"host": "xxxx", "port": 9092}',
                "meanings": [],
                "priority": 0,
                "storage_cluster_config_id": 39,
                "version": "0.10.2",
            },
            "storage_config": "{}",
            "updated_at": "2018-12-23 21:56:41",
            "updated_by": "",
        },
    },
    "fields": [
        {
            "field_index": 0,
            "field_name": "timestamp",
            "field_type": "timestamp",
            "field_alias": "内部时间字段",
            "is_dimension": False,
            "description": "timestamp description",
            "created_at": "2018-12-23 16:45:46",
            "created_by": "anonymous",
            "updated_at": "2018-12-23 16:45:46",
            "origins": "",
            "id": 3470,
            "updated_by": "",
        },
        {
            "field_index": 1,
            "field_name": "field1",
            "field_type": "string",
            "description": "field1 description",
            "created_at": "2018-12-23 16:45:46",
            "is_dimension": True,
            "created_by": "anonymous",
            "updated_at": "2018-12-23 16:45:46",
            "origins": "",
            "field_alias": "field1 field_alias",
            "id": 3471,
            "updated_by": "",
        },
        {
            "field_index": 2,
            "field_name": "field2",
            "field_type": "int",
            "description": "field2 description",
            "created_at": "2018-12-23 16:45:46",
            "is_dimension": False,
            "created_by": "anonymous",
            "updated_at": "2018-12-23 16:45:46",
            "origins": "",
            "field_alias": "field2 field_alias",
            "id": 3472,
            "updated_by": "",
        },
    ],
}
