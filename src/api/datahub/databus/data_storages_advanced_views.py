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

from common.views import APIViewSet
from datahub.databus.exceptions import NotFoundRtError
from datahub.databus.serializers import (
    DataFlowDeleteSerializer,
    DataFlowSerializer,
    DataFlowUpdateSerializer,
)
from rest_framework.response import Response

from datahub.databus import data_storages_advanced, rt


class DataStoragesAdvancedViewset(APIViewSet):
    """
    数据开发相关的接口，包含创建、查看、列表等
    """

    # 清洗配置的名称，用于唯一确定一个清洗配置，同rt_id
    lookup_field = "cluster_type"

    def create(self, request, result_table_id):
        """
        @api {post} /v3/databus/data_storages_advanced/:result_table_id/ 新增rt存储关系
        @apiName data_flow_save
        @apiGroup data_flow
        @apiParam {string} cluster_type 存储类型
        @apiParam {string} cluster_name 存储集群
        @apiParam {string} expires 过期时间
        @apiParam {string} storage_config 存储配置。
        @apiParam {string} channel_mode(batch/stream) 管道模式
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。

        @apiParamExample {json} 参数样例:
        {
            "cluster_type": "es",
            "cluster_name":"es-test",
            "channel_mode": "batch",
            "expires": "1d",
            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "bizId_xxxxxx",
                "result_table": {
                    "result_table_name_alias": "1_xxxx",
                    "sensitivity": "private",
                    "updated_at": "2020-05-14 10:34:31",
                    "generate_type": "user",
                    "processing_type": "batch",
                    "updated_by": "",
                    "created_by": "",
                    "count_freq_unit": "H",
                    "platform": "bk_data",
                    "geog_area": "inland",
                    "storages": {
                        "ignite": {
                            "storage_channel": {},
                            "expires": "1d",
                            "description": "",
                            "data_type": "",
                            "result_table_id": "bizId_xxxxxx",
                            "created_at": "2020-05-15 22:51:16",
                            "storage_channel_id": null,
                            "updated_at": "2020-05-15 22:51:16",
                            "created_by": "xxxx",
                            "storage_cluster_config_id": 454,
                            "priority": 0,
                            "storage_config": "{\"scenario_type\": \"join\", \"max_records\": 100000, \"storage_keys\":
                            [\"timestamp\"], \"indexed_fields\": []}",
                            "updated_by": "xxxx",
                            "generate_type": "user",
                            "previous_cluster_name": null,
                            "active": true,
                            "storage_cluster": {
                                "priority": 60,
                                "connection_info": "{\"host\":\"xx.xx.xx.xx\", \"port\": 11800, \"user\": \"bkdata\",
                                 \"password\": \"xxx\", \"cluster.name\": \"ignite-test\",
                                 \"zk.domain\": \"xxx\", \"zk.port\": 2181, \"zk.root.path\":
                                 \"/ignite_test4\", \"session.timeout\": 30000, \"join.timeout\": 10000}",
                                "cluster_type": "ignite",
                                "expires":"{\"min_expire\": 1, \"list_expire\": [{\"name\": \"1\\天\", \"value\": \"1d\"}
                                , {\"name\": \"3\\天\", \"value\": \"3d\"}, {\"name\": \"7\\天\", \"value\": \"7d\"},
                                {\"name\": \"14\\天\", \"value\": \"14d\"}, {\"name\": \"21\\天\", \"value\": \"21d\"},
                                {\"name\": \"30\\天\", \"value\": \"30d\"}, {\"name\": \"60\\天\", \"value\": \"60d\"},
                                 {\"name\": \"90\\天\", \"value\": \"90d\"}, {\"name\": \"120\\天\", \"value\": \"120d\"},
                                  {\"name\": \"180\\天\", \"value\": \"180d\"}, {\"name\": \"360\\天\", \"value\":
                                  \"360d\"}], \"max_expire\": 365}",
                                "belongs_to": "bkdata",
                                "cluster_name": "ignite-test",
                                "storage_cluster_config_id": 454,
                                "version": "ignite-2.7.5",
                                "cluster_group": "default",
                                "id": 454
                            },
                            "id": 9302,
                            "physical_table_name": "mapleleaf_1.xxx_1"
                        },
                        "hdfs": {
                            "storage_channel": {},
                            "expires": "7d",
                            "description": "DataFlow增加存储",
                            "data_type": "parquet",
                            "result_table_id": "bizId_xxxxxx",
                            "created_at": "2020-05-14 10:34:44",
                            "storage_channel_id": null,
                            "updated_at": "2020-05-14 10:34:44",
                            "created_by": "xxxx",
                            "storage_cluster_config_id": 24,
                            "priority": 0,
                            "storage_config": "{}",
                            "updated_by": "xxxx",
                            "generate_type": "system",
                            "previous_cluster_name": null,
                            "active": true,
                            "storage_cluster": {
                                "priority": 95,
                                "connection_info": "{\"interval\": 60000, \"hdfs_url\": \"hdfs://hdfsTest\",
                                \"rpc_port\": 9000, \"topic_dir\": \"/kafka/data/\", \"servicerpc_port\": 53310,
                                \"ids\": \"nn1\", \"port\": 8081, \"hdfs_conf_dir\": \"/conf\",
                                 \"hosts\": \"test-master-01\", \"log_dir\": \"/kafka/logs\", \"hdfs_default_params\":
                                  {\"dfs.replication\": 2}, \"flush_size\": 1000000, \"hdfs_cluster_name\":
                                  \"hdfsTest\"}",
                                "cluster_type": "hdfs",
                                "expires": "{\"min_expire\": 1, \"max_expire\": -1, \"list_expire\": [{\"name\":
                                \"7天\", \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},{\"name\": \"30天\",
                                 \"value\": \"30d\"},{\"name\": \"60天\", \"value\": \"60d\"},{\"name\": \"永久保存\",
                                 \"value\": \"-1\"}]}",
                                "belongs_to": "bkdata",
                                "cluster_name": "hdfs-test",
                                "storage_cluster_config_id": 24,
                                "version": "2.0.0",
                                "cluster_group": "default",
                                "id": 24
                            },
                            "id": 9209,
                            "physical_table_name": "/api/flow/1/xxx_1"
                        },
                        "tredis": {
                            "storage_channel": {},
                            "expires": "1d",
                            "description": "",
                            "data_type": "",
                            "result_table_id": "bizId_xxxxxx",
                            "created_at": "2020-05-15 22:31:29",
                            "storage_channel_id": null,
                            "updated_at": "2020-05-15 22:31:29",
                            "created_by": "xxxx",
                            "storage_cluster_config_id": 31,
                            "priority": 0,
                            "storage_config": "{\"storage_key_separator\": \"_\", \"storage_type\": \"join\",
                            \"storage_expire_days\": -1, \"storage_separator\": \":\", \"storage_keys\": \"timestamp\"}"
                            ,
                            "updated_by": "xxxx",
                            "generate_type": "user",
                            "previous_cluster_name": null,
                            "active": true,
                            "storage_cluster": {
                                "priority": 0,
                                "connection_info": "{\"enable_sentinel\": false, \"name_sentinel\": \"\", \"host\":
                                \"xx.xx.xx.xx\", \"host_sentinel\": \"\", \"port_sentinel\": 0, \"password\":
                                \"xxx\", \"port\": \"30000\"}",
                                "cluster_type": "tredis",
                                "expires": "{\"min_expire\":7, \"max_expire\":-1, \"list_expire\":[ { \"name\":\"7天\",
                                \"value\":\"7d\" }, { \"name\":\"15天\", \"value\":\"15d\" }, { \"name\":\"30天\",
                                \"value\":\"30d\" }, { \"name\":\"60天\", \"value\":\"60d\" }, { \"name\":\"永久保存\",
                                \"value\":\"-1\" } ]}",
                                "belongs_to": "someone",
                                "cluster_name": "tredis-test",
                                "storage_cluster_config_id": 31,
                                "version": "2.0.0",
                                "cluster_group": "default",
                                "id": 31
                            },
                            "id": 9298,
                            "physical_table_name": "bizId_xxxxxx"
                        },
                        "pulsar": {
                            "storage_channel": {
                                "priority": 1,
                                "description": "",
                                "cluster_backup_ips": "",
                                "cluster_domain": "xx.xx.xx.xx",
                                "attribute": "bkdata",
                                "cluster_port": 6650,
                                "cluster_role": "inner",
                                "zk_port": 2181,
                                "zk_domain": "",
                                "cluster_name": "test_pulsar_inner",
                                "cluster_type": "pulsar",
                                "active": true,
                                "channel_cluster_config_id": 42,
                                "id": 42,
                                "zk_root_path": "/"
                            },
                            "expires": "3d",
                            "description": "计算表存储",
                            "data_type": "",
                            "result_table_id": "bizId_xxxxxx",
                            "created_at": "2020-05-15 20:08:15",
                            "storage_channel_id": 42,
                            "updated_at": "2020-05-15 20:08:15",
                            "created_by": "xxxx",
                            "storage_cluster_config_id": null,
                            "priority": 1,
                            "storage_config": "{}",
                            "updated_by": "xxxx",
                            "generate_type": "user",
                            "previous_cluster_name": null,
                            "active": true,
                            "storage_cluster": {},
                            "id": 9254,
                            "physical_table_name": "bizId_xxxxxx"
                        },
                        "es": {
                            "storage_channel": {},
                            "expires": "1d",
                            "description": "",
                            "data_type": "",
                            "result_table_id": "bizId_xxxxxx",
                            "created_at": "2020-05-15 21:32:00",
                            "storage_channel_id": null,
                            "updated_at": "2020-05-15 21:32:00",
                            "created_by": "xxxx",
                            "cluster_name_config_id": 342,
                            "priority": 0,
                            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": [],
                             \"has_replica\": false}",
                            "updated_by": "xxxx",
                            "generate_type": "user",
                            "previous_cluster_name": null,
                            "active": true,
                            "cluster_name": {
                                "priority": 96,
                                "connection_info": "{\"enable_auth\": true, \"es_cluster\": \"bk-test661\", \"host\":
                                \"xx.xx.xx.xx\", \"user\": \"blueking\", \"shard_docs_limit\": 10000000, \"password\":
                                 \"xxx\", \"port\": 8080, \"transport\": 9300}",
                                "cluster_type": "es",
                                "expires":"{\"min_expire\": 3, \"list_expire\": [{\"name\": \"3\\天\", \"value\": \"3d\"}
                                , {\"name\": \"7\\天\", \"value\": \"7d\"}, {\"name\": \"14\\天\", \"value\": \"14d\"},
                                {\"name\": \"30\\天\", \"value\": \"30d\"}, {\"name\": \"60\\天\", \"value\": \"60d\"},
                                {\"name\": \"90\\天\", \"value\": \"90d\"}, {\"name\": \"120\\天\", \"value\": \"120d\"},
                                 {\"name\": \"180\\天\", \"value\": \"180d\"}], \"max_expire\": 200}",
                                "belongs_to": "bkdata-eee",
                                "cluster_name": "es-662",
                                "cluster_name_config_id": 342,
                                "version": "6.6.2",
                                "cluster_group": "default",
                                "id": 342
                            },
                            "id": 9281,
                            "physical_table_name": "bizId_xxxxxx"
                        }
                    },
                    "project_id": 12946,
                    "result_table_id": "bizId_xxxxxx",
                    "project_name": "xxxx-test",
                    "count_freq": 1,
                    "description": "1_xxx",
                    "tags": {
                        "manage": {
                            "geog_area": [
                                {
                                    "alias": "中国内地",
                                    "code": "inland"
                                }
                            ]
                        }
                    },
                    "bk_biz_id": 1,
                    "fields": [
                        {
                            "field_type": "string",
                            "field_alias": "1",
                            "description": null,
                            "roles": {
                                "event_time": false
                            },
                            "created_at": "2020-05-14 10:34:31",
                            "is_dimension": false,
                            "created_by": "xxxx",
                            "updated_at": "2020-05-14 10:34:31",
                            "origins": null,
                            "field_name": "datetime",
                            "id": 75302,
                            "field_index": 0,
                            "updated_by": ""
                        },
                        {
                            "field_type": "string",
                            "field_alias": "1",
                            "description": null,
                            "roles": {
                                "event_time": false
                            },
                            "created_at": "2020-05-14 10:34:31",
                            "is_dimension": false,
                            "created_by": "xxxx",
                            "updated_at": "2020-05-14 10:34:31",
                            "origins": null,
                            "field_name": "data",
                            "id": 75303,
                            "field_index": 1,
                            "updated_by": ""
                        }
                    ],
                    "created_at": "2020-05-14 10:34:31",
                    "result_table_type": null,
                    "result_table_name": "xxx",
                    "data_category": "UTF8",
                    "is_managed": 1
                }
            },
            "result": true
        }

        """
        params = self.params_valid(serializer=DataFlowSerializer)
        # 根据rt_id查询rt_info 并且校验rt 关联的存储是否重复
        rt_info = rt.get_rt_fields_storages(result_table_id)
        if not rt_info:
            raise NotFoundRtError()
        params["bk_biz_id"] = rt_info["bk_biz_id"]
        result = data_storages_advanced.create_flow(params, rt_info)
        return Response(result)

    def destroy(self, request, result_table_id, cluster_type):
        """
        @api {delete} /v3/databus/data_storages_advanced/:result_table_id/:cluster_type/ 删除flow
        @apiName data_flow_delete
        @apiGroup data_flow
        @apiParam {string} channel_mode(batch/stream) 管道模式
        @apiDescription 删除flow入库, 包含rt,存储关联关系, 当前不删除数据
        @apiParamExample {json} 参数样例:
        {
            "data_quality":"False",
            "channel_mode":"batch"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        # 参数校验
        params = self.params_valid(serializer=DataFlowDeleteSerializer)
        rt_info = rt.get_rt_fields_storages(result_table_id)
        params["bk_biz_id"] = rt_info["bk_biz_id"]
        params["cluster_type"] = cluster_type
        if not rt_info:
            raise NotFoundRtError()
        data_storages_advanced.delete_flow(params, result_table_id, rt_info)
        return Response(True)

    def update(self, request, result_table_id, cluster_type):
        """
        @api {put} v3/storekit/data_storages_advanced/:result_table_id/:cluster_type/ 更新rt存储关系
        @apiName data_flow_update
        @apiGroup data_flow
        @apiParam {string} cluster_name 存储集群
        @apiParam {string} expires 过期时间
        @apiParam {string} storage_config 存储配置。
        @apiParam {string} channel_mode(batch/stream) 管道模式
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。

        @apiParamExample {json} 参数样例:
        {
            "cluster_name":"es-test",
            "channel_mode": "batch",
            "expires": "1d",
            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}"
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "",
            "result": true
        }

        """
        # 参数校验
        params = self.params_valid(serializer=DataFlowUpdateSerializer)
        # 根据rt_id查询rt_info 并且校验rt 关联的存储是否重复
        rt_info = rt.get_rt_fields_storages(result_table_id)
        params["bk_biz_id"] = rt_info["bk_biz_id"]
        params["cluster_type"] = cluster_type
        if not rt_info:
            raise NotFoundRtError()
        result = data_storages_advanced.update_flow(params, rt_info, result_table_id)
        return Response(result)
