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

from rest_framework.response import Response

from common.decorators import detail_route, list_route
from common.views import APIViewSet
from datahub.common.const import (
    CLUSTER_NAME,
    ES,
    FALSE,
    INDICES,
    LIMIT,
    RESULT_TABLE_ID,
    STORAGES,
    TRUE,
    TYPE,
)
from datahub.storekit import es, util
from datahub.storekit.exceptions import RtStorageNotExistsError
from datahub.storekit.serializers import (
    CreateTableSerializer,
    EsIndicesDelSerializer,
    EsIndicesSerializer,
    EsRouteSerializer,
)
from datahub.storekit.settings import INITIAL_SHARD_NUM


class EsSet(APIViewSet):

    # 结果表ID
    lookup_field = RESULT_TABLE_ID

    def create(self, request):
        """
        @api {post} v3/storekit/es/ 初始化es存储
        @apiGroup ES
        @apiDescription 创建rt的一些元信息，关联对应的es存储
        @apiParam {string{小于128字符}} result_table_id 结果表名称。
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "591_test_rt"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        params = self.params_valid(serializer=CreateTableSerializer)
        rt = params[RESULT_TABLE_ID]
        rt_info = util.get_rt_info(rt)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.initialize(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt, TYPE: ES})

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/es/:result_table_id/ 获取rt的es信息
        @apiGroup ES
        @apiDescription 获取es存储上rt的一些元信息，包含索引列表、别名列表等
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {
                "active": true,
                "created_at": "2019-06-06 17:03:19",
                "created_by": "",
                "data_type": "",
                "description": "",
                "es_indices": [
                    "591_test_bk_language_2019062711",
                    "591_test_bk_language_2019062710",
                    "591_test_bk_language_2019062709"
                ],
                "es_mappings": {
                    "test_bk_language": {
                        "dynamic_templates": [
                            {
                                "strings_as_keywords": {
                                    "mapping": {
                                        "norms": "false",
                                        "type": "keyword"
                                    },
                                    "match_mapping_type": "string"
                                }
                            }
                        ],
                        "properties": {
                            "_copy": {
                                "type": "text"
                            },
                            "_iteration_idx": {
                                "type": "long"
                            },
                            "dtEventTime": {
                                "doc_values": false,
                                "format": "yyyy-MM-dd HH:mm:ss",
                                "type": "date"
                            },
                            "dtEventTimeStamp": {
                                "format": "epoch_millis",
                                "type": "date"
                            },
                            "gseindex": {
                                "type": "long"
                            },
                            "ip": {
                                "type": "keyword"
                            },
                            "log": {
                                "copy_to": [
                                    "_copy"
                                ],
                                "type": "text"
                            },
                            "path": {
                                "doc_values": false,
                                "type": "keyword"
                            },
                            "report_time": {
                                "doc_values": false,
                                "type": "keyword"
                            }
                        }
                    }
                },
                "es_settings": {
                    "1_xxx": {
                        "settings": {
                            "index": {
                                "blocks": {
                                    "read_only_allow_delete": "true"
                                },
                                "creation_date": "1561638924066",
                                "number_of_replicas": "0",
                                "number_of_shards": "30",
                                "provided_name": "1_xxx",
                                "routing": {
                                    "allocation": {
                                        "include": {
                                            "tag": "hot"
                                        }
                                    }
                                },
                                "uuid": "xxx",
                                "version": {
                                    "created": "6060199"
                                }
                            }
                        }
                    }
                },
                "expires": "3d",
                "generate_type": "user",
                "id": 5334,
                "physical_table_name": "1_xxx",
                "priority": 0,
                "result_table_id": "1_xxx",
                "storage_channel": {},
                "storage_channel_id": null,
                "storage_cluster": {
                    "belongs_to": "admin",
                    "cluster_group": "default",
                    "cluster_name": "es-test",
                    "cluster_type": "es",
                    "connection_info": "{\"enable_auth\": true, \"host\": \"xx.xx.xx.xx\", \"user\": \"xxx\",
                                        \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                    "id": 342,
                    "priority": 0,
                    "storage_cluster_config_id": 342,
                    "version": "6.6.2"
                },
                "storage_cluster_config_id": 342,
                "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [\"log\"], \"doc_values_fields\": []}",
                "updated_at": "2019-06-27 20:35:20",
                "updated_by": "xxx"
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    def update(self, request, result_table_id):
        """
        @api {put} v3/storekit/es/:result_table_id/ 更新rt的es存储
        @apiGroup ES
        @apiDescription 变更rt的一些元信息，比如修改过期时间、修改schema等
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.alter(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    def destroy(self, request, result_table_id):
        """
        @api {delete} v3/storekit/es/:result_table_id/ 删除rt的es存储
        @apiGroup ES
        @apiDescription 删除rt对应es存储上的数据，以及已关联的元数据
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.delete(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    @detail_route(methods=["get"], url_path="prepare")
    def prepare(self, request, result_table_id):
        """
        @api {get} v3/storekit/es/:result_table_id/prepare/ 准备rt的es存储
        @apiGroup ES
        @apiDescription 准备rt关联的es存储，例如元信息、数据目录等
        @apiParam {string} force_create 是否强制创建新索引，可选参数
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        force_create = request.query_params.get("force_create", FALSE)
        # url中如果传入参数shard_num，则取配置值，否则取默认值，shard_num仅在force_create=true时生效
        shard_num = request.query_params.get("shard_num", INITIAL_SHARD_NUM)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.prepare(rt_info, force_create.lower() == TRUE, shard_num)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    @detail_route(methods=["get"], url_path="check_schema")
    def check_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/es/:result_table_id/check_schema/ 对比rt和es的schema
        @apiGroup ES
        @apiDescription 对比rt的schema和存储的schema，找出不兼容的字段
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {
                "es_conf": {
                    "analyzed_fields": [
                        "log"
                    ],
                    "date_fields": [
                        "dtEventTimeStamp",
                        "dtEventTime"
                    ],
                    "doc_values_fields": [
                        "dtEventTimeStamp",
                        "gseindex",
                        "ip",
                        "_iteration_idx"
                    ],
                    "json_fields": []
                },
                "es_fields": {
                    "_copy": "text",
                    "_iteration_idx": "long",
                    "dtEventTime": "date",
                    "dtEventTimeStamp": "date",
                    "gseindex": "long",
                    "ip": "keyword",
                    "log": "text",
                    "path": "keyword",
                    "report_time": "keyword"
                },
                "rt_conf": {
                    "analyzed_fields": [
                        "log"
                    ],
                    "date_fields": [
                        "dtEventTimeStamp",
                        "dtEventTime"
                    ],
                    "doc_values_fields": [
                        "dtEventTimeStamp",
                        "_iteration_idx",
                        "gseindex",
                        "ip"
                    ],
                    "json_fields": []
                },
                "rt_fields": {
                    "gseindex": "long",
                    "ip": "string",
                    "log": "text",
                    "path": "string",
                    "report_time": "string",
                    "timestamp": "timestamp"
                }
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.check_schema(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {get} v3/storekit/es/:result_table_id/maintain/ 维护rt的es存储
        @apiGroup ES
        @apiDescription 维护rt在es存储的数据，以及一些元信息
        @apiError (错误码) 1578104 result_table_id未关联存储es。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and ES in rt_info[STORAGES]:
            result = es.maintain(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: ES})

    @list_route(methods=["get"], url_path="maintain_all")
    def maintain_all(self, request):
        """
        @api {get} v3/storekit/es/maintain_all/ 维护所有rt的es存储
        @apiGroup ES
        @apiDescription 维护es存储，清理过期数据，创建别名等
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        result = es.maintain_all_rts()
        return Response(result)

    @list_route(methods=["get"], url_path="clusters")
    def clusters(self, request):
        """
        @api {get} v3/storekit/es/clusters/ es存储集群列表
        @apiGroup ES
        @apiDescription 获取es存储集群列表
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [
                {
                    "belongs_to": "admin",
                    "cluster_group": "default",
                    "cluster_name": "es-test",
                    "cluster_type": "es",
                    "connection": {
                        "enable_auth": true,
                        "host": "xx.xx.xx.xx",
                        "password": "xxx",
                        "port": 8080,
                        "transport": 9300,
                        "user": "xxx"
                    },
                    "connection_info": "{\"enable_auth\": true, \"host\": \"xx.xx.xx.xx\", \"user\": \"xxx\",
                                        \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                    "created_at": "2019-04-10 10:29:48",
                    "created_by": "",
                    "description": "es-test\\u96c6\\u7fa4",
                    "id": 342,
                    "priority": 0,
                    "updated_at": "2019-06-17 16:32:03",
                    "updated_by": "xx",
                    "version": "6.6.2"
                },
                {
                    "belongs_to": "admin",
                    "cluster_group": "default",
                    "cluster_name": "es-test",
                    "cluster_type": "es",
                    "connection": {
                        "enable_auth": true,
                        "host": "xx.x.x.x",
                        "password": "xxx",
                        "port": 8080,
                        "transport": 9300,
                        "user": "xxx"
                    },
                    "connection_info": "{\"enable_auth\": true, \"host\": \"xx.x.x.x\", \"user\": \"xxx\",
                                        \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                    "created_at": "2019-04-15 21:22:13",
                    "created_by": "",
                    "description": "es\\u96c6\\u7fa4\\uff08\\u4e0d\\u5efa\\u8bae\\u4f7f\\u7528\\uff09",
                    "id": 350,
                    "priority": 0,
                    "updated_at": "2019-06-17 16:32:53",
                    "updated_by": "xx",
                    "version": "1.0.0"
                }
            ],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        result = es.clusters()
        return Response(result)

    @list_route(methods=["get"], url_path="route")
    def route(self, request):
        """
        @api {get} v3/storekit/es/route/ es rest服务路由, 只限制get请求
        @apiGroup ES
        @apiDescription es rest服务路由
        @apiParam {string} cluster_name  集群名称
        @apiParam {string} uri  相对请求地址
        @apiParam {string} bk_username  操作用户
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": "",
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=EsRouteSerializer)
        result = es.route_es_request(params["uri"], params[CLUSTER_NAME])
        return Response(result)

    @list_route(methods=["get"], url_path="indices")
    def indices(self, request):
        """
        @api {get} v3/storekit/es/indices/ es 索引列表
        @apiGroup ES
        @apiDescription  es 索引列表
        @apiParam {string} cluster_name  集群名称
        @apiParam {string} limit  返回数据最大的top
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": "",
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=EsIndicesSerializer)
        result = es.cat_indices(params[CLUSTER_NAME], params[LIMIT])
        return Response(result)

    @list_route(methods=["get"], url_path="delete_indices")
    def delete_indices(self, request):
        """
        @api {get} v3/storekit/es/delete_indices/ es 删除索引列表
        @apiGroup ES
        @apiDescription es 删除索引列表，一般不会有太多需要删除的index，使用get方法即可
        @apiParam {string} cluster_name  集群名称
        @apiParam {string} indices  索引列表，用逗号,隔开
        @apiParam {string} bk_username  操作用户
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": "",
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=EsIndicesDelSerializer)
        result = es.del_indices(params[CLUSTER_NAME], params[INDICES])
        return Response(result)
