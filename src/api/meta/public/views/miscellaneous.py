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


from common.bklanguage import bktranslates
from common.decorators import params_valid
from common.views import APIViewSet
from django.db.models import Q
from rest_framework.decorators import action
from rest_framework.response import Response

from meta.basic.common import RPCViewSet
from meta.public.models.data_processing import DataProcessingRelation
from meta.public.models.data_transferring import DataTransferringRelation
from meta.public.serializers.common import DataSetUpstreamsSerializer
from meta.utils.basicapi import parseresult


class DataSetViewSet(APIViewSet):
    lookup_field = "data_set_id"

    @action(detail=True, methods=["get"], url_path="upstreams")
    @params_valid(serializer=DataSetUpstreamsSerializer)
    def get_upstreams(self, request, data_set_id, params):
        """
        @api {get} /meta/data_sets/:data_set_id/upstreams/ 获取某个data_set上游的数据处理或数据传输及该传输依赖的输入

        @apiVersion 0.2.0
        @apiGroup DataSet
        @apiName get_upstreams

        @apiParam {String} [storage_type] data_set所在存储类型
        @apiParam {Int} [storage_id] data_set所在存储集群ID

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "type": "data_processing",
                        "processing_id": "591_datamonitor_batch2",
                        "storage_type": "storage",
                        "storage_cluster_config_id": 24,
                        "channel_cluster_config_id": null,
                        "inputs": [
                            {
                                "data_set_type": "result_table",
                                "data_set_id": "591_datamonitor_batch1",
                                "storage_type": "storage",
                                "storage_cluster_config_id": 24,
                                "channel_cluster_config_id": null
                            }
                        ]
                    },
                    {
                        "type": "data_transferring",
                        "transferring_id": "kafka-591_datamonitor_batch2",
                        "storage_type": "channel",
                        "storage_cluster_config_id": null,
                        "channel_cluster_config_id": 10,
                        "inputs": [
                            {
                                "data_set_type": "result_table",
                                "data_set_id": "591_datamonitor_batch2",
                                "storage_type": "storage",
                                "storage_cluster_config_id": 24,
                                "channel_cluster_config_id": null
                            }
                        ]
                    }
                ]
            }
        """
        upstreams = []

        # 查找上游data_processing和data_transferring
        processing_query_set = DataProcessingRelation.objects.filter(data_set_id=data_set_id, data_directing="output")
        transferring_query_set = DataTransferringRelation.objects.filter(
            data_set_id=data_set_id, data_directing="output"
        )

        if "storage_type" in params:
            storage_type = params["storage_type"]
            processing_query_set = processing_query_set.filter(storage_type=storage_type)
            transferring_query_set = transferring_query_set.filter(storage_type=storage_type)
            if "storage_id" in params:
                storage_id = params["storage_id"]
                processing_query_set = processing_query_set.filter(
                    Q(storage_cluster_config_id=storage_id) | Q(channel_cluster_config_id=storage_id)
                )
                transferring_query_set = transferring_query_set.filter(
                    Q(storage_cluster_config_id=storage_id) | Q(channel_cluster_config_id=storage_id)
                )

        for relation in processing_query_set:
            processing_id = relation.processing_id
            upstream = {
                "type": "data_processing",
                "processing_id": processing_id,
                "storage_type": relation.storage_type,
                "storage_cluster_config_id": relation.storage_cluster_config_id,
                "channel_cluster_config_id": relation.channel_cluster_config_id,
                "inputs": [],
            }
            for input_item in DataProcessingRelation.objects.filter(
                processing_id=processing_id, data_directing="input"
            ):
                upstream["inputs"].append(
                    {
                        "data_set_type": input_item.data_set_type,
                        "data_set_id": input_item.data_set_id,
                        "storage_type": input_item.storage_type,
                        "storage_cluster_config_id": input_item.storage_cluster_config_id,
                        "channel_cluster_config_id": input_item.channel_cluster_config_id,
                    }
                )
            upstreams.append(upstream)

        for relation in transferring_query_set:
            transferring_id = relation.transferring_id
            upstream = {
                "type": "data_transferring",
                "transferring_id": transferring_id,
                "storage_type": relation.storage_type,
                "storage_cluster_config_id": relation.storage_cluster_config_id,
                "channel_cluster_config_id": relation.channel_cluster_config_id,
                "inputs": [],
            }
            for input_item in DataTransferringRelation.objects.filter(
                transferring_id=transferring_id, data_directing="input"
            ):
                upstream["inputs"].append(
                    {
                        "data_set_type": input_item.data_set_type,
                        "data_set_id": input_item.data_set_id,
                        "storage_type": input_item.storage_type,
                        "storage_cluster_config_id": input_item.storage_cluster_config_id,
                        "channel_cluster_config_id": input_item.channel_cluster_config_id,
                    }
                )
            upstreams.append(upstream)

        return Response(upstreams)


class LineageViewSet(APIViewSet):
    lookup_field = "lineage_id"

    def list(self, request):
        """
        @api {get} /meta/lineage/ 获取数据的血缘信息

        @apiVersion 0.2.0
        @apiGroup Lineage
        @apiName get_lineage

        @apiParam {String} type 节点类型,取值[raw_data,result_table]
        @apiParam {String} qualified_name 节点索引值,取值[raw_data为id,result_table为result_table_id]
        @apiParam {Number} [depth] 数字，默认3[可选]
        @apiParam {String} [direction] BOTH or INPUT or OUTPUT, 默认BOTH[可选]
        @apiParam {Boolean} [only_user_entity] True or False, 默认False[可选]
        @apiDescription  数据血缘接口，获取影响当前数据，以及当前数据影响的数据&处理节点。

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "direction": "BOTH",
                    "depth": 2,
                    "nodes": {
                        "result_table_1":{},
                        "result_table_2":{},
                        "result_table_3":{},
                        "result_table_4":{},
                        "data_processing_1":{},
                        "data_processing_2":{}
                    },
                    "relations": [
                        {
                        "from": "result_table_1",
                        "to": "data_processing_1"
                        },
                        {
                        "from": "data_processing_1",
                        "to": "result_table_2"
                        },
                        {
                        "from": "result_table_2",
                        "to": "data_processing_2"
                        },
                        {
                        "from": "result_table_4",
                        "to": "data_processing_2"
                        },
                        {
                        "from": "data_processing_2",
                        "to": "result_table_3"
                        }
                    ]
                }
            }
        """
        type_name = request.query_params.get("type")
        qualified_name = request.query_params.get("qualified_name")
        depth = request.query_params.get("depth")
        direction = request.query_params.get("direction")
        backend_type = request.query_params.get("backend_type")
        extra_retrieve = request.query_params.get("extra_retrieve")
        only_user_entity = request.query_params.get("only_user_entity")
        if not type_name or not qualified_name:  # type_name is empty or qualified_name is empty
            return Response({})

        # equal_field_name = None
        if type_name == "result_table":
            type_name = "ResultTable"
            # equal_field_name = 'result_table_id'
        elif type_name == "raw_data":
            type_name = "RawData"
            # equal_field_name = 'id'
        params = {"type_name": type_name, "qualified_name": qualified_name}
        if depth:
            params["depth"] = depth
        if direction:
            params["direction"] = direction
        if backend_type:
            params["backend_type"] = backend_type
        if extra_retrieve:
            params["extra_retrieve"] = extra_retrieve
        if only_user_entity:
            params["only_user_entity"] = only_user_entity
        res_lineage = parseresult.get_result_table_lineage_info(params)
        return Response(res_lineage)


class DMCategoryConfigViewSet(RPCViewSet):
    lookup_field = "id"

    def list(self, request):
        """
        @api {get} /meta/dm_category_configs/ 获取数据域信息

        @apiVersion 0.2.0
        @apiGroup DMCategoryConfig
        @apiName list_dm_category_configs

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "category_alias": "硬件配置",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": "包含设备以及系统，诸如备用电源设备、主机配置、集群配置等信息",
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 1,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "hw_config",
                        "id": 3,
                        "icon": null
                    },
                    {
                        "category_alias": "网络数据",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": "包含网络设备、网络拓扑等",
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 2,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "net",
                        "id": 4,
                        "icon": null
                    },
                    {
                        "category_alias": "基础性能",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": "主机性能数据的采集，主要是CPU、磁盘、内存、负载、网络等数据信息",
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 3,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "performance",
                        "id": 5,
                        "icon": null
                    },
                    {
                        "category_alias": "模块组件",
                        "sub_list": [
                            {
                                "category_alias": "基础组件",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "行业通用类组件",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 1,
                                "parent_id": 6,
                                "active": true,
                                "category_name": "base_componet",
                                "id": 14,
                                "icon": null
                            },
                            {
                                "category_alias": "公共组件",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "企业内部通用类组件或者叫业务组件",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 2,
                                "parent_id": 6,
                                "active": true,
                                "category_name": "public_componet",
                                "id": 15,
                                "icon": null
                            },
                            {
                                "category_alias": "业务程序",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "自定义的非通用类组件（业务相关程务）",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 3,
                                "parent_id": 6,
                                "active": true,
                                "category_name": "biz_process",
                                "id": 16,
                                "icon": null
                            }
                        ],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 4,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "component",
                        "id": 6,
                        "icon": null
                    },
                    {
                        "category_alias": "运维操作",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": "描述运维动作，包含标准运维数据等",
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 5,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "ops",
                        "id": 7,
                        "icon": null
                    },
                    {
                        "category_alias": "营销操作",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 6,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "marketingops",
                        "id": 8,
                        "icon": null
                    },
                    {
                        "category_alias": "告警/故障",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": "也包含系统崩溃类的故障数据，包含系统crash信息",
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 7,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "alert",
                        "id": 9,
                        "icon": null
                    },
                    {
                        "category_alias": "公告",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 8,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "notice",
                        "id": 10,
                        "icon": null
                    },
                    {
                        "category_alias": "安全",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 9,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "security",
                        "id": 11,
                        "icon": null
                    },
                    {
                        "category_alias": "游戏",
                        "sub_list": [
                            {
                                "category_alias": "设备信息",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "客户端设备数据，比如用户手机型号，手机信号强弱等；",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 1,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "equipment",
                                "id": 17,
                                "icon": null
                            },
                            {
                                "category_alias": "道具信息",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": null,
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 3,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "iteminfo",
                                "id": 19,
                                "icon": null
                            },
                            {
                                "category_alias": "玩家行为",
                                "sub_list": [
                                    {
                                        "category_alias": "下载安装",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 1,
                                        "parent_id": 20,
                                        "active": true,
                                        "category_name": "download_install",
                                        "id": 24,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "玩家注册",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 2,
                                        "parent_id": 20,
                                        "active": true,
                                        "category_name": "player_register",
                                        "id": 25,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "登录登出",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 3,
                                        "parent_id": 20,
                                        "active": true,
                                        "category_name": "loginout",
                                        "id": 26,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "游戏过程",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 4,
                                        "parent_id": 20,
                                        "active": true,
                                        "category_name": "game_procedure",
                                        "id": 27,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "营销支付",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": "主要包含支付和帐单流水指标，包含用户预支付、充值/支付、金币交易、
                                        消费/扣费结算帐单等指标或数据",
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 5,
                                        "parent_id": 20,
                                        "active": true,
                                        "category_name": "payments",
                                        "id": 28,
                                        "icon": null
                                    }
                                ],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "玩家关于游戏的操作数据；",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 4,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "player_act",
                                "id": 20,
                                "icon": null
                            },
                            {
                                "category_alias": "玩家体验",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "关乎用户体验的指标，比如游戏玩家体验的FPS/PING性能指标、玩家游戏过程中出现的卡顿、掉线，
                                以及维护游戏安全所做的反外挂、踢人等数据指标",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 5,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "player_exp",
                                "id": 21,
                                "icon": null
                            },
                            {
                                "category_alias": "玩家状态",
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "玩家在游戏内的状态信息，包含大区创建了角色，角色名称，角色拥有道具数量，
                                同时也包含玩家昵称等属性信息；",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 1,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "player_prop",
                                "id": 22,
                                "icon": null
                            },
                            {
                                "category_alias": "用户运营",
                                "sub_list": [
                                    {
                                        "category_alias": "在线相关",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 1,
                                        "parent_id": 23,
                                        "active": true,
                                        "category_name": "onlineabv",
                                        "id": 29,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "容量相关",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 2,
                                        "parent_id": 23,
                                        "active": true,
                                        "category_name": "capacityabv",
                                        "id": 30,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "活跃相关",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 3,
                                        "parent_id": 23,
                                        "active": true,
                                        "category_name": "activeabv",
                                        "id": 31,
                                        "icon": null
                                    },
                                    {
                                        "category_alias": "付费相关",
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 13:00:04",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "seq_index": 4,
                                        "parent_id": 23,
                                        "active": true,
                                        "category_name": "payabv",
                                        "id": 32,
                                        "icon": null
                                    }
                                ],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 13:00:04",
                                "description": "主要是围绕用户运营的通用指标，包含在线人数、回流、新进、活跃等业务指标",
                                "updated_at": null,
                                "created_by":"admin",
                                "seq_index": 2,
                                "parent_id": 12,
                                "active": true,
                                "category_name": "user_ops",
                                "id": 23,
                                "icon": null
                            }
                        ],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 1,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "game",
                        "id": 12,
                        "icon": null
                    },
                    {
                        "category_alias": "舆情",
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 13:00:04",
                        "description": null,
                        "updated_at": null,
                        "created_by":"admin",
                        "seq_index": 2,
                        "parent_id": 0,
                        "active": true,
                        "category_name": "sentiment",
                        "id": 13,
                        "icon": null
                    }
                ],
                "result": true
            }
        """

        sql = """select id,category_name,category_alias,parent_id,seq_index,icon,active,visible,created_by,
        date_format(created_at,'{0}') created_at,updated_by,date_format(updated_at,'{0}') updated_at,description
        from dm_category_config order by parent_id,seq_index""".format(
            parseresult.DEFAULT_DATE_FORMAT
        )
        rpc_response = self.entity_complex_search(sql)
        ret_result = rpc_response.result
        ret_result = parseresult.parse_field_to_boolean(ret_result, "active")
        ret_result = parseresult.parse_field_to_boolean(ret_result, "visible")
        for per_ret in ret_result:
            if "category_alias" in per_ret:
                per_ret["category_alias"] = bktranslates(per_ret["category_alias"])
        return Response(parseresult.parse_data_category_result(ret_result))


class DMLayerConfigViewSet(RPCViewSet):
    lookup_field = "id"

    def list(self, request):
        """
        @api {get} /meta/dm_layer_configs/ 获取数据分层信息

        @apiVersion 0.2.0
        @apiGroup DMLayerConfig
        @apiName list_dm_layer_configs

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "sub_list": [
                            {
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 12:59:08",
                                "description": "资源配置类信息，包含电源、机房、集群、主机配置等；",
                                "updated_at": null,
                                "created_by":"admin",
                                "id": 2,
                                "parent_id": 1,
                                "layer_alias": "硬件资源",
                                "active": true,
                                "seq_index": 1,
                                "layer_name": "hw_resources",
                                "icon": null
                            },
                            {
                                "sub_list": [],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 12:59:08",
                                "description": "网络设备、网络拓扑信息；",
                                "updated_at": null,
                                "created_by":"admin",
                                "id": 3,
                                "parent_id": 1,
                                "layer_alias": "网络",
                                "active": true,
                                "seq_index": 2,
                                "layer_name": "net_resources",
                                "icon": null
                            },
                            {
                                "sub_list": [
                                    {
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 12:59:08",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "id": 5,
                                        "parent_id": 4,
                                        "layer_alias": "物理机",
                                        "active": true,
                                        "seq_index": 1,
                                        "layer_name": "physical_machine",
                                        "icon": null
                                    },
                                    {
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 12:59:08",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "id": 6,
                                        "parent_id": 4,
                                        "layer_alias": "虚拟机",
                                        "active": true,
                                        "seq_index": 2,
                                        "layer_name": "virtual_machine",
                                        "icon": null
                                    },
                                    {
                                        "sub_list": [],
                                        "updated_by": "admin",
                                        "visible": true,
                                        "created_at": "2018-12-20 12:59:08",
                                        "description": null,
                                        "updated_at": null,
                                        "created_by":"admin",
                                        "id": 7,
                                        "parent_id": 4,
                                        "layer_alias": "容器",
                                        "active": true,
                                        "seq_index": 3,
                                        "layer_name": "container",
                                        "icon": null
                                    }
                                ],
                                "updated_by": "admin",
                                "visible": true,
                                "created_at": "2018-12-20 12:59:08",
                                "description": "机器的基础指标数据，包含物理机、虚拟机、容器的性能指标；",
                                "updated_at": null,
                                "created_by":"admin",
                                "id": 4,
                                "parent_id": 1,
                                "layer_alias": "服务器",
                                "active": true,
                                "seq_index": 3,
                                "layer_name": "server",
                                "icon": null
                            }
                        ],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 12:59:08",
                        "description": "包含设备以及系统等资源数据，诸如备用电源设备、主机配置、网络部署、机器各项指标等相关信息",
                        "updated_at": null,
                        "created_by":"admin",
                        "id": 1,
                        "parent_id": 0,
                        "layer_alias": "基础设施层",
                        "active": true,
                        "seq_index": 1,
                        "layer_name": "Infrastructure",
                        "icon": null
                    },
                    {
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 12:59:08",
                        "description": "各类大数据组件或自定义组件数据；",
                        "updated_at": null,
                        "created_by":"admin",
                        "id": 8,
                        "parent_id": 0,
                        "layer_alias": "应用（组件）层",
                        "active": true,
                        "seq_index": 2,
                        "layer_name": "component",
                        "icon": null
                    },
                    {
                        "sub_list": [],
                        "updated_by": "admin",
                        "visible": true,
                        "created_at": "2018-12-20 12:59:08",
                        "description": "业务相关的数据；",
                        "updated_at": null,
                        "created_by":"admin",
                        "id": 9,
                        "parent_id": 0,
                        "layer_alias": "业务层",
                        "active": true,
                        "seq_index": 3,
                        "layer_name": "business",
                        "icon": null
                    }
                ],
                "result": true
            }
        """
        sql = """select id,layer_name,layer_alias,parent_id,seq_index,icon,active,created_by,
        date_format(created_at,'{0}') created_at,updated_by,date_format(updated_at,'{0}') updated_at,description
        from dm_layer_config order by parent_id,seq_index""".format(
            parseresult.DEFAULT_DATE_FORMAT
        )
        rpc_response = self.entity_complex_search(sql)
        ret_result = parseresult.parse_field_to_boolean(rpc_response.result, "active")
        for per_ret in ret_result:
            if "layer_alias" in per_ret:
                per_ret["layer_alias"] = bktranslates(per_ret["layer_alias"])
        if ret_result:
            for ret_dict in ret_result:
                ret_dict["visible"] = True
        return Response(parseresult.parse_data_category_result(ret_result))
