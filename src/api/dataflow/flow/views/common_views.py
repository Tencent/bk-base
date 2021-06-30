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

from common.decorators import params_valid
from common.errorcodes import ErrorCode
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowCode
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.flow.models import FlowNodeInstance
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer import serializers
from dataflow.flow.utils.templates import load_code_template
from dataflow.shared.meta.field_type_config.field_type_config_helper import FieldTypeConfigHelper
from dataflow.shared.permission import require_username
from dataflow.shared.utils.error_codes import get_codes
from dataflow.udf.functions.function_driver import list_function_doc


class CommonViewSet(APIViewSet):
    @require_username
    def list_function_doc(self, request):
        """
        @api {get}  /dataflow/flow/list_function_doc/ 获取函数
        @apiName list_function_doc
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        'func_groups':[
                            'display':'',
                            'func':[],
                            'group_name':''
                        ],
                        'type_name':'',
                        'display':'',
                        'id':''
                    }
                ]
        """
        res = list_function_doc()
        return Response(res)

    def node_type_config_v2(self, request):
        """
        @api {get} /dataflow/flow/node_type_config_v2 查询节点配置信息
        @apiName node_type_config_v2
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
        [
            {
                "group_type_name": "source",
                "group_type_alias": "数据源",
                "order": 1,
                "instances": [
                    {
                        "available": true,
                        "support_query": false,
                        "description": "清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算",
                        "support_debug": true,
                        "created_at": "2018-12-25 19:15:02",
                        "updated_at": "2018-12-25 19:15:02",
                        "created_by": "xxx",
                        "id": 1,
                        "node_type_instance_alias": "实时数据源",
                        "group_type_alias": "数据源",
                        "node_type_instance_name": "stream_source",
                        "group_type_name": "source",
                        "order": 101,
                        "updated_by": "xxx"
                    }
                ]
            }
        ]
        """
        return Response(FlowNodeInstance.list_instance())

    def node_link_rules_v2(self, request):
        """
        @api {get}  /dataflow/flow/node_link_rules_v2/ 获取节点连线规则
        @apiName node_link_rules_v2
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "node_rules": {
                        "kv_source": {      // kv_source节点类型实例所有有效下游节点类型实例列表
                            "stream": {     // kv_source可连接stream类型的节点
                                'default':{ // 多条路径分为 stream_path,batch_path，单条默认为 default
                                    "upstream_link_limit": [1, 1],
                                    // kv_source->stream，在stream的角度上，上游kv_source的连线范围为1~1
                                    "downstream_link_limit": [1, 10]
                                    // kv_source->stream，在kv_source的角度上，下游stream的连线范围为1~10
                                    "detail_path": "kv_source->stream" // 具体路径
                                }
                            },
                        }
                    },
                    "group_rules": [
                        {
                            "downstream_instance": "stream",        // 下游节点是 stream
                            "upstream_max_link_limit": {
                                // 上游节点有两个: kv_source,stream_source，且它们和stream的连线最大为1条
                                "kv_source": 1,
                                "stream_source": 1
                            }
                        }
                    ]
                }
        """
        return Response(NodeInstanceLink.get_link_rules_config())

    def field_type_configs(self, request):
        """
        @api {get}  /dataflow/flow/field_type_configs/ 获取当前支持的数据类型
        @apiName field_type_configs
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        'field_name': 'xx',
                        'field_type_name': ''
                        'updated_by': 'xx'
                        'created_at': 'xx'
                        'updated_at': 'xx'
                        'created_by': 'xx'
                        'field_type_alias': 'xx'
                        'active': true
                        'description': 'xx'
                    }
                ]
        """
        res = FieldTypeConfigHelper.list_field_type_config()
        support_field_config = list(
            filter(
                lambda field_config: field_config["field_type"] not in ["text", "timestamp"],
                res,
            )
        )
        return Response(support_field_config)

    @params_valid(serializer=serializers.GetLatestMsgSerializer)
    def get_latest_msg(self, request, params):
        """
        @api {get}  /dataflow/flow/get_latest_msg/ 获取节点使用的结果表最近一条数据
        @apiName get_latest_msg
        @apiGroup Flow
        @apiParam {string} result_table_id
        @apiParam {string} node_type
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "dtEventTimeStamp": 1591100157000,
                    "report_time": "2020-06-02 20:12:57",
                    "Cost_Time": 112.35
                }
        """
        result_table_id = params["result_table_id"]
        if params["node_type"] in NodeTypes.BATCH_CATEGORY + NodeTypes.BATCH_SOURCE_CATEGORY:
            latest_msg = ResultTable(result_table_id).get_hdfs_msg()
        else:
            latest_msg = ResultTable(result_table_id).get_kafka_msg()
        if not latest_msg:
            latest_msg = {}
        return Response({"data": latest_msg})

    @params_valid(serializer=serializers.NodeCodeFrameSerializer)
    def code_frame(self, request, params):
        """
        @api {get} /dataflow/flow/code_frame/ 获取代码节点的代码模板
        @apiName code_frame_for_node_code
        @apiGroup Flow
        @apiDescription 获取代码节点的代码模板
        @apiParamExample {json}
            {
                "programming_language": "programming_language",
                "node_type": "flink_streaming",
                "input_result_table_ids": ["id"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "code": "the code",
                }
        """
        programming_language = params["programming_language"]
        node_type = params["node_type"]
        input_result_table_ids = params["input_result_table_ids"]
        return Response({"code": load_code_template(programming_language, node_type, input_result_table_ids)})

    def get_error_codes(self, request):
        """
        @api {get} /dataflow/flow/errorcodes/ 获取flow的errorcodes
        @apiName errorcodes
        @apiGroup udf
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [[
                        {
                        message: "函数开发界面被锁定",
                        code: "1581001",
                        name: "DEV_LOCK_ERR",
                        solution: "-",
                        }]
                    "result": true
                }
        """
        return Response(get_codes(FlowCode, ErrorCode.BKDATA_DATAAPI_FLOW))
