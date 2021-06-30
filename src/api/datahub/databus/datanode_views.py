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

from common.base_utils import model_to_dict
from common.decorators import detail_route
from common.log import logger
from common.views import APIViewSet
from datahub.databus.constants import NodeType
from datahub.databus.exceptions import (
    DataNodeCreateError,
    DataNodeDeleteError,
    DataNodeUpdateError,
    DataNodeValidError,
    NotSupportDataNodeError,
    SourceDuplicateError,
    TransfromProcessingNotFoundError,
)
from datahub.databus.models import TransformProcessing
from datahub.databus.serializers import (
    DataNodeCreateSerializer,
    DataNodeDestroySerializer,
    DataNodeUpdateSerializer,
)
from rest_framework.response import Response

from datahub.databus import datanode, rt


class DataNodeViewSet(APIViewSet):
    """
    固化节点配置相关接口，包括添加、修改、删除
    """

    # 任务的id，用于关联相应的离线hdfs任务列表
    lookup_field = "processing_id"

    def create(self, request):
        """
        @api {POST} /v3/databus/datanodes/ 创建固化节点配置
        @apiGroup DataNode
        @apiDescription 创建固化节点配置
        @apiParam {string{小于255字符}} processing_id transform_processing表的processing_id，固化节点id。
        @apiParam {string{小于255字符}} node_type 节点类型（merge, split）
        @apiParam {int{合法的整数}} bk_biz_id 业务id。
        @apiParam {string{小于255字符}} result_table_name 结果表名，英文表示。
        @apiParam {string{小于255字符}} result_table_name_alias 结果表名，中文表示。
        @apiParam {int{合法的整数}} project_id 项目id
        @apiParam {string{小于2048字符}} source_result_table_ids 源结果表列表，逗号分隔。
        @apiParam {text} config 固化算子逻辑配置
        @apiParam {string{小于128字符}} [description] 备注信息

        @apiError (错误码) 1570022 固化节点算子创建失败.

        @apiParamExample {json} 参数样例:
        {
            "processing_id": "591_pizza_test",
            "node_type": "merge",
            "description": "test",
            "bk_biz_id": 1000,
            "project_id": 25,
            "result_table_name": "test_merge",
            "result_table_name_alias": u"测试merge",
            "source_result_table_ids": "rt_1,rt_2,rt_3",
            "config": ""
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "heads": ["1000_test_merge"],
                "tails": ["1000_test_merge"],
                "result_table_ids": ["1000_test_merge"]
            }
        }
        """
        params = self.params_valid(serializer=DataNodeCreateSerializer)  # type: dict
        node_type = params["node_type"]
        source_result_table_ids = params["source_result_table_ids"]
        source_rt_arr = source_result_table_ids.split(",")
        if len(set(source_rt_arr)) != len(source_rt_arr):
            # 出现重复源节点
            raise SourceDuplicateError()

        if node_type == NodeType.MERGE.value:
            if len(source_rt_arr) == 1 or len(source_rt_arr) > 10:
                raise DataNodeValidError(message=u"固化节点配置参数校验失败,merge节点的源rt数要大于等于2，小于10。当前rt数:%s" % len(source_rt_arr))
            add_res = datanode.add_merge_datanode(params)
        elif node_type == NodeType.SPLIT.value:
            if len(source_rt_arr) > 1:
                raise DataNodeValidError(message=u"固化节点配置参数校验失败,split节点的源rt数要等于1。当前rt数:%s" % len(source_rt_arr))
            add_res = datanode.add_split_datanode(params)
        else:
            raise NotSupportDataNodeError(message_kv={"node_type": node_type})
        if add_res:
            return Response(
                {
                    "heads": add_res["heads"],
                    "tails": add_res["tails"],
                    "result_table_ids": add_res["result_table_ids"],
                }
            )
        else:
            raise DataNodeCreateError()

    def destroy(self, request, processing_id):
        """
        @api {delete} /v3/databus/datanodes/:processing_id/ 删除固化节点配置
        @apiGroup DataNode
        @apiDescription 删除数据库中固化节点配置
        @apiParam {string{小于255字符}} processing_id transform_processing表的processing_id，固化节点id。
        @apiError (错误码) 1570023 固化节点配置删除失败
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": true
        }
        """
        params = self.params_valid(serializer=DataNodeDestroySerializer)
        # 当存在with_data参数时, result_table_ids参数不生效
        with_data = False
        delete_result_tables = list()
        if "with_data" in params:
            with_data = params["with_data"]
        elif "delete_result_tables" in params:
            delete_result_tables = params["delete_result_tables"]

        result = datanode.delete_datanode_config(processing_id, with_data, delete_result_tables)
        if result:
            return Response(True)
        else:
            raise DataNodeDeleteError(message_kv={"processing_id": processing_id})

    def retrieve(self, request, processing_id):
        """
        @api {delete} /v3/databus/datanodes/:processing_id/ 查询固化节点配置
        @apiGroup DataNode
        @apiDescription 删除数据库中固化节点配置
        @apiParam {string{小于255字符}} processing_id transform_processing表的processing_id，固化节点id。
        @apiError (错误码) 1570023 固化节点配置删除失败
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "config": "",
                "connector_name": "puller_merge_xxx",
                "created_at": "2019-01-04 19:09:25",
                "created_by": "xxx",
                "description": "xxx",
                "id": 1,
                "node_type": "merge",
                "processing_id": "xxx",
                "source_result_table_ids": "xx,xx",
                "updated_at": null,
                "updated_by": ""
            }
        }
        """
        try:
            result = TransformProcessing.objects.get(processing_id=processing_id)
            return Response(model_to_dict(result))
        except TransformProcessing.DoesNotExist:
            logger.error(u"查询指定id的固化节点不存在,节点id:%s" % processing_id)
            raise TransfromProcessingNotFoundError(message_kv={"processing_id": processing_id})

    def partial_update(self, request, processing_id):
        """
        @api {patch} /databus/datanodes/:processing_id/ 更新固化节点配置
        @apiGroup DataNode
        @apiDescription 更新总线固化节点配置
        @apiParam {string{小于255字符}} processing_id transform_processing表的processing_id，固化节点id。
        @apiParam {string{小于255字符}} node_type 节点类型（merge, split）
        @apiParam {int{合法的整数}} bk_biz_id 业务id。
        @apiParam {string{小于255字符}} result_table_name 结果表名，英文表示。
        @apiParam {string{小于255字符}} result_table_name_alias 结果表名，中文表示。
        @apiParam {int{合法的整数}} project_id 项目id
        @apiParam {string{小于2048字符}} source_result_table_ids 源结果表列表，逗号分隔。
        @apiParam {text} config 固化算子逻辑配置
        @apiParam {string{小于128字符}} [description] 备注信息

        @apiError (错误码) 1570022 固化节点算子创建失败.

        @apiParamExample {json} 参数样例:
        {
            "node_type": "merge",
            "description": "test",
            "bk_biz_id": 1000,
            "project_id": 25,
            "result_table_name": "test_merge",
            "result_table_name_alias": u"测试merge",
            "source_result_table_ids": "rt_1,rt_2,rt_3",
            "config": ""
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "heads": ["1000_test_merge"],
                "tails": ["1000_test_merge"],
                "result_table_ids": ["1000_test_merge"]
            }
        }
        """
        params = self.params_valid(serializer=DataNodeUpdateSerializer)
        node_type = params["node_type"]
        # result_table更新参数，只能更新中文名和描述
        params["processing_id"] = processing_id
        source_result_table_ids = params["source_result_table_ids"]
        source_rt_arr = source_result_table_ids.split(",")
        if len(set(source_rt_arr)) != len(source_rt_arr):
            # 出现重复源节点
            raise SourceDuplicateError()

        if node_type == NodeType.MERGE.value:
            if len(source_rt_arr) == 1 or len(source_rt_arr) > 10:
                raise DataNodeValidError(message=u"固化节点配置参数校验失败,merge节点的源rt数要大于等于2，小于10。当前rt数:%s" % len(source_rt_arr))
            update_res = datanode.update_merge_datanode(params)
        elif node_type == NodeType.SPLIT.value:
            if len(source_rt_arr) > 1:
                raise DataNodeValidError(message=u"固化节点配置参数校验失败,split节点的源rt数要等于1。当前rt数:%s" % len(source_rt_arr))
            update_res = datanode.update_split_datanode(params)
        else:
            raise NotSupportDataNodeError(message_kv={"node_type": node_type})
        if update_res:
            return Response(
                {
                    "heads": update_res["heads"],
                    "tails": update_res["tails"],
                    "result_table_ids": update_res["result_table_ids"],
                }
            )
        else:
            raise DataNodeUpdateError(message_kv={"processing_id": processing_id})

    @detail_route(methods=["post"], url_path="refresh")
    def refresh(self, request, processing_id):
        """
        @api {post} /databus/datanodes/:processing_id/refresh 下发固化节点配置
        @apiGroup DataNode
        @apiDescription 下发固化节点配置

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": True
        }
        """
        return Response(rt.notify_change(processing_id))
