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
from common.transaction import auto_meta_sync
from rest_framework.decorators import action
from rest_framework.response import Response

from meta import exceptions as meta_errors
from meta.basic.common import RPCViewSet
from meta.public.models.data_processing import DataProcessing
from meta.public.serializers.common import (
    DataProcessingBulkDestroySerializer,
    DataProcessingDestroySerializer,
    DataProcessingSerializer,
    DataProcessingUpdateSerializer,
)
from meta.utils.basicapi import parseresult


class DataProcessingViewSet(RPCViewSet):
    lookup_field = "processing_id"

    def list(self, request):
        """
        @api {get} /meta/data_processings/ 获取数据处理信息列表

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName list_data_processing

        @apiParam {Number} [project_id] 项目ID
        @apiParam {String} [processing_type] processing_type
        @apiParam {Number} [page] 页码
        @apiParam {Number} [page_size] 分页大小

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "project_id": 1,
                        "project_name": "测试项目",
                        "processing_id": "xxx",
                        "processing_alias": "xxx",
                        "processing_type": "xxx",
                        "generate_type": "user",
                        "created_by ": "xxx",
                        "created_at": "xxx",
                        "updated_by": "xxx",
                        "updated_at": "xxx",
                        "description": "xxx",
                        "platform" : "bkdata",
                        "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
                        "inputs": [
                            {
                                "data_set_type": "raw_data",
                                "data_set_id": 4666,
                                "storage_cluster_config_id": null,
                                "channel_cluster_config_id": 1,
                                "storage_type": "channel"
                            }
                        ],
                        "outputs": [
                            {
                                "data_set_type": "result_table",
                                "data_set_id": "639_battle_info",
                                "storage_cluster_config_id": 1,
                                "channel_cluster_config_id": null,
                                "storage_type": "storage"
                            }
                        ]
                    }
                ]
            }
        """
        # TODO
        project_id = request.query_params.get("project_id")
        processing_ids = request.query_params.getlist("processing_id")
        processing_type = request.query_params.get("processing_type")
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")

        where_cond = ""
        if project_id:
            where_cond += " and a.project_id=" + parseresult.escape_string(project_id)

        if processing_type:
            where_cond += " and a.processing_type='" + parseresult.escape_string(processing_type) + "'"

        if processing_ids:
            where_cond += ' and a.processing_id in ("{}")'.format(
                '", "'.join(parseresult.escape_string(processing_id) for processing_id in processing_ids)
            )

        query_result = parseresult.get_data_processings_v3(where_cond, page=page, page_size=page_size)
        parseresult.add_manage_tag_to_data_processing(query_result)
        return Response(query_result)

    @params_valid(serializer=DataProcessingSerializer)
    def create(self, request, params):
        """
        @api {post} /meta/data_processings/ 创建数据处理

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName create_data_processing

        @apiParam {String} bk_username 用户名
        @apiParam {Number} project_id 项目ID
        @apiParam {String} processing_id 数据处理ID
        @apiParam {String} processing_alias 数据处理名称
        @apiParam {String} processing_type 数据处理类型
        @apiParam {String='bkdata','tdw'} platform 数据处理平台
        @apiParam {String="user", "system"} generate_type 生成类型
        @apiParam {String} description 数据处理的描述
        @apiParam {Object[]} [result_tables] 需要跟数据处理一起创建的结果表
        @apiParam {Object[]} [inputs] 当前数据处理的输入源
        @apiParam {Object[]} [outputs] 当前数据处理输出的表
        @apiParam {String[]} tags 标签code列表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "project_id": 123,
                "processing_id": "132_xxx",
                "processing_alias": "数据处理1",
                "processing_type": "clean",
                "generate_type": "user",
                "description": "xxx",
                "platform": "bkdata",
                "tags": ["NA"],
                "result_tables": [
                    {
                        "result_table_id": "591_xxx",
                        "bk_biz_id": 591,
                        "result_table_name": "xxx",
                        "result_table_name_alias ": "xxx",
                        "result_table_type": null,
                        "sensitivity": "public",
                        "count_freq": 60,
                        "description": "xxx",
                        "tags": ["NA"],
                        "fields": [
                            {
                                "field_index": 1,
                                "field_name": "timestamp",
                                "field_alias": "时间",
                                "description": "",
                                "field_type": "timestamp",
                                "is_dimension": 0,
                                "origins": ""
                            }
                        ]
                    }
                ],
                "inputs": [
                    {
                        "data_set_type": "raw_data",
                        "data_set_id": 4666,
                        "storage_cluster_config_id": null,
                        "channel_cluster_config_id": 1,
                        "storage_type": "channel",
                        "tags": ["tag_input"]
                    }
                ],
                "outputs": [
                    {
                        "data_set_type": "result_table",
                        "data_set_id": "639_battle_info",
                        "storage_cluster_config_id": 1,
                        "channel_cluster_config_id": null,
                        "storage_type": "storage",
                        "tags": ["tag_output"]
                    }
                ]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{processing_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521026 结果表已存在
        """
        if not DataProcessing.objects.check_processing_stream_legality(request, params):
            raise meta_errors.DataProcessingRelationInputIllegalityError(message_kv={"msg": "stream is illegal"})

        with auto_meta_sync(using="bkdata_basic"):
            DataProcessing.objects.create_data_processing(request, params)

        return Response(params["processing_id"])

    def retrieve(self, request, processing_id):
        """
        @api {get} /meta/data_processings/:processing_id/ 获取单个数据处理实例的信息

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName retrieve_data_processing

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "project_id": 1,
                    "project_name": "测试项目",
                    "processing_id": "132_xxx",
                    "processing_alias": "数据处理1",
                    "processing_type": "clean",
                    "generate_type": "user",
                    "created_by ": "xxx",
                    "created_at": "xxx",
                    "updated_by": "xxx",
                    "updated_at": "xxx",
                    "description": "xxx",
                    "platform": "bkdata",
                    "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
                    "inputs": [
                        {
                            "data_set_type": "raw_data",
                            "data_set_id": 4666,
                            "storage_cluster_config_id": null,
                            "channel_cluster_config_id": 1,
                            "storage_type": "channel"
                        }
                    ],
                    "outputs": [
                        {
                            "data_set_type": "result_table",
                            "data_set_id": "639_battle_info",
                            "storage_cluster_config_id": 1,
                            "channel_cluster_config_id": null,
                            "storage_type": "storage"
                        }
                    ]
                }
            }

        @apiError 1521040 数据处理不存在
        """
        # TODO
        where_cond = ""
        if processing_id:
            where_cond += " and a.processing_id='" + parseresult.escape_string(processing_id) + "'"

        query_result = parseresult.get_data_processings_v3(where_cond)
        if query_result:
            parseresult.add_manage_tag_to_data_processing(query_result[0])
            return Response(query_result[0])
        else:
            return Response({})

    @params_valid(serializer=DataProcessingUpdateSerializer)
    def update(self, request, processing_id, params):
        """
        @api {put} /meta/data_processings/:processing_id/ 更新数据处理

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName update_data_processing

        @apiParam {String} bk_username 用户名
        @apiParam {String} [processing_alias] 数据处理名称
        @apiParam {String="user", "system"} [generate_type] 生成类型
        @apiParam {String} [description] 数据处理的描述
        @apiParam {Object[]} [result_tables] 数据处理的结果表列表
        @apiParam {Object[]} [inputs] 当前数据处理的输入源
        @apiParam {Object[]} [outputs] 当前数据处理输出的表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "processing_alias": "xxx",
                "processing_type": "xxx",
                "description": "xxx",
                "result_tables": [
                    {
                        "result_table_id": "591_xxx",
                        "bk_biz_id": 591,
                        "result_table_name": "xxx",
                        "result_table_name_alias ": "xxx",
                        "result_table_type": null,
                        "generate_type": "user",
                        "sensitivity": "public",
                        "count_freq": 60,
                        "description": "xxx",
                        "platform": "bkdata",
                        "fields": [
                            {
                                "field_index": 1,
                                "field_name": "timestamp",
                                "field_alias": "时间",
                                "description": "",
                                "field_type": "timestamp",
                                "is_dimension": 0,
                                "origins": ""
                            }
                        ]
                    }
                ],
                "inputs": [
                    {
                        "data_set_type": "raw_data",
                        "data_set_id": 4666,
                        "storage_cluster_config_id": null,
                        "channel_cluster_config_id": 1,
                        "storage_type": "channel",
                        "tags": ["tag_input"]
                    }
                ],
                "outputs": [
                    {
                        "data_set_type": "result_table",
                        "data_set_id": "639_battle_info",
                        "storage_cluster_config_id": 1,
                        "channel_cluster_config_id": null,
                        "storage_type": "storage",
                        "tags": ["tag_output"]
                    }
                ]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{processing_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521040 数据处理不存在
        """
        try:
            data_processing = DataProcessing.objects.get(processing_id=processing_id)
        except DataProcessing.DoesNotExist:
            raise meta_errors.DataProcessingNotExistError(message_kv={"processing_id": processing_id})

        with auto_meta_sync(using="bkdata_basic"):
            DataProcessing.objects.update_data_processing(request, data_processing, params)

        return Response(processing_id)

    @params_valid(serializer=DataProcessingDestroySerializer)
    def destroy(self, request, processing_id, params):
        """
        @api {delete} /meta/data_processings/:processing_id/ 删除数据处理

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName delete_data_processing

        @apiParam {String} bk_username 用户名
        @apiParam {Boolean} with_data=false 是否删除下游的结果表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{processing_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521040 数据处理不存在
        """
        with_data = params.pop("with_data", False)

        try:
            data_processing = DataProcessing.objects.get(processing_id=processing_id)
        except DataProcessing.DoesNotExist:
            raise meta_errors.DataProcessingNotExistError(message_kv={"processing_id": processing_id})

        # 把待删除的结果表信息组装后写入result_table_del表中，并从result_table和result_table_field中删除记录
        with auto_meta_sync(using="bkdata_basic"):
            DataProcessing.objects.delete_data_processing(request, data_processing, with_data)

        return Response(processing_id)

    @action(detail=False, methods=["delete"], url_path="bulk")
    @params_valid(serializer=DataProcessingBulkDestroySerializer)
    def bulk_delete(self, request, params):
        """
        @api {delete} /meta/data_processings/bulk/ 批量删除数据处理

        @apiVersion 0.2.0
        @apiGroup DataProcessing
        @apiName bulk_delete_data_processing

        @apiParam {String} bk_username 用户名
        @apiParam {Object[]} processings 数据处理删除配置列表
        @apiParam {Boolean} [with_data=false] 是否删除全部下游的结果表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "admin",
                "processings": [{
                    "processing_id": "abc",
                    "with_data": false
                }]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }

        @apiError 1521040 数据处理不存在
        """
        with_data = params.pop("with_data", False)
        processings = params.pop("processings", [])
        del_processing_ids = []

        # 把待删除的结果表信息组装后写入result_table_del表中，并从result_table和result_table_field中删除记录
        with auto_meta_sync(using="bkdata_basic"):
            for processing in processings:
                try:
                    processing_id = processing["processing_id"]
                    data_processing = DataProcessing.objects.get(processing_id=processing_id)
                except DataProcessing.DoesNotExist:
                    continue

                try:
                    DataProcessing.objects.delete_data_processing(
                        request, data_processing, processing.get("with_data", with_data)
                    )
                    del_processing_ids.append(processing_id)
                except Exception:
                    continue
        return Response(del_processing_ids)
