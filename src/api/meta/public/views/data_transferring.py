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
from common.views import APIViewSet
from rest_framework.response import Response

from meta import exceptions as meta_errors
from meta.public.models.data_transferring import DataTransferring
from meta.public.serializers.common import (
    DataTransferringSerializer,
    DataTransferringUpdateSerializer,
    DestroySerializer,
)
from meta.utils.basicapi import parseresult


class DataTransferringViewSet(APIViewSet):
    lookup_field = "transferring_id"

    def list(self, request):
        """
        @api {get} /meta/data_transferrings/ 获取数据传输信息列表

        @apiVersion 0.2.0
        @apiGroup DataTransferring
        @apiName list_data_transferring

        @apiParam {Number} [project_id] 项目ID
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
                        "transferring_id": "xxx",
                        "transferring_alias": "xxx",
                        "transferring_type": "shipper",
                        "generate_type": "user",
                        "created_by ": "xxx",
                        "created_at": "xxx",
                        "updated_by": "xxx",
                        "updated_at": "xxx",
                        "description": "xxx",
                        "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
                        "inputs": [
                            {
                                "data_set_type": "result_table",
                                "data_set_id": "639_battle_info",
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
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")

        where_cond = ""
        if project_id:
            where_cond += " and a.project_id=" + parseresult.escape_string(project_id)

        query_result = parseresult.get_data_transfers_v3(where_cond, page=page, page_size=page_size)
        parseresult.add_manage_tag_to_data_transferring(query_result)
        return Response(query_result)

    @params_valid(serializer=DataTransferringSerializer)
    def create(self, request, params):
        """
        @api {post} /meta/data_transferrings/ 创建数据传输

        @apiVersion 0.2.0
        @apiGroup DataTransferring
        @apiName create_data_transferring

        @apiParam {String} bk_username 用户名
        @apiParam {Number} project_id 项目ID
        @apiParam {String} transferring_id 数据传输ID
        @apiParam {String} transferring_alias 数据传输名称
        @apiParam {String} transferring_type 数据传输类型
        @apiParam {String="user", "system"} generate_type 生成类型
        @apiParam {String} description 数据传输的描述
        @apiParam {Object[]} [inputs] 当前数据传输的输入源
        @apiParam {Object[]} [outputs] 当前数据传输输出的表
        @apiParam {String[]} tags 标签code列表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "project_id": 123,
                "transferring_id": "132_xxx",
                "transferring_alias": "数据传输1",
                "transferring_type": "shipper",
                "generate_type": "user",
                "description": "xxx",
                "tags": ["NA"],
                "inputs": [
                    {
                        "data_set_type": "result_table",
                        "data_set_id": "639_battle_info",
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

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{transferring_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521050 数据传输不存在
        """
        with auto_meta_sync(using="bkdata_basic"):
            DataTransferring.objects.create_data_tranferring(params)

        return Response(params["transferring_id"])

    def retrieve(self, request, transferring_id):
        """
        @api {get} /meta/data_transferrings/:transferring_id/ 获取单个数据传输实例的信息

        @apiVersion 0.2.0
        @apiGroup DataTransferring
        @apiName retrieve_data_transferring

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
                    "transferring_id": "132_xxx",
                    "transferring_alias": "数据传输1",
                    "transferring_type": "shipper",
                    "generate_type": "user",
                    "created_by ": "xxx",
                    "created_at": "xxx",
                    "updated_by": "xxx",
                    "updated_at": "xxx",
                    "description": "xxx",
                    "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
                    "inputs": [
                        {
                            "data_set_type": "result_table",
                            "data_set_id": "639_battle_info",
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

        @apiError 1521050 数据传输不存在
        """
        # TODO
        where_cond = ""
        if transferring_id:
            where_cond += " and a.transferring_id='" + parseresult.escape_string(transferring_id) + "'"

        query_result = parseresult.get_data_transfers_v3(where_cond)
        if query_result:
            parseresult.add_manage_tag_to_data_transferring(query_result[0])
            return Response(query_result[0])
        else:
            return Response({})

    @params_valid(serializer=DataTransferringUpdateSerializer)
    def update(self, request, transferring_id, params):
        """
        @api {put} /meta/data_transferrings/:transferring_id/ 更新数据传输

        @apiVersion 0.2.0
        @apiGroup DataTransferring
        @apiName update_data_transferring

        @apiParam {String} bk_username 用户名
        @apiParam {String} [transferring_alias] 数据传输名称
        @apiParam {String="user", "system"} [generate_type] 生成类型
        @apiParam {String} [description] 数据传输的描述
        @apiParam {Object[]} [inputs] 当前数据传输的输入源
        @apiParam {Object[]} [outputs] 当前数据传输输出的表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "transferring_alias": "xxx",
                "transferring_type": "shipper",
                "description": "xxx",
                "inputs": [
                    {
                        "data_set_type": "result_table",
                        "data_set_id": "639_battle_info",
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

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{processing_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521050 数据传输不存在
        """
        try:
            data_transferring = DataTransferring.objects.get(transferring_id=transferring_id)
        except DataTransferring.DoesNotExist:
            raise meta_errors.DataTransferringNotExistError(message_kv={"transferring_id": transferring_id})

        with auto_meta_sync(using="bkdata_basic"):
            DataTransferring.objects.update_data_transferring(data_transferring, params)

        return Response(transferring_id)

    @params_valid(serializer=DestroySerializer)
    def destroy(self, request, transferring_id, params):
        """
        @api {delete} /meta/data_transferrings/:transferring_id/ 删除数据传输

        @apiVersion 0.2.0
        @apiGroup DataTransferring
        @apiName delete_data_transferring

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{transferring_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521050 数据传输不存在
        """
        try:
            data_transferring = DataTransferring.objects.get(transferring_id=transferring_id)
        except DataTransferring.DoesNotExist:
            raise meta_errors.DataTransferringNotExistError(message_kv={"transferring_id": transferring_id})

        with auto_meta_sync(using="bkdata_basic"):
            DataTransferring.objects.delete_data_transferring(data_transferring)

        return Response(transferring_id)
