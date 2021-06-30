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


from common.base_utils import custom_params_valid
from common.decorators import params_valid
from common.exceptions import ValidationError
from common.transaction import auto_meta_sync
from common.views import APIView
from rest_framework.response import Response

from meta import exceptions as meta_errors
from meta.public.models.data_processing import DataProcessing
from meta.public.models.data_transferring import DataTransferring
from meta.public.models.result_table import ResultTable
from meta.public.models.result_table_action import ResultTableActions
from meta.public.serializers.common import (
    DataProcessingDestroySerializer,
    DataProcessingSerializer,
    DataProcessingUpdateSerializer,
    DataTransferringSerializer,
    DataTransferringUpdateSerializer,
    DestroySerializer,
    MetaTransactionSerializer,
)
from meta.public.serializers.result_table import (
    ResultTableSerializer,
    ResultTableUpdateSerializer,
)


class MetaTransactionView(APIView):
    OBJECT_OPERATE_SERIALIZERS = {
        "result_table": {
            "create": ResultTableSerializer,
            "update": ResultTableUpdateSerializer,
            "destroy": DestroySerializer,
        },
        "data_processing": {
            "create": DataProcessingSerializer,
            "update": DataProcessingUpdateSerializer,
            "destroy": DataProcessingDestroySerializer,
        },
        "data_transferring": {
            "create": DataTransferringSerializer,
            "update": DataTransferringUpdateSerializer,
            "destroy": DestroySerializer,
        },
    }

    OBJECT_OPERATE_PRIMARY_KEY = {
        "result_table": "result_table_id",
        "data_processing": "processing_id",
        "data_transferring": "transferring_id",
    }

    @params_valid(serializer=MetaTransactionSerializer)
    def post(self, request, params):
        """
        @api {post} /meta/meta_transaction/ MetaApi集合事务接口
        @apiVersion 0.2.0
        @apiGroup MetaTransaction
        @apiName meta_transaction

        @apiParam {String} bk_username 操作者
        @apiParam {Object[]} api_operate_list API操作集合列表
        @apiParam {String="result_table", "data_processing", "data_transferring"} operate_object 操作对象
        @apiParam {String="create", "update", "destroy"} operate_type 操作类型
        @apiParam {Object} operate_params 操作参数

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangsan",
                "api_operate_list": [
                    {
                        "operate_object": "data_processing",
                        "operate_type": "update",
                        "operate_params": {
                            "processing_id": "591_hahaha",
                            "outputs": [
                                {
                                    "data_set_type": "result_table",
                                    "data_set_id": "591_haha_test",
                                    "storage_cluster_config_id": null,
                                    "channel_cluster_config_id": 1,
                                    "storage_type": "channel"
                                }
                            ]
                        }
                    },
                    {
                        "operate_object": "data_transferring",
                        "operate_type": "create",
                        "operate_params": {
                            "project_id": 591,
                            "transferring_id": "591_xxx",
                            "transferring_alias": "数据传输1",
                            "transferring_type": "shipper",
                            "generate_type": "system",
                            "description": "xxx",
                            "tags": ["NA"],
                            "inputs": [
                                {
                                    "data_set_type": "result_table",
                                    "data_set_id": "591_haha_test",
                                    "storage_cluster_config_id": null,
                                    "channel_cluster_config_id": 1,
                                    "storage_type": "channel"
                                }
                            ],
                            "outputs": [
                                {
                                    "data_set_type": "result_table",
                                    "data_set_id": "591_haha_test",
                                    "storage_cluster_config_id": 1,
                                    "channel_cluster_config_id": null,
                                    "storage_type": "storage"
                                }
                            ]
                        }
                    },
                    {
                        "operate_object": "data_processing",
                        "operate_type": "create",
                        "operate_params": {
                            "processing_id": "591_new_processing",
                            "tags": ["NA"],
                            ...
                        }
                    }
                ]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": ["591_hahaha", "591_xxx", "591_new_processing"]
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiSuccessExample Error-Response:
            HTTP/1.1 200 OK
            {
                "data": null
                "result": false,
                "message": "参数校验失败",
                "code": 1500001,
                "errors": [
                    {
                        "errors": {},
                        "message": ""
                    },
                    {
                        "errors": {},
                        "message": ""
                    },
                    {
                        "errors": {},
                        "message": ""
                    }
                ]
            }

        @apiError 1500001 参数校验失败
        @apiError 1521021 结果表字段冲突
        """
        api_operate_list = params["api_operate_list"]
        bk_username = params["bk_username"]

        errors = []
        # 用校验器对参数进行校验
        validate_error = False
        for item in api_operate_list:
            operate_object = item["operate_object"]
            operate_type = item["operate_type"]
            if "bk_username" not in item["operate_params"]:
                item["operate_params"]["bk_username"] = bk_username
            serializer = self.OBJECT_OPERATE_SERIALIZERS.get(operate_object, {}).get(operate_type, None)
            if serializer:
                try:
                    item["primary_key"] = item["operate_params"].get(
                        self.OBJECT_OPERATE_PRIMARY_KEY.get(operate_object, None), None
                    )
                    item["operate_params"] = custom_params_valid(serializer=serializer, params=item["operate_params"])
                    errors.append({"errors": {}, "message": ""})
                except ValidationError as e:
                    validate_error = True
                    errors.append({"errors": e.errors, "message": e.message})
        if validate_error:
            raise ValidationError("参数校验失败", errors=errors)

        response_data = []
        with auto_meta_sync(using="bkdata_basic"):
            rt_actions = ResultTableActions()
            for item in api_operate_list:
                operate_object = item["operate_object"]
                operate_type = item["operate_type"]
                operate_params = item["operate_params"]
                primary_key = item["primary_key"]
                if operate_object == "result_table":
                    response_data.append(
                        self.operate_result_table(request, rt_actions, operate_type, operate_params, primary_key)
                    )
                elif operate_object == "data_processing":
                    response_data.append(
                        self.operate_data_processing(request, operate_type, operate_params, primary_key)
                    )
                elif operate_object == "data_transferring":
                    response_data.append(self.operate_data_transferring(operate_type, operate_params, primary_key))
                else:
                    raise meta_errors.NotSupportOperateObject(message_kv={"operate_object": operate_object})

        return Response(response_data)

    @staticmethod
    def operate_result_table(request, rt_actions, operate_type, operate_params, result_table_id):
        if operate_type == "create":
            rt_actions.run_create(request, operate_params)
            return result_table_id

        try:
            result_table = ResultTable.objects.get(result_table_id=result_table_id)
        except ResultTable.DoesNotExist:
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})
        if operate_type == "update":
            rt_actions.run_update(request, result_table, operate_params)
            return result_table_id

        if operate_type == "destroy":
            rt_actions.run_destroy(request, result_table)
            return result_table_id

        raise meta_errors.NotSupportOperateType(
            message_kv={"operate_object": "result_table", "operate_type": operate_type}
        )

    @staticmethod
    def operate_data_processing(request, operate_type, operate_params, processing_id):
        if operate_type == "create":
            if not DataProcessing.objects.check_processing_stream_legality(request, operate_params):
                raise meta_errors.DataProcessingRelationInputIllegalityError(message_kv={"msg": "stream is illegal"})
            DataProcessing.objects.create_data_processing(request, operate_params)
            return processing_id

        try:
            data_processing = DataProcessing.objects.get(processing_id=processing_id)
        except DataProcessing.DoesNotExist:
            raise meta_errors.DataProcessingNotExistError(message_kv={"processing_id": processing_id})

        if operate_type == "update":
            DataProcessing.objects.update_data_processing(request, data_processing, operate_params)
            return processing_id

        if operate_type == "destroy":
            DataProcessing.objects.delete_data_processing(request, data_processing)
            return processing_id

        raise meta_errors.NotSupportOperateType(
            message_kv={"operate_object": "data_processing", "operate_type": operate_type}
        )

    @staticmethod
    def operate_data_transferring(operate_type, operate_params, transferring_id):
        if operate_type == "create":
            DataTransferring.objects.create_data_tranferring(operate_params)
            return transferring_id

        try:
            data_transferring = DataTransferring.objects.get(transferring_id=transferring_id)
        except DataTransferring.DoesNotExist:
            raise meta_errors.DataTransferringNotExistError(message_kv={"transferring_id": transferring_id})

        if operate_type == "update":
            DataTransferring.objects.update_data_transferring(data_transferring, operate_params)
            return transferring_id

        if operate_type == "destroy":
            DataTransferring.objects.delete_data_transferring(data_transferring)
            return transferring_id

        raise meta_errors.NotSupportOperateType(
            message_kv={"operate_object": "data_transferring", "operate_type": operate_type}
        )
