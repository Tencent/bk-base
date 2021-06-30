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

from common.decorators import list_route, params_valid
from common.django_utils import DataResponse
from rest_framework.response import Response

from dataflow.modeling.exceptions.comp_exceptions import (
    DEFAULT_MODELING_ERR,
    ModelingExpectedException,
    ModelingUnexpectedException,
    SqlParseError,
)
from dataflow.modeling.processing.process_controller import ModelingProcessController
from dataflow.modeling.processing.processing_serializer import (
    CreateBulkProcessSerializer,
    CreateDataflowProcessSerializer,
    DeleteDataflowProcessSerializer,
    DeleteProcessSerializer,
    ModelingProcessingsSerializer,
    ParseBulkProcessSerializer,
    UpdateDataflowProcessSerializer,
)
from dataflow.modeling.views.base_views import BaseViewSet
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.permission import require_username


class ProcessingViewSet(BaseViewSet):
    lookup_field = "processing_id"

    @params_valid(serializer=ModelingProcessingsSerializer)
    @require_username
    def create(self, request, params):
        """
        @api {post} /dataflow/modeling/processings 新增TensorFlow Processing
        @apiName create_processing
        @apiGroup modeling
        @apiParam {string} bk_username 提交人
        @apiParam {int} project_id 项目id
        @apiParam {list} outputs 输出配置
        @apiParam {list} tags 地域标签
        @apiParam {json} dedicated_config 离线计算配置
        @apiParam {list} window_info 上游节点窗口配置
        @apiParamExample {json} 参数样例:
        {
            "bk_username": "xxxx",
            "project_id": 13105,
            "tags": [
                "inland"
            ],
            "name": "xxx",
            "bk_biz_id": 591,
            "dedicated_config": {
                "batch_type": "tensorflow",
                "user_args": "optimizer=xxxx epochs=5 batch_size=5",
                "script": "xxx.py",
                "self_dependence": {
                    "self_dependency": false,
                    "self_dependency_config": {}
                },
                "schedule_config": {
                    "count_freq": 1,
                    "schedule_period": "hour",
                    "start_time": "2021-03-11 00:00:00"
                },
                "recovery_config": {
                    "recovery_enable": false,
                    "recovery_times": "1",
                    "recovery_interval": "60m"
                },
                "input_config": {
                    "models": [
                        {
                            "name": "591_xxxx"
                        }
                    ],
                    "tables": [
                        {
                            "result_table_id": "591_xxxx",
                            "label_shape": 0
                        }
                    ]
                },
                "output_config": {
                    "tables": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "xxxx_9",
                            "table_alias": "xxxx_9",
                            "enable_customize_output": false,
                            "need_create_storage":true,
                            "fields": [
                                {
                                    "is_dimension": false,
                                    "field_name": "x_1",
                                    "field_type": "double",
                                    "field_alias": "CRIM",
                                    "field_index": 0
                                },
                                {
                                    "is_dimension": false,
                                    "field_name": "x_2",
                                    "field_type": "double",
                                    "field_alias": "CRIM",
                                    "field_index": 0
                                }
                            ]
                        }
                    ],
                    "models": []
                }
            },
            "window_info": [
                {
                    "result_table_id": "591_xxxx",
                    "window_type": "scroll",
                    "window_offset": "0",
                    "window_offset_unit": "hour",
                    "dependency_rule": "at_least_one_finished",
                    "window_size": "2",
                    "window_size_unit": "hour",
                    "window_start_offset": "1",
                    "window_start_offset_unit": "hour",
                    "window_end_offset": "2",
                    "window_end_offset_unit": "hour",
                    "accumulate_start_time": "2021-03-11 00:00:00"
                }
            ]
        }
        @apiSuccessExample {json} 成功返回processing_id
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data":  {
                    "result_table_ids": [processing_id],
                    "processing_id": processing_id,
                    "result_model_ids": [result_model_id]
                },
                "result": true
            }
        """
        res = ModelingProcessController.create_bulk_processing(params)
        return Response(res)

    @params_valid(serializer=ModelingProcessingsSerializer)
    def update(self, request, processing_id, params):
        """
        @api {post} /dataflow/modeling/processings/:processing_id 更新TensorFlow processing
        @apiName update_processing
        @apiGroup modeling
        @apiParam {string} bk_username 提交人
        @apiParam {int} project_id 项目id
        @apiParam {list} outputs 输出配置
        @apiParam {list} tags 地域标签
        @apiParam {json} dedicated_config 离线计算配置
        @apiParam {list} window_info 上游节点窗口配置
        @apiParamExample {json} 参数样例:
        {
            "bk_username": "xxxx",
            "project_id": 13105,
            "tags": [
                "inland"
            ],
            "name": "xxx",
            "bk_biz_id": 591,
            "dedicated_config": {
                "batch_type": "tensorflow",
                "user_args": "optimizer=xxxx epochs=5 batch_size=5",
                "script": "xxx.py",
                "self_dependence": {
                    "self_dependency": false,
                    "self_dependency_config": {}
                },
                "schedule_config": {
                    "count_freq": 1,
                    "schedule_period": "hour",
                    "start_time": "2021-03-11 00:00:00"
                },
                "recovery_config": {
                    "recovery_enable": false,
                    "recovery_times": "1",
                    "recovery_interval": "60m"
                },
                "input_config": {
                    "models": [
                        {
                            "name": "591_xxxx"
                        }
                    ],
                    "tables": [
                        {
                            "result_table_id": "591_xxxx",
                            "label_shape": 0
                        }
                    ]
                },
                "output_config": {
                    "tables": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "xxxx_9",
                            "table_alias": "xxxx_9",
                            "enable_customize_output": false,
                            "need_create_storage":true,
                            "fields": [
                                {
                                    "is_dimension": false,
                                    "field_name": "x_1",
                                    "field_type": "double",
                                    "field_alias": "CRIM",
                                    "field_index": 0
                                },
                                {
                                    "is_dimension": false,
                                    "field_name": "x_2",
                                    "field_type": "double",
                                    "field_alias": "CRIM",
                                    "field_index": 0
                                }
                            ]
                        }
                    ],
                    "models": []
                }
            },
            "window_info": [
                {
                    "result_table_id": "591_xxxx",
                    "window_type": "scroll",
                    "window_offset": "0",
                    "window_offset_unit": "hour",
                    "dependency_rule": "at_least_one_finished",
                    "window_size": "2",
                    "window_size_unit": "hour",
                    "window_start_offset": "1",
                    "window_start_offset_unit": "hour",
                    "window_end_offset": "2",
                    "window_end_offset_unit": "hour",
                    "accumulate_start_time": "2021-03-11 00:00:00"
                }
            ]
        }
        @apiSuccessExample {json} 成功返回processing_id
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data":  {
                    "result_table_ids": [processing_id],
                    "processing_id": processing_id,
                    "result_model_ids": [result_model_id]
                },
                "result": true
            }
        """
        controller = ModelingProcessController(processing_id)
        res = controller.multi_update_process(params)
        return Response(res)

    @params_valid(serializer=DeleteProcessSerializer)
    def destroy(self, request, processing_id, params):
        """
        @api {delete} /dataflow/modeling/processings/:processing_id 删除Modeling的processing
        @apiName delete_processing
        @apiGroup modeling
        @apiParam {string} api_version api版本
        @apiParam {string} processing_id processing的id
        @apiParamExample {json} 参数样例:
            {
                "api_version", "v2"
                "processing_id": processing_id
                "with_data": False
            }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {"processing_id": processing_id},
                    "result": true
                }
        """
        controller = ModelingProcessController(processing_id)
        controller.multi_delete_process(params)
        return Response({"processing_id": processing_id})

    @list_route(methods=["post"], url_path="bulk")
    @params_valid(serializer=CreateBulkProcessSerializer)
    def bulk(self, request, params):
        """
        @api {post}  /dataflow/modeling/processing/bulk/ 创建多个processing
        @apiName processing/bulk
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "sql_list" : [sql1, sql2],
                "bk_biz_id": 591,
                "project_id": 13105,
                "type": "datalab",
                "bk_username": "xxxx",
                "tags": ["inland"],
                "notebook_id": 499,
                "cell_id": 7,
                "component_type": "spark_mllib"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "job_id": "13105_xxxxxxxx",
                    "processing_ids": [591_pca_table_result],
                    "heads": [591_pca_table, 591_pca_model],
                    "tails": [591_pca_table_result],
                    "result_table_ids": [591_pca_table_result]
                }
        """
        try:
            result = ModelingProcessController.create_bulk_processing(params)
            return Response(result)
        except (ModelingExpectedException, ModelingUnexpectedException) as e:
            logger.error(e.message)
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="parse_mlsql_tables")
    @params_valid(serializer=ParseBulkProcessSerializer)
    def parse_mlsql_tables(self, request, params):
        """
        @api {post}  /dataflow/modeling/processing/parse_mlsql_tables/ 解析mlsql中表的操作
        @apiName processing/parse_mlsql_tables
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "sql_list" : [sql1, sql2]
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "read": [
                            "591_xxx_mock_mix_1",
                            "591_xxx_mock_mix_1",
                            "591_string_indexer_result_106",
                            "591_xxx_mock_mix_1"
                        ],
                        "write": []
                    },
                    "result": true
                }
        """
        try:
            result = ModelingProcessController.parse_mlsql_tables(params["sql_list"])
            return Response(result)
        except SqlParseError as e:
            logger.error("parse mlsql error:%s" % e)
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            logger.error("parse mlsql error:%s" % e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_save_processings")
    @params_valid(serializer=CreateDataflowProcessSerializer)
    @require_username
    def multi_save_process(self, request, params):
        """
        @api {post}  /dataflow/modeling/processings/multi_save_processings/ 将模型中的JSON转换为process
        @apiName processings/multi_save_processings
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "model_config" : {
                    'model_1': {},
                    'model':{}
                },
                "submit_args": {'dependence':{}, 'node':{}},
                "type": "dataflow",
                "project_id": xxxx,
                "bk_biz_id": xxxx,
                "tags": ["xxxx"]
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "processing_id": '591_xxxxx',
                        "result_tables": [
                            "591_xxx_mock_model_1"
                        ]
                    },
                    "result": true
                }
        """
        try:
            result = ModelingProcessController.create_bulk_processing(params)
            return Response(result)
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_update_processings")
    @require_username
    @params_valid(serializer=UpdateDataflowProcessSerializer)
    def multi_update_process(self, request, params):
        """
        @api {post}  /dataflow/modeling/processings/multi_update_processings/ 更新模型应用节点
        @apiName processing/multi_update_processings
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "model_config" : {
                    'model_1': {},
                    'model':{}
                },
                "submit_args": {'dependence':{}, 'node':{}},
                "type": "dataflow",
                "project_id": xxxx,
                "bk_biz_id": xxxx,
                "tags": ["xxxx"]
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                 {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "processing_id": '591_xxxxx',
                        "result_tables": [
                            "591_xxx_mock_model_1"
                        ]
                    },
                    "result": true
                }
        """
        try:
            controller = ModelingProcessController(params["processing_id"])
            result = controller.multi_update_process(params)
            return Response(result)
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_delete_processings")
    @params_valid(serializer=DeleteDataflowProcessSerializer)
    def multi_delete_process(self, request, params):
        """
        @api {post}  /dataflow/modeling/processings/multi_delete_processings/ 更新模型应用节点
        @apiName processing/multi_delete_processings
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "591_xxxxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                 {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        try:
            controller = ModelingProcessController(params["processing_id"])
            result = controller.multi_delete_process(params)
            return Response(result)
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))
