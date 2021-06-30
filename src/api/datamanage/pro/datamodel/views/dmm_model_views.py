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


from datamanage.pro.datamodel.dmm.manager import (
    CalculationAtomManager,
    DataModelManager,
    IndicatorManager,
    MasterTableManager,
    OperationLogManager,
)
from datamanage.pro.datamodel.handlers.constraint import get_field_constraint_tree_list
from datamanage.pro.datamodel.handlers.field_type import get_field_type_configs
from datamanage.pro.datamodel.models.model_dict import (
    CalculationAtomType,
    InnerField,
    TimeField,
)
from datamanage.pro.datamodel.serializers.data_model import (
    BkUserNameSerializer,
    DataModelCreateSerializer,
    DataModelDiffSerializer,
    DataModelImportSerializer,
    DataModelInfoSerializer,
    DataModelListSerializer,
    DataModelNameValidateSerializer,
    DataModelOverviewSerializer,
    DataModelReleaseSerializer,
    DataModelUpdateSerializer,
    FieldTypeListSerializer,
    MasterTableCreateSerializer,
    MasterTableListSerializer,
    OperationLogListSerializer,
    RelatedDimensionModelListSerializer,
    ResultTableFieldListSerializer,
)
from datamanage.pro.datamodel.serializers.validators.url_params import convert_to_number
from datamanage.utils.api.meta import MetaApi
from rest_framework.response import Response

from common.auth import check_perm
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIModelViewSet, APIViewSet


class DataModelViewSet(APIViewSet):
    lookup_value_regex = "[0-9]+"
    lookup_field = "model_id"

    @params_valid(serializer=DataModelCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/datamodel/models/ *创建数据模型

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_create
        @apiDescription 创建数据模型

        @apiParam {String} model_name 模型名称
        @apiParam {String} model_alias 模型别名
        @apiParam {String} model_type 模型类型
        @apiParam {String} description 模型描述
        @apiParam {Int} project_id 项目id
        @apiParam {List} tags 标签
        @apiParam {String} bk_username 用户名

        @apiParamExample {json} 参数样例:
        {
            "model_name": "fact_item_flow",
            "model_alias": "道具流水表",
            "model_type": "fact_table",
            "description": "道具流水",
            "tags": [
                {
                    "tag_code":"common_dimension",
                    "tag_alias":"公共维度"
                },{
                    "tag_code":"",
                    "tag_alias":"自定义标签名称"
                }
            ],
            "project_id": 4172,
            "bk_username": "xx"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_id": 1,
                    "model_name": "fact_item_flow",
                    "model_alias": "道具流水表",
                    "model_type": "fact_table",
                    "description": "道具流水",
                    "tags": [
                        {
                            "alias": "公共维度",
                            "code": "common_dimension"
                        },
                        {
                            "alias": "测试标签",
                            "code": "c_tag_1603956645_976235_139710369135088"
                        }
                    ],
                    "table_name": "fact_item_flow",
                    "table_alias": "道具流水表",
                    "project_id": 4172,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "publish_status":"developing",
                    "active_status":"active",
                    "step_id": 1
                }
            }
        """
        bk_username = get_request_username()
        check_perm("datamodel.create", params["project_id"])
        # 创建数据模型
        datamodel_dict = DataModelManager.create_data_model(params, bk_username)
        return Response(datamodel_dict)

    @list_route(methods=["get"], url_path="validate_model_name")
    @params_valid(serializer=DataModelNameValidateSerializer)
    def validate_model_name(self, request, params):
        """
        @api {get} /datamanage/datamodel/models/validate_model_name/ *判断数据模型名称是否存在

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_validate_model_name
        @apiDescription 判断数据模型名称是否存在

        @apiParam {String} model_name 模型名称

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        is_model_name_existed = DataModelManager.validate_model_name(params)
        return Response(is_model_name_existed)

    @params_valid(serializer=DataModelUpdateSerializer)
    def update(self, request, model_id, params):
        """
        @api {put} /datamanage/datamodel/models/:model_id/ *修改数据模型

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_update
        @apiDescription 修改数据模型

        @apiParam {String} [model_alias] 模型别名
        @apiParam {String} [description] 模型描述
        @apiParam {List} [tags] 标签
        @apiParam {String} bk_username 用户名

        @apiParamExample {json} 参数样例:
        {
            "model_alias": "道具流水表",
            "description": "道具流水",
            "tags": [
                {
                    "tag_code":"common_dimension",
                    "tag_alias":"公共维度"
                },{
                    "tag_code":"",
                    "tag_alias":"自定义标签名称"
                }
            ],
            "bk_username": "xx"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_id": 1,
                    "model_name": "fact_item_flow",
                    "model_alias": "道具流水表",
                    "model_type": "fact_table",
                    "description": "道具流水",
                    "tags": [
                        {
                            "tag_code":"common_dimension",
                            "tag_alias":"公共维度"
                        },{
                            "tag_code":"",
                            "tag_alias":"自定义标签名称"
                        }
                    ],
                    "table_name": "fact_item_flow",
                    "table_alias": "道具流水表",
                    "project_id": 4172,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "publish_status":"developing",
                    "active_status":"active",
                    "step_id": 1
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        check_perm("datamodel.update", model_id)
        # 修改数据模型
        datamodel_dict = DataModelManager.update_data_model(model_id, params, bk_username)
        return Response(datamodel_dict)

    def delete(self, request, model_id):
        """
        @api {delete} /datamanage/datamodel/models/:model_id/ *删除数据模型(软删除)

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_delete
        @apiDescription 删除数据模型

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_id": 1,
                    "model_name": "fact_item_flow",
                    "model_alias": "道具流水表",
                    "model_type": "fact_table",
                    "description": "道具流水",
                    "tags": [
                        {
                            "tag_code":"common_dimension",
                            "tag_alias":"公共维度"
                        },{
                            "tag_code":"",
                            "tag_alias":"自定义标签名称"
                        }
                    ],
                    "table_name": "fact_item_flow",
                    "table_alias": "道具流水表",
                    "project_id": 4172,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "publish_status":"developing",
                    "active_status":"disabled"
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        check_perm("datamodel.delete", model_id)
        # 软删除数据模型
        datamodel_dict = DataModelManager.delete_data_model(model_id, bk_username)
        return Response(datamodel_dict)

    @params_valid(serializer=DataModelListSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/datamodel/models/ *数据模型列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_list
        @apiDescription 数据模型列表

        @apiParam {Int} [project_id] 项目id
        @apiParam {String} [model_type] 模型类型,事实表模型/维度表模型
        @apiParam {Int} [model_id] 模型id
        @apiParam {String} [model_name] 模型名称
        @apiParam {String} [keyword] 搜索关键字，支持模型名称/模型别名/模型描述/标签名称/标签别名
        @apiParam {String} bk_username 用户名

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {String} data.model_type 模型类型,fact_table/dimension_table
        @apiSuccess (返回) {String} data.description 模型描述
        @apiSuccess (返回) {List} data.tags 标签列表，包含标签名称和标签别名
        @apiSuccess (返回) {String} data.table_name 主表名称
        @apiSuccess (返回) {String} data.table_alias 主表别名
        @apiSuccess (返回) {Int} data.project_id 项目id
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {String} data.publish_status 发布状态
        @apiSuccess (返回) {String} data.active_status 可用状态
        @apiSuccess (返回) {String} data.step_id 模型构建&发布完成步骤
        @apiSuccess (返回) {Int} data.applied_count 应用数量
        @apiSuccess (返回) {Boolean} data.sticky_on_top 模型是否置顶
        @apiSuccess (返回) {Boolean} data.is_instantiated 模型是否被实例化
        @apiSuccess (返回) {Boolean} data.is_quoted 模型是否被引用
        @apiSuccess (返回) {Boolean} data.is_related 模型是否被关联
        @apiSuccess (返回) {Boolean} data.can_be_deleted 模型是否可以被删除

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":[
                    {
                        "model_id":1,
                        "model_name":"fact_item_flow",
                        "model_alias":"道具流水表",
                        "model_type":"fact_table",
                        "description":"道具流水",
                        "tags": [
                            {
                                "tag_alias": "登出",
                                "tag_code": "logout"
                            }
                        ],
                        "table_name":"fact_item_flow",
                        "table_alias":"道具流水表",
                        "project_id":4172,
                        "created_by":"admin",
                        "created_at":"2020-10-18 15:38:56",
                        "updated_by":"admin",
                        "updated_at":"2020-10-18 15:38:56",
                        "publish_status":"developing",
                        "active_status":"active",
                        "step_id": 1,
                        "applied_count":2,
                        "sticky_on_top": true,
                        "is_instantiated": False,
                        "is_related": False,
                        "is_quoted": True,
                        "can_be_deleted": False
                    }
                ]
            }
        """
        datamodel_list = DataModelManager.get_data_model_list(params)
        return Response(datamodel_list)

    @detail_route(methods=["get"], url_path="dimension_models/can_be_related")
    @params_valid(serializer=RelatedDimensionModelListSerializer)
    def get_dim_model_list_can_be_related(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/dimension_models/can_be_related/ 可以关联的维度模型列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_dimension_models_can_be_related
        @apiDescription 可以关联的维度模型列表

        @apiParam {Int} [model_id] 模型id
        @apiParam {Int} [related_model_id] 关联模型id，用于前端点击关联模型设置回填

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {Boolean} data.has_extended_fields 模型下除主键和时间字段以外是否有其他维度字段

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":[
                    {
                        "model_id":1,
                        "model_name":"fact_item_flow",
                        "model_alias":"道具流水表",
                        "has_extended_fields": True
                    }
                ]
            }
        """
        related_model_id = params["related_model_id"]
        published = params["published"]
        dmm_model_list = DataModelManager.get_dim_model_list_can_be_related(model_id, related_model_id, published)
        return Response(dmm_model_list)

    @detail_route(methods=["get"], url_path="info")
    @params_valid(serializer=DataModelInfoSerializer)
    def info(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/info/ *数据模型详情

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_info
        @apiDescription 数据模型详情

        @apiParam {List} [with_details] 展示模型主表字段、模型关联关系、统计口径、指标等详情,
        取值['release_info', 'master_table', 'calculation_atoms', 'indicators']

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {String} data.table_name 主表名称
        @apiSuccess (返回) {String} data.table_alias 主表别名
        @apiSuccess (返回) {String} data.model_type 模型类型：fact_table/dimension_table
        @apiSuccess (返回) {String} data.description 模型描述
        @apiSuccess (返回) {List} data.tags 标签，例如[{"tag_alias": "道具", "tag_code": "props"}]
        @apiSuccess (返回) {Int} data.project_id 项目id
        @apiSuccess (返回) {String} data.active_status 可用状态：developing/published/re-developing
        @apiSuccess (返回) {String} data.publish_status 发布状态：active/disabled/conflicting
        @apiSuccess (返回) {Int} data.step_id 模型构建&发布完成步骤
        @apiSuccess (返回) {Int} data.applied_count 应用数量
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {String} data.version_log 发布描述
        @apiSuccess (返回) {String} data.release_created_by 发布者
        @apiSuccess (返回) {String} data.release_created_at 发布时间
        @apiSuccess (返回) {Json} data.model_detail 模型主表、统计口径和指标等详情,当with_details非空时展示
        @apiSuccess (返回) {List} data.model_detail.fields 主表字段列表
        @apiSuccess (返回) {Int} data.model_detail.fields.id 字段ID
        @apiSuccess (返回) {Int} data.model_detail.fields.model_id 模型ID
        @apiSuccess (返回) {String} data.model_detail.fields.field_name 字段名称
        @apiSuccess (返回) {String} data.model_detail.fields.field_alias 字段别名
        @apiSuccess (返回) {Int} data.model_detail.fields.field_index 字段位置
        @apiSuccess (返回) {String} data.model_detail.fields.field_type 数据类型
        @apiSuccess (返回) {String} data.model_detail.fields.field_category 字段类型：measure/dimension
        @apiSuccess (返回) {String} data.model_detail.fields.is_primary_key 是否主键：True/False
        @apiSuccess (返回) {String} data.model_detail.fields.description 字段描述
        @apiSuccess (返回) {List} data.model_detail.fields.field_constraint_content 字段约束内容,例如：
        {
            "op": "OR",
            "groups": [
                {
                    "op": "AND",
                    "items": [
                        {"constraint_id": "", "constraint_content": ""},
                        {"constraint_id": "", "constraint_content": ""}
                    ]
                },
                {
                    "op": "OR",
                    "items": [
                        {"constraint_id": "", "constraint_content": ""},
                        {"constraint_id": "", "constraint_content": ""}
                    ]
                }
           ]
        }
        @apiSuccess (返回) {Json} data.model_detail.fields.field_clean_content 清洗规则，例如：
        {
            "clean_option":"SQL",
            "clean_content":"price * 100 as price"
        }
        @apiSuccess (返回) {List} data.model_detail.fields.origin_fields 计算来源字段，例如['price']
        @apiSuccess (返回) {Int} data.model_detail.fields.source_model_id 拓展字段来源模型id
        @apiSuccess (返回) {String} data.model_detail.fields.source_field_name 拓展字段来源模型字段
        @apiSuccess (返回) {Boolean} data.model_detail.fields.is_join_field 是否主表关联字段
        @apiSuccess (返回) {Boolean} data.model_detail.fields.is_extended_field 是否扩展字段
        @apiSuccess (返回) {String} data.model_detail.fields.join_field_name 扩展字段对应的主表关联字段
        @apiSuccess (返回) {List} data.model_detail.model_relation 主表关联关系
        @apiSuccess (返回) {Int} data.model_detail.model_relation.model_id 主表模型ID
        @apiSuccess (返回) {String} data.model_detail.model_relation.field_name 主表关联字段
        @apiSuccess (返回) {Int} data.model_detail.model_relation.related_model_id 关联维度模型ID
        @apiSuccess (返回) {String} data.model_detail.model_relation.related_field_name 关联维度模型关联字段
        @apiSuccess (返回) {String} data.model_detail.model_relation.related_method 关联维度模型关联方法
        @apiSuccess (返回) {Int} data.model_detail.calculation_atoms 统计口径列表
        @apiSuccess (返回) {Int} data.model_detail.calculation_atoms.model_id 创建统计口径的模型ID
        @apiSuccess (返回) {Int} data.model_detail.calculation_atoms.project_id 创建统计口径的项目ID
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.calculation_atom_alias 统计口径中文名
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.calculation_atom_type 统计口径类型：create/quote
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.description 统计口径描述
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.field_type 统计口径字段类型
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.calculation_content 统计方式，例如
        表单提交示例：
        {
            'option': 'TABLE',
            'content': {
                'calculation_field': 'price',
                'calculation_function': 'sum'
            }
        }
        SQL提交示例：
        {
            'option': 'SQL',
            'content': {
                'calculation_formula': 'sum(price)'
            }
        }
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.calculation_formula 统计SQL
        @apiSuccess (返回) {String} data.model_detail.calculation_atoms.origin_fields 统计口径计算来源字段
        @apiSuccess (返回) {Boolean} data.model_detail.calculation_atoms.editable 统计口径能否编辑
        @apiSuccess (返回) {Boolean} data.model_detail.calculation_atoms.deletable 统计口径能否删除
        @apiSuccess (返回) {List} data.model_detail.indicators 指标列表
        @apiSuccess (返回) {String} data.model_detail.indicators.indicator_name 指标名称
        @apiSuccess (返回) {String} data.model_detail.indicators.indicator_alias 指标中文名
        @apiSuccess (返回) {String} data.model_detail.indicators.description 指标描述
        @apiSuccess (返回) {String} data.model_detail.indicators.calculation_atom_name 指标统计口径
        @apiSuccess (返回) {List} data.model_detail.indicators.aggregation_fields 指标聚合字段，例如['channel_name']
        @apiSuccess (返回) {List} data.model_detail.indicators.aggregation_fields_alias 指标聚合字段中文名,['大区名称']
        @apiSuccess (返回) {String} data.model_detail.indicators.filter_formula 指标过滤条件
        @apiSuccess (返回) {String} data.model_detail.indicators.scheduling_type 指标调度类型：stream/batch
        @apiSuccess (返回) {Json} data.model_detail.indicators.scheduling_content 指标调度内容，详见dataflow文档
        离线参数示例：
        {
            "window_type": "fixed",
            "count_freq": 1,
            "schedule_period": "day",
            "fixed_delay": 0,
            "dependency_config_type": "unified",
            "unified_config":{
                "window_size": 1,
                "window_size_period": "day",
                "dependency_rule": "all_finished"
            },
            "advanced":{
                "recovery_times":3,
                "recovery_enable":false,
                "recovery_interval":"60m"
            }
        }
        实时参数示例：
        {
            "window_type":"scroll",
            "window_lateness":{
                "allowed_lateness":true,
                "lateness_count_freq":60,
                "lateness_time":6
            },
            "window_time":1440,
            "count_freq":30,
            "waiting_time":0
        }
        @apiSuccess (返回) {String} data.model_detail.indicators.parent_indicator_name 父指标名称

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "model_id":1,
                    "model_name":"fact_item_flow",
                    "model_alias":"道具流水表",
                    "table_name":"fact_item_flow",
                    "table_alias":"道具流水表",
                    "model_type":"fact_table",
                    "description":"道具流水",
                    "tags":[
                        {
                            "tag_code":"common_dimension",
                            "tag_alias":"公共维度"
                        },{
                            "tag_code":"",
                            "tag_alias":"自定义标签名称"
                        }
                    ],
                    "project_id":3,
                    "active_status":"active",
                    "publish_status":"developing",
                    "step_id": 1,
                    "applied_count":2,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "model_detail":{
                        "fields":[
                            {
                                "id":1,
                                "field_name":"price",
                                "field_alias":"道具价格",
                                "field_index":1,
                                "field_type":"long",
                                "field_category":"metric",
                                "description":"道具价格",
                                "field_constraint_content":[
                                    {"content": {"constraint_id": "gt", "constraint_content": "0"}}
                                ],
                                "field_clean_content":{
                                    "clean_option":"SQL",
                                    "clean_content":"price * 100 as price"
                                },
                                "origin_fields":[
                                    "price"
                                ],
                                "source_model_id":null,
                                "source_field_name":null,
                                "is_join_field": false,
                                "is_extended_field": false,
                                "join_field_name": null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ],
                        "model_relation":[
                            {
                                "model_id":1,
                                "field_name":"channel_id",
                                "related_model_id":2,
                                "related_field_name":"channel_id",
                                "related_method":"left-join"
                            }
                        ],
                        "calculation_atoms":[
                            {
                                "model_id":1,
                                "project_id":3,
                                "calculation_atom_name":"item_sales_amt",
                                "calculation_atom_alias":"item_sales_amt",
                                "calculation_atom_type":"create",
                                "description":"item_sales_amt",
                                "field_type":"long",
                                "calculation_content":{
                                    "option":"TABLE",
                                    "content":{
                                        "calculation_field":"price",
                                        "calculation_function":"sum"
                                    }
                                },
                                "calculation_formula":"sum(price)",
                                "origin_fields":[
                                    "price"
                                ],
                                "editable":true,
                                "deletable":false,
                                "quoted_count":0,
                                "indicator_count":1,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ],
                        "indicators":[
                            {
                                "model_id":1,
                                "project_id":3,
                                "indicator_name":"item_sales_amt_china_1d",
                                "indicator_alias":"国内每天按大区统计道具销售额",
                                "description":"国内每天按大区统计道具销售额",
                                "calculation_atom_name":"item_sales_amt",
                                "aggregation_fields":[
                                    "channel_name"
                                ],
                                "aggregation_fields_alias":[
                                    "渠道号"
                                ],
                                "filter_formula":"os='android'",
                                "scheduling_content":{
                                    "window_type":"scroll",
                                    "window_lateness":{
                                        "allowed_lateness":true,
                                        "lateness_count_freq":60,
                                        "lateness_time":6
                                    },
                                    "window_time":1440,
                                    "count_freq":30,
                                    "waiting_time":0
                                },
                                "parent_indicator_name":null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ]
                    }
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        datamodel_dict = DataModelManager.get_data_model_info(model_id, params)
        return Response(datamodel_dict)

    @detail_route(methods=["get"], url_path="latest_version/info")
    @params_valid(serializer=DataModelInfoSerializer)
    def latest_version_info(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/latest_version/info/ *数据模型最新发布版本详情

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_latest_version_info
        @apiDescription 数据模型最新发布版本详情

        @apiParam {List} [with_details] 展示统计口径 & 指标在草稿态中是否存在, 取值['existed_in_stage']

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {String} data.table_name 主表名称
        @apiSuccess (返回) {String} data.table_alias 主表别名
        @apiSuccess (返回) {String} data.model_type 模型类型：fact_table/dimension_table
        @apiSuccess (返回) {String} data.description 模型描述
        @apiSuccess (返回) {List} data.tags 标签，例如[{"tag_alias": "道具", "tag_code": "props"}]
        @apiSuccess (返回) {Int} data.project_id 项目id
        @apiSuccess (返回) {String} data.active_status 可用状态：developing/published/re-developing
        @apiSuccess (返回) {String} data.publish_status 发布状态：active/disabled/conflicting
        @apiSuccess (返回) {Int} data.step_id 模型构建&发布完成步骤
        @apiSuccess (返回) {Int} data.applied_count 应用数量
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {String} data.version_log 发布描述
        @apiSuccess (返回) {String} data.release_created_by 发布者
        @apiSuccess (返回) {String} data.release_created_at 发布时间
        @apiSuccess (返回) {Json} data.model_detail 模型主表、统计口径和指标等详情
        @apiSuccess (返回) {List} data.model_detail.fields 主表字段列表
        @apiSuccess (返回) {List} data.model_detail.model_relation 主表关联关系
        @apiSuccess (返回) {Int} data.model_detail.calculation_atoms 统计口径列表
        @apiSuccess (返回) {List} data.model_detail.indicators 指标列表
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        datamodel_dict = DataModelManager.get_data_model_latest_version_info(model_id, params["with_details"])
        return Response(datamodel_dict)

    @detail_route(methods=["post"], url_path="release")
    @params_valid(serializer=DataModelReleaseSerializer)
    def release(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/release/ 数据模型发布

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_release
        @apiDescription 数据模型发布

        @apiParam {String} version_log 发布描述

        @apiParamExample {json} 参数样例:
        {
            "version_log": "道具流水表发布"
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
        """
        bk_username = get_request_username()
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.update", model_id)
        datamodel_release_dict = DataModelManager.release_data_model(model_id, params["version_log"], bk_username)
        return Response(datamodel_release_dict)

    @detail_route(methods=["get"], url_path="release_list")
    def release_list(self, request, model_id):
        """
        @api {get} /datamanage/datamodel/models/:model_id/release_list/ 数据模型发布列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_release_list
        @apiDescription 数据模型发布列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "results": [
                        {
                            "created_at": "2020-11-26 00:28:32",
                            "version_log": "道具流水模型发布1.0.0",
                            "created_by": "admin"
                            "version_id": "xxxxx"
                        }
                    ]
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        datamodel_release_list = DataModelManager.get_data_model_release_list(model_id)
        return Response({"results": datamodel_release_list})

    @detail_route(methods=["get"], url_path="overview")
    @params_valid(serializer=DataModelOverviewSerializer)
    def overview(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/overview/ *数据模型预览

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_overview
        @apiDescription 数据模型预览,用于模型预览树形结构展示

        @apiParam {Boolean} [latest_version] 是否返回模型最新发布版本预览信息

        @apiSuccess (返回) {Int} data.nodes 节点，包括维表、主表、统计口径、指标，不同类型用node_type区分
        @apiSuccess (返回) {String} data.lines 边

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "nodes":[
                        {
                            "node_type":"fact_table",
                            "node_id":"fact_table-fact_item_flow",
                            "model_id":1,
                            "model_name":"fact_item_flow",
                            "model_alias":"道具流水表",
                            "model_type":"fact_table",
                            "description":"道具流水",
                            "tags":[
                                {
                                    "tag_code":"common_dimension",
                                    "tag_alias":"公共维度"
                                },{
                                    "tag_code":"",
                                    "tag_alias":"自定义标签名称"
                                }
                            ],
                            "table_name":"fact_item_flow",
                            "table_alias":"道具流水表",
                            "project_id":4172,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56",
                            "publish_status":"developing",
                            "active_status":"active",
                            "applied_count":2,
                            "model_detail":{
                                "fields":[
                                    {
                                        "field_id":1,
                                        "field_name":"price",
                                        "field_alias":"道具价格",
                                        "field_index":1,
                                        "field_type":"long",
                                        "field_category":"metric",
                                        "description":"道具价格",
                                        "field_constraint_content":null,
                                        "field_clean_content":{
                                            "clean_option":"SQL",
                                            "clean_content":"price * 100 as price"
                                        },
                                        "source_model_id":null,
                                        "source_field_name":null,
                                        "is_join_field":false,
                                        "is_extended_field":false,
                                        "join_field_name":null,
                                        "created_by":"admin",
                                        "created_at":"2020-10-18 15:38:56",
                                        "updated_by":"admin",
                                        "updated_at":"2020-10-18 15:38:56"
                                    },
                                    {
                                        "field_id":2,
                                        "field_name":"channel_id",
                                        "field_alias":"渠道号",
                                        "field_index":2,
                                        "field_type":"string",
                                        "field_category":"dimension",
                                        "description":"渠道号",
                                        "field_constraint_content":null,
                                        "field_clean_content":null,
                                        "source_model_id":null,
                                        "source_field_name":null,
                                        "is_join_field":true,
                                        "is_extended_field":false,
                                        "join_field_name":null,
                                        "created_by":"admin",
                                        "created_at":"2020-10-18 15:38:56",
                                        "updated_by":"admin",
                                        "updated_at":"2020-10-18 15:38:56"
                                    }
                                ]
                            }
                        },
                        {
                            "node_type":"dimension_table",
                            "node_id":"dimension_table-dm_channel",
                            "model_id":2,
                            "model_name":"dm_channel",
                            "model_alias":"渠道表",
                            "model_type":"dimension_table",
                            "description":"渠道表",
                            "tags":[
                                {
                                    "tag_code":"common_dimension",
                                    "tag_alias":"公共维度"
                                },{
                                    "tag_code":"",
                                    "tag_alias":"自定义标签名称"
                                }
                            ],
                            "table_name":"dm_channel",
                            "table_alias":" 渠道表",
                            "project_id":4172,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56",
                            "publish_status":"developing",
                            "active_status":"active",
                            "applied_count":2,
                            "fields":[
                                {
                                    "field_id":3,
                                    "field_name":"channel_id",
                                    "field_alias":"渠道号",
                                    "field_index":1,
                                    "field_type":"string",
                                    "field_category":"dimension",
                                    "description":"渠道号",
                                    "field_constraint_content":null,
                                    "field_clean_content":null,
                                    "source_model_id":null,
                                    "source_field_name":null,
                                    "is_join_field":false,
                                    "is_extended_field":false,
                                    "join_field_name":null,
                                    "created_by":"admin",
                                    "created_at":"2020-10-18 15:38:56",
                                    "updated_by":"admin",
                                    "updated_at":"2020-10-18 15:38:56"
                                }
                            ]
                        },
                        {
                            "node_type":"calculation_atom",
                            "node_id":"calculation_atom-item_sales_amt",
                            "calculation_atom_name":"item_sales_amt",
                            "calculation_atom_alias":"item_sales_amt",
                            "description":"item_sales_amt",
                            "field_type":"long",
                            "calculation_content":{
                                "option":"table",
                                "content":{
                                    "calculation_field":"price",
                                    "calculation_function":"sum"
                                }
                            },
                            "calculation_formula":"sum(price)",
                            "indicator_count":2,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56"
                        },
                        {
                            "node_type":"indicator",
                            "node_id":"indicator-item_sales_amt",
                            "model_id":1,
                            "indicator_name":"item_sales_amt_china_1d",
                            "indicator_alias":"国内每天按大区统计道具销售额",
                            "description":"国内每天按大区统计道具销售额",
                            "calculation_atom_name":"item_sales_amt",
                            "aggregation_fields":[
                                "channel_name"
                            ],
                            "filter_formula":"os='android'",
                            "scheduling_content":{},
                            "parent_indicator_name":null,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56"
                        }
                    ],
                    "lines":[
                        {
                            "from":"dimension_table-dm_channel",
                            "to":"fact_table_fact-item_flow",
                            "from_field_name": "channel_id",
                            "to_field_name": "channel_id",
                        },
                        {
                            "from":"fact_table_fact-item_flow",
                            "to":"calculation_atom-item_sales_amt"
                        },
                        {
                            "from":"calculation_atom-item_sales_amt",
                            "to":"indicator-item_sales_amt"
                        }
                    ]
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        datamodel_overview_dict = DataModelManager.get_data_model_overview_info(
            model_id, latest_version=params["latest_version"]
        )
        return Response(datamodel_overview_dict)

    @detail_route(methods=["get"], url_path="diff")
    @params_valid(serializer=DataModelDiffSerializer)
    def diff(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/diff/ 数据模型变更内容

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_diff
        @apiDescription 数据模型变更内容, 用模型当前内容和dmm_model_release的latest版本的model_content作diff

        @apiParam {String} [orig_version_id] 源版本ID
        @apiParam {String} [new_version_id] 目标版本ID

        @apiSuccess (返回) {Json} data.orig_contents 源版本
        @apiSuccess (返回) {Json} data.new_content 当前版本
        @apiSuccess (返回) {Json} data.diff 变更内容
        @apiSuccess (返回) {Json} data.diff.diff_result 变更结论
        @apiSuccess (返回) {Int} data.diff.diff_result.create 新增数目
        @apiSuccess (返回) {Int} data.diff.diff_result.update 变更数目
        @apiSuccess (返回) {Int} data.diff.diff_result.delete 删除数目
        @apiSuccess (返回) {Int} data.diff.diff_result.field 字段变更结论
        @apiSuccess (返回) {Int} data.diff.diff_result.field.create 字段新增数目
        @apiSuccess (返回) {Int} data.diff.diff_result.field.update 字段变更数目
        @apiSuccess (返回) {Int} data.diff.diff_result.field.delete 字段删除数目
        @apiSuccess (返回) {Int} data.diff.diff_result.field.field_index_update 字段顺序变更数目
        @apiSuccess (返回) {List} data.diff.diff_objects 变更对象
        @apiSuccess (返回) {String} data.diff.diff_objects.object_type 对象类型
        @apiSuccess (返回) {String} data.diff.diff_objects.object_id 对象ID
        @apiSuccess (返回) {String} data.diff.diff_objects.diff_type 对象变更类型
        @apiSuccess (返回) {List} [data.diff.diff_objects.diff_keys] 变更内容对应的keys
        @apiSuccess (返回) {List} [data.diff.diff_objects.diff_objects] 变更字段列表
        @apiSuccess (返回) {String} data.diff.diff_objects.diff_objects.object_type 对象类型
        @apiSuccess (返回) {String} data.diff.diff_objects.diff_objects.object_id 对象ID
        @apiSuccess (返回) {String} data.diff.diff_objects.diff_objects.diff_type 对象变更类型
        @apiSuccess (返回) {List} [data.diff.diff_objects.diff_objects.diff_keys] 变更内容对应的keys

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "orig_contents": {
                        "created_at": "admin",
                        "created_by": "2020-12-11 15:41:28",
                        "objects": [
                            {
                                "object_type": "master_table",
                                "object_id": "fact_table-fact_item_flow",
                                "fields": [
                                    {
                                        "object_type": "field",
                                        "object_id": "field-price",
                                        "field_name":"price",
                                        "field_alias":"道具价格",
                                        "field_index":1,
                                        "field_type":"long",
                                        "field_category":"metric",
                                        "description":"道具价格",
                                        "field_constraint_content":null,
                                        "field_clean_content":{
                                            "clean_option":"SQL",
                                            "clean_content":"price * 100 as price"
                                        },
                                        "source_model_id":null,
                                        "source_field_name":null,
                                        "is_join_field":false,
                                        "is_extended_field":false,
                                        "join_field_name":null,
                                        "created_by":"admin",
                                        "created_at":"2020-10-18 15:38:56",
                                        "updated_by":"admin",
                                        "updated_at":"2020-10-18 15:38:56"
                                    }
                                ]
                            },
                            {
                                "object_type": "calculation_atom",
                                "object_id": "calculation_atom-item_sales_amt",
                                "calculation_atom_name":"item_sales_amt",
                                "calculation_atom_alias":"item_sales_amt",
                                "description":"item_sales_amt",
                                "field_type":"long",
                                "calculation_content":{
                                    "option":"table",
                                    "content":{
                                        "calculation_field":"price",
                                        "calculation_function":"sum"
                                    }
                                },
                                "calculation_formula":"sum(price)",
                                "indicator_count":2,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ]
                    },
                    "new_contents": {
                        "created_at": "admin",
                        "created_by": "2020-12-11 15:41:28",
                        "objects": [
                            {
                                "object_type": "master_table",
                                "object_id": "fact_table-fact_item_flow",
                                "fields": [
                                    {
                                        "object_type": "field",
                                        "object_id": "field-price",
                                        "field_name":"price",
                                        "field_alias":"道具价格",
                                        "field_index":1,
                                        "field_type":"long",
                                        "field_category":"metric",
                                        "description":"道具价格1",
                                        "field_constraint_content":null,
                                        "field_clean_content":{
                                            "clean_option":"SQL",
                                            "clean_content":"price * 100 as price"
                                        },
                                        "source_model_id":null,
                                        "source_field_name":null,
                                        "is_join_field":false,
                                        "is_extended_field":false,
                                        "join_field_name":null,
                                        "created_by":"admin",
                                        "created_at":"2020-10-18 15:38:56",
                                        "updated_by":"admin",
                                        "updated_at":"2020-10-18 15:38:56"
                                    }
                                ]
                            },
                            {
                                "object_type": "indicator",
                                "object_id": "indicator-item_sales_amt",
                                "indicator_name":"item_sales_amt_china_1d",
                                "indicator_alias":"国内每天按大区统计道具销售额",
                                "description":"国内每天按大区统计道具销售额",
                                "calculation_atom_name":"item_sales_amt",
                                "aggregation_fields":[
                                    "channel_name"
                                ],
                                "filter_formula":"os='android'",
                                "scheduling_content":{
                                    "window_type":"fixed",
                                    "count_freq":1,
                                    "schedule_period":"day",
                                    "fixed_delay":0,
                                    "fallback_window":1
                                },
                                "parent_indicator_name":null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ]
                    },
                    "diff": {
                        "diff_result": {
                            "create": 1,
                            "update": 1,
                            "delete": 1,
                            "field": {
                                "field_index_update": 2,
                                "create": 1,
                                "update": 2,
                                "delete": 0
                            }
                        },
                        "diff_objects": [
                            {
                                "object_type": "master_table",
                                "object_id": "master_table-fact_item_flow",
                                "diff_type": "update",
                                "diff_objects":[
                                    {
                                        "object_type": "field",
                                        "object_id": "field-price",
                                        "diff_type": "update",
                                        "diff_keys": ["description"]
                                    }
                                ]
                            },
                            {
                                "object_type": "calculation_atom",
                                "object_id": "calculation_atom-item_sales_amt",
                                "diff_type": "delete",
                                "diff_keys": ["description"]
                            },
                            {
                                "object_type": "indicator",
                                "object_id": "indicator-item_sales_amt",
                                "diff_type": "create",
                                "diff_keys": ["description"]
                            }
                        ]
                    }
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        orig_version_id = params["orig_version_id"]
        new_version_id = params["new_version_id"]
        # 模型两个指定版本间diff
        if orig_version_id or new_version_id:
            diff_dict = DataModelManager.diff_data_model_version_content(model_id, orig_version_id, new_version_id)
        # 模型上一个发布版本内容 和 当前内容diff
        else:
            diff_dict = DataModelManager.diff_data_model(model_id)
        return Response(diff_dict)

    @detail_route(methods=["get"], url_path="export")
    def export_datamodel(self, request, model_id):
        """
        @api {get} /datamanage/datamodel/models/:model_id/export/ 导出模型

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_export
        @apiDescription 导出模型

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {Int} data.project_id 项目id
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {String} data.model_type 模型类型
        @apiSuccess (返回) {String} data.description 模型描述
        @apiSuccess (返回) {String} data.table_name 主表名称
        @apiSuccess (返回) {String} data.table_alias 主表别名
        @apiSuccess (返回) {String} data.publish_status 发布状态
        @apiSuccess (返回) {String} data.active_status 可用状态
        @apiSuccess (返回) {List} data.tags 标签
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {Int} data.applied_count 应用数量
        @apiSuccess (返回) {Json} data.model_detail 模型主表、统计口径等详情
        @apiSuccess (返回) {Json} data.model_detail.fields 主表字段信息
        @apiSuccess (返回) {Json} data.model_detail.model_relation 模型关联关系
        @apiSuccess (返回) {Json} data.model_detail.calculation_atoms 模型统计口径
        @apiSuccess (返回) {Json} data.model_detail.indicators 模型指标
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "model_id":1,
                    "model_name":"fact_item_flow",
                    "model_alias":"道具流水表",
                    "model_type":"fact_table",
                    "description":"道具流水",
                    "tags":[
                        {
                            "tag_code":"common_dimension",
                            "tag_alias":"公共维度"
                        },{
                            "tag_code":"",
                            "tag_alias":"自定义标签名称"
                        }
                    ],
                    "table_name":"fact_item_flow",
                    "table_alias":"道具流水表",
                    "project_id":4172,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "publish_status":"developing",
                    "active_status":"active",
                    "applied_count":2,
                    "model_detail":{
                        "fields":[
                            {
                                "field_id":1,
                                "field_name":"price",
                                "field_alias":"道具价格",
                                "field_index":1,
                                "field_type":"long",
                                "field_category":"metric",
                                "description":"道具价格",
                                "field_constraint_content":null,
                                "field_clean_content":{
                                    "clean_option":"SQL",
                                    "clean_content":"price * 100 as price"
                                },
                                "source_model_id":null,
                                "source_field_name":null,
                                "is_join_field": false,
                                "is_extended_field": false,
                                "join_field_name": null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            },
                            {
                                "field_id":2,
                                "field_name":"channel_id",
                                "field_alias":"渠道号",
                                "field_index":2,
                                "field_type":"string",
                                "field_category":"dimension",
                                "description":"渠道号",
                                "field_constraint_content":null,
                                "field_clean_content":null,
                                "source_model_id":null,
                                "source_field_name":null,
                                "is_join_field": true,
                                "is_extended_field": false,
                                "join_field_name": null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ],
                        "model_relation":[
                            {
                                "model_id":1,
                                "field_name":"channel_id",
                                "related_model_id":2,
                                "related_field_name":"channel_id",
                                "related_method":"left-join"
                            }
                        ],
                        "calculation_atoms":[
                            {
                                "calculation_atom_name":"item_sales_amt",
                                "calculation_atom_alias":"item_sales_amt",
                                "description":"item_sales_amt",
                                "field_type":"long",
                                "calculation_content":{
                                    "option":"table",
                                    "content":{
                                        "calculation_field":"price",
                                        "calculation_function":"sum"
                                    }
                                },
                                "calculation_formula":"sum(price)",
                                "indicator_count":2,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ],
                        "indicators":[
                            {
                                "model_id":1,
                                "indicator_name":"item_sales_amt_china_1d",
                                "indicator_alias":"国内每天按大区统计道具销售额",
                                "description":"国内每天按大区统计道具销售额",
                                "calculation_atom_name":"item_sales_amt",
                                "aggregation_fields":[
                                    "channel_name"
                                ],
                                "filter_formula":"os='android'",
                                "scheduling_content":{},
                                "parent_indicator_name":null,
                                "created_by":"admin",
                                "created_at":"2020-10-18 15:38:56",
                                "updated_by":"admin",
                                "updated_at":"2020-10-18 15:38:56"
                            }
                        ]
                    }
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        datamodel_dict = DataModelManager.get_data_model_info(
            model_id,
            {"with_details": ["master_table", "calculation_atoms", "indicators"]},
        )
        return Response(datamodel_dict)

    @list_route(methods=["post"], url_path="import")
    @params_valid(serializer=DataModelImportSerializer)
    def import_datamodel(self, request, params):
        """
        @api {post} /datamanage/datamodel/models/import/ 导入模型
        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_import
        @apiDescription 导入模型

        @apiParamExample {json} 参数样例:
        {
            "model_name":"fact_item_flow_import",
            "project_id":3,
            "model_alias":"道具流水表",
            "model_type":"fact_table",
            "description":"模型描述",
            "tags":[
                {
                    "tag_code":"props",
                    "tag_alias":"道具"
                }
            ],
            "model_detail":{
                "indicators":[
                    {
                        "indicator_name":"sum_props_price_180s",
                        "indicator_alias":"指标中文名",
                        "description":"指标描述",
                        "calculation_atom_name":"sum_props_price",
                        "aggregation_fields":[
                        ],
                        "filter_formula":"-- WHERE 之后的语句",
                        "scheduling_type":"stream",
                        "scheduling_content":{
                            "window_type":"scroll",
                            "count_freq":180,
                            "format_window_size":180,
                            "window_lateness":{
                                "allowed_lateness":false,
                                "lateness_count_freq":60,
                                "lateness_time":1
                            },
                            "window_time":1440,
                            "expired_time":0,
                            "format_window_size_unit":"s",
                            "session_gap":0,
                            "waiting_time":0
                        }
                    }
                ],
                "fields":[
                    {
                        "field_name":"id",
                        "field_alias":"主键id",
                        "field_index":1,
                        "field_type":"int",
                        "field_category":"dimension",
                        "is_primary_key":false,
                        "description":"主键id",
                        "field_constraint_content":null,
                        "field_clean_content":null,
                        "source_model_id":null,
                        "source_field_name":null
                    },
                    {
                        "field_name":"price",
                        "field_alias":"渠道xxx",
                        "field_index":2,
                        "field_type":"int",
                        "field_category":"measure",
                        "is_primary_key":false,
                        "description":"价格",
                        "field_constraint_content":{
                            "groups":[
                                {
                                    "items":[
                                        {
                                            "constraint_id":"gte",
                                            "constraint_content":"0"
                                        },
                                        {
                                            "constraint_id":"gte",
                                            "constraint_content":"100"
                                        }
                                    ],
                                    "op":"AND"
                                },
                                {
                                    "items":[
                                        {
                                            "constraint_id":"not_null",
                                            "constraint_content":null
                                        }
                                    ],
                                    "op":"AND"
                                }
                            ],
                            "op":"AND"
                        },
                        "field_clean_content":{
                            "clean_option":"SQL",
                            "clean_content":"price as price"
                        },
                        "source_model_id":null,
                        "source_field_name":null
                    },
                    {
                        "field_name":"channel_id",
                        "field_alias":"渠道号",
                        "field_index":12,
                        "field_type":"string",
                        "field_category":"dimension",
                        "is_primary_key":false,
                        "description":"渠道号",
                        "field_constraint_content":{
                            "groups":[
                                {
                                    "items":[
                                        {
                                            "constraint_id":"not_null",
                                            "constraint_content":null
                                        }
                                    ],
                                    "op":"AND"
                                }
                            ],
                            "op":"AND"
                        },
                        "field_clean_content":{
                            "clean_option":"SQL",
                            "clean_content":"channel_id as channel_id"
                        },
                        "source_model_id":null,
                        "source_field_name":null
                    },
                    {
                        "field_name":"channel_description",
                        "field_alias":"渠道描述",
                        "field_index":4,
                        "field_type":"string",
                        "field_category":"dimension",
                        "is_primary_key":false,
                        "description":"渠道描述",
                        "field_constraint_content":null,
                        "field_clean_content":null,
                        "source_model_id":67,
                        "source_field_name":"channel_description"
                    },
                    {
                        "field_name":"__time__",
                        "field_alias":"时间字段",
                        "field_index":5,
                        "field_type":"timestamp",
                        "field_category":"dimension",
                        "is_primary_key":false,
                        "description":"平台内置时间字段，数据入库后将装换为可查询字段，比如 dtEventTime/dtEventTimeStamp/localtime",
                        "field_constraint_content":null,
                        "field_clean_content":null,
                        "source_model_id":null,
                        "source_field_name":null

                    }
                ],
                "model_relation":[
                    {
                        "related_method":"left-join",
                        "related_model_id":67,
                        "field_name":"channel_id",
                        "related_field_name":"id"
                    }
                ],
                "calculation_atoms":[
                    {
                        "calculation_atom_name":"sum_props_price_calc",
                        "calculation_atom_alias":"统计口径中文名",
                        "description":"统计口径描述",
                        "field_type":"long",
                        "calculation_content":{
                            "content":{
                                "calculation_formula":"sum(price)+1+1+1"
                            },
                            "option":"SQL"
                        }
                    }
                ]
            }
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "model_id": 23
                }
        """
        bk_username = get_request_username()
        check_perm("datamodel.create", params["project_id"])
        # 创建数据模型
        datamodel_dict = DataModelManager.create_data_model(params, bk_username)
        # 判断是否有模型的修改权限
        check_perm("datamodel.update", datamodel_dict["model_id"])
        # 创建主表
        MasterTableManager.update_master_table(datamodel_dict["model_id"], params["model_detail"], bk_username)
        # 创建统计口径
        for calc_atom_dict in params["model_detail"]["calculation_atoms"]:
            calc_atom_dict["model_id"] = datamodel_dict["model_id"]
            if calc_atom_dict.get("calculation_atom_type", None) == CalculationAtomType.QUOTE:
                CalculationAtomManager.quote_calculation_atoms(
                    {
                        "model_id": datamodel_dict["model_id"],
                        "calculation_atom_names": [calc_atom_dict["calculation_atom_name"]],
                    },
                    bk_username,
                )
            else:
                CalculationAtomManager.create_calculation_atom(calc_atom_dict, bk_username)
        # 创建指标
        for indicator_dict in params["model_detail"]["indicators"]:
            indicator_dict["model_id"] = datamodel_dict["model_id"]
            IndicatorManager.create_indicator(indicator_dict, bk_username)
        return Response({"model_id": datamodel_dict["model_id"]})

    @detail_route(methods=["get"], url_path="operators")
    def operator_list(self, request, model_id):
        """
        @api {get} /datamanage/datamodel/models/:model_id/operators/ 操作者列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_operators
        @apiDescription 模型操作者列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "results": [
                        "admin1",
                        "admin2"
                    ]
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        return Response(OperationLogManager.get_operator_list(model_id))

    @detail_route(methods=["post"], url_path="operation_log")
    @params_valid(serializer=OperationLogListSerializer)
    def operation_log(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/operation_log/ 操作记录

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_operation_log
        @apiDescription 数据模型操作记录

        @apiParam {Json} [conditions] 搜索条件参数,object_operation操作类型，object_type操作对象类型，created_by操作者，object
        操作对象, 模糊搜索query
        @apiParam {String} [start_time] 启始时间
        @apiParam {String} [end_time] 终止时间
        @apiParam {Int} page 页码
        @apiParam {Int} page_size 每页条数
        @apiParam {String} [order_by_created_at] 按照操作时间排序 desc/asc
        @apiParam {String} bk_username 用户名

        @apiSuccess (返回) {Int} data.count 模型ID
        @apiSuccess (返回) {List} data.results 操作记录列表
        @apiSuccess (返回) {String} data.results.object_operation 操作类型
        @apiSuccess (返回) {String} data.results.object_type 操作对象类别
        @apiSuccess (返回) {String} data.results.object_name 操作对象中文名
        @apiSuccess (返回) {String} data.results.object_alias 操作对象英文名
        @apiSuccess (返回) {String} data.results.description 描述
        @apiSuccess (返回) {String} data.results.created_by 操作者
        @apiSuccess (返回) {String} data.results.created_at 操作时间
        @apiSuccess (返回) {String} data.results.id 操作id
        @apiSuccess (返回) {String} data.results.object_id 操作对象id

        @apiParamExample {json} 参数样例:
        {
            "conditions": [
                {"key":"object_operation","value":["create"]},
                {"key":"object_type","value":["model"]},
                {"key":"created_by","value":["admin"]},
                {"key":"query","value":["每日道具销售额"]},
            ],
            "page": 1,
            "page_size": 10,
            "start_time":"2021-01-06 00:00:45",
            "end_time":"2021-01-06 20:57:45",
            "bk_username": admin
        }

        @apiParamExample {json} conditions内容:
        {
            "object_operation": {
                "create": "新增",
                "update": "变更",
                "delete": "删除",
                "release": "发布"
            },
            "object_type": {
                "model": "数据模型",
                "master_table": "主表",
                "calculation_atom": "统计口径",
                "indicator": "指标"
            }
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "count": 10,
                    "results": [
                        {
                            "object_operation":"update",
                            "created_at":"2020-12-04 21:34:12",
                            "object_type":"master_table",
                            "object_id":"23",
                            "object_alias":"道具流水表",
                            "object_name":"fact_item_flow_15",
                            "created_by":"xx",
                            "id":10,
                            "description": null
                        }
                    ]
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        return Response(OperationLogManager.get_operation_log_list(model_id, params))

    @detail_route(methods=["get"], url_path=r"operation_log/(?P<operation_id>\w+)/diff")
    @params_valid(serializer=DataModelInfoSerializer)
    def operation_log_diff(self, request, model_id, operation_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/operation_log/:operation_id/diff  数据模型操作前后diff

        @apiVersion 3.5.0
        @apiGroup DataModel_OperationLog
        @apiName datamodel_operation_log_diff
        @apiDescription 数据模型操作记录diff

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "diff": {
                        "diff_objects": [
                            {
                                "diff_type": "update",
                                "object_type": "calculation_atom",
                                "diff_keys": [
                                    "calculation_content.content.calculation_formula",
                                    "calculation_formula"
                                ],
                                "object_id": "calculation_atom-item_sales_test5"
                            }
                        ]
                    },
                    "new_contents": {
                        "created_at": "2020-12-04 22:08:57",
                        "objects": [],
                        "created_by": "admin"
                    },
                    "orig_contents": {
                        "created_at": "2020-12-04 22:07:51",
                        "objects": [],
                        "created_by": "admin"
                    }
                },
                "result": true
            }
        """
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        operation_id = convert_to_number("operation_id", operation_id)
        return Response(OperationLogManager.diff_operation_log(operation_id))

    def applied(self, request, model_id):
        """
        @api {get} /datamanage/datamodel/models/:model_id/applied/ 模型应用列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_applied
        @apiDescription 数据模型应用列表

        @apiParam {Int} model_id 模型ID

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": []
            }
        """
        return Response(True)

    @detail_route(methods=["post"], url_path="top")
    @params_valid(serializer=BkUserNameSerializer)
    def top(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/top/ *模型置顶

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_top
        @apiDescription 模型置顶

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        top_ret = DataModelManager.top_data_model(model_id, bk_username)
        return Response(top_ret)

    @detail_route(methods=["post"], url_path="cancel_top")
    @params_valid(serializer=BkUserNameSerializer)
    def cancel_top(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/cancel_top/ *模型取消置顶

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_cancel_top
        @apiDescription 模型取消置顶

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        cancel_top_ret = DataModelManager.cancel_top_data_model(model_id, bk_username)
        return Response(cancel_top_ret)

    @detail_route(methods=["post"], url_path="confirm_overview")
    @params_valid(serializer=BkUserNameSerializer)
    def confirm_overview(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/confirm_overview/ *确认模型预览

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_confirm_overview
        @apiDescription 确认模型预览,记录已完成步骤(仅模型预览后点击下一步调用)

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "step_id": 4
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        check_perm("datamodel.update", model_id)
        step_id = DataModelManager.confirm_data_model_overview(model_id, bk_username)
        return Response({"step_id": step_id})

    @detail_route(methods=["post"], url_path="confirm_indicators")
    @params_valid(serializer=BkUserNameSerializer)
    def confirm_indicators(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/confirm_indicators/ *确认指标

        @apiVersion 3.5.0
        @apiGroup DataModel_Model
        @apiName datamodel_model_confirm_indicators
        @apiDescription 确认指标设计,记录已完成步骤(仅指标设计页面点击下一步调用)

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "step_id": 3
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        check_perm("datamodel.update", model_id)
        step_id = DataModelManager.confirm_indicators(model_id, bk_username)
        return Response({"step_id": step_id})


class MasterTableViewSet(APIViewSet):
    @params_valid(serializer=MasterTableCreateSerializer)
    def create(self, request, model_id, params):
        """
        @api {post} /datamanage/datamodel/models/:model_id/master_tables/ *创建和修改主表

        @apiVersion 3.5.0
        @apiGroup DataModel_MasterTable
        @apiName datamodel_master_table_update
        @apiDescription 创建和修改主表

        @apiParam {List} fields 模型主表字段列表
        @apiParam {Int} fields.model_id 模型ID
        @apiParam {Int} fields.field_name 字段名称
        @apiParam {Int} fields.field_alias 字段别名
        @apiParam {String} fields.field_index 字段位置
        @apiParam {String} fields.field_type 数据类型
        @apiParam {String} fields.field_category 字段类型
        @apiParam {String} fields.description 字段描述
        @apiParam {Json} fields.field_constraint_content 字段约束内容
        @apiParam {Json} fields.field_clean_content 清洗规则
        @apiParam {Int} fields.source_model_id 来源模型id
        @apiParam {String} fields.source_field_name 来源字段
        @apiParam {List} [model_relation] 模型主表关联信息

        @apiParamExample {json} 参数样例:
        {
            "fields":[
                {
                    "model_id":32,
                    "field_name":"price",
                    "field_alias":"道具价格",
                    "field_index":1,
                    "field_type":"long",
                    "field_category":"measure",
                    "description":"道具价格111",
                    "field_constraint_content":{
                        "op": "OR",
                        "groups": [
                            {
                                "op": "AND",
                                "items": [
                                    {"constraint_id": "", "constraint_content": ""},
                                    {"constraint_id": "", "constraint_content": ""}
                                ]
                            },
                            {
                                "op": "OR",
                                "items": [
                                    {"constraint_id": "", "constraint_content": ""},
                                    {"constraint_id": "", "constraint_content": ""}
                                ]
                            }
                       ]
                    },
                    "field_clean_content":{
                        "clean_option":"SQL",
                        "clean_content":"price * 100 as price"
                    },
                    "source_model_id":null,
                    "source_field_name":null
                },
                {
                    "model_id":32,
                    "field_name":"channel_id",
                    "field_alias":"渠道号",
                    "field_index":2,
                    "field_type":"string",
                    "field_category":"dimension",
                    "description":"渠道号",
                    "field_constraint_content":null,
                    "field_clean_content":null,
                    "source_model_id":null,
                    "source_field_name":null
                },
                {
                    "model_id":32,
                    "field_name":"channel_name",
                    "field_alias":"渠道名称",
                    "field_index":3,
                    "field_type":"string",
                    "field_category":"dimension",
                    "description":"渠道名称",
                    "field_constraint_content":null,
                    "field_clean_content":null,
                    "source_model_id":33,
                    "source_field_name":"channel_id"
                },
                {
                    "model_id":32,
                    "field_name":"__time__",
                    "field_alias":"时间字段",
                    "field_index":4,
                    "field_type":"timestamp",
                    "field_category":"dimension",
                    "description":"平台内置时间字段，数据入库后将转换为可查询字段，比如 dtEventTime/dtEventTimeStamp/localtime",
                    "field_constraint_content":null,
                    "field_clean_content":null,
                    "source_model_id":null,
                    "source_field_name":null
                }
            ],
            "model_relation":[
                {
                    "model_id":32,
                    "field_name":"channel_id",
                    "related_model_id":33,
                    "related_field_name":"channel_id"
                }
            ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "step_id": 2
                }
            }
        """
        model_id = convert_to_number("model_id", model_id)
        bk_username = get_request_username()
        check_perm("datamodel.update", model_id)
        # 修改主表
        master_table_dict = MasterTableManager.update_master_table(model_id, params, bk_username)
        return Response(master_table_dict)

    @params_valid(serializer=MasterTableListSerializer)
    def list(self, request, model_id, params):
        """
        @api {get} /datamanage/datamodel/models/:model_id/master_tables/ *主表详情

        @apiVersion 3.5.0
        @apiGroup DataModel_MasterTable
        @apiName datamodel_master_table_info
        @apiDescription 主表详情

        @apiParam {Boolean} with_time_field 是否展示时间字段,默认不展示
        @apiParam {List} allow_field_type 允许展示的字段类型
        @apiParam {List} with_details 展示字段详情，示例：['deletable', 'editable']

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.model_name 模型名称
        @apiSuccess (返回) {String} data.model_alias 模型别名
        @apiSuccess (返回) {String} data.table_name 主表名称
        @apiSuccess (返回) {String} data.table_alias 主表别名
        @apiSuccess (返回) {String} data.step_id 模型构建&发布完成步骤
        @apiSuccess (返回) {List} data.fields 主表字段信息
        @apiSuccess (返回) {Int} data.fields.model_id 模型ID
        @apiSuccess (返回) {String} data.fields.field_name 字段名称
        @apiSuccess (返回) {String} data.fields.field_alias 字段别名
        @apiSuccess (返回) {Int} data.fields.field_index 字段位置
        @apiSuccess (返回) {String} data.fields.field_type 数据类型
        @apiSuccess (返回) {String} data.fields.field_category 字段类型
        @apiSuccess (返回) {String} data.fields.description 字段描述
        @apiSuccess (返回) {List} data.fields.field_constraint_content 字段约束内容
        @apiSuccess (返回) {Json} data.fields.field_clean_content 清洗规则
        @apiSuccess (返回) {Int} data.fields.source_model_id 来源模型id
        @apiSuccess (返回) {String} data.fields.source_field_name 来源字段
        @apiSuccess (返回) {Boolean} data.fields.is_join_field 是否关联字段
        @apiSuccess (返回) {Boolean} data.fields.is_extended_field 是否扩展字段
        @apiSuccess (返回) {String} data.fields.join_field_name 扩展字段对应的关联字段名称
        @apiSuccess (返回) {String} data.fields.deletable 字段能否被删除
        @apiSuccess (返回) {String} data.fields.editable 字段能否被编辑
        @apiSuccess (返回) {String} data.fields.editable_deletable_info 字段能否被编辑、被删除详情
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_join_field 字段是否关联维度模型
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_source_field 字段是否被模型作为扩展字段
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.source_field_models 字段被什么模型作为扩展字段
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_used_by_other_fields 字段是否被其他字段加工逻辑引用
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.fields 字段被什么字段加工逻辑引用
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_used_by_calc_atom 字段是否被统计口径引用
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.calculation_atoms 字段被什么统计口径引用
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_aggregation_field 字段是否是指标聚合字段
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.aggregation_field_indicators 字段是什么指标聚合字段
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.is_condition_field 字段是否是指标过滤条件字段
        @apiSuccess (返回) {String} data.fields.editable_deletable_info.condition_field_indicators 是什么指标过滤条件字段
        @apiSuccess (返回) {List} data.model_relation 模型关联关系

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "model_id":1,
                "model_name":"fact_item_flow",
                "model_alias":"道具流水表",
                "table_name":"fact_item_flow",
                "table_alias":"道具流水表",
                "fields":[
                    {
                        "model_id": 1,
                        "id": 1,
                        "field_name":"price",
                        "field_alias":"道具价格",
                        "field_index": 1,
                        "field_type":"long",
                        "field_category":"metric",
                        "description":"道具价格",
                        "field_constraint_content":[
                            {
                                "content": {
                                    "constraint_id": "value_enum",
                                    "constraint_content": "1,2"
                                }
                            }
                        ],
                        "field_clean_content":{
                            "clean_option":"SQL",
                            "clean_content":"price * 100 as price"
                        },
                        "source_model_id":null,
                        "source_field_name":null,
                        "is_join_field": false,
                        "is_extended_field": false,
                        "join_field_name": null,
                        "deletable": True,
                        "editable": True,
                        "editable_deletable_info": {
                            "source_field_models": [],
                            "is_source_field": false,
                            "is_used_by_calc_atom": false,
                            "aggregation_field_indicators": [],
                            "is_aggregation_field": false,
                            "is_used_by_other_fields": false,
                            "is_join_field": true,
                            "condition_field_indicators": [],
                            "calculation_atoms": [],
                            "fields": [],
                            "is_condition_field": false
                        },
                    },
                    {
                        "model_id": 1,
                        "id": 2,
                        "field_name":"channel_id",
                        "field_alias":"渠道号",
                        "field_index": 2,
                        "field_type":"string",
                        "field_category":"dimension",
                        "description":"渠道号",
                        "field_constraint_content":[],
                        "field_clean_content":null,
                        "source_model_id":null,
                        "source_field_name":null,
                        "is_join_field": true,
                        "is_extended_field": false,
                        "join_field_name": null,
                    },
                    {
                        "model_id":32,
                        "field_name":"__time__",
                        "field_alias":"时间字段",
                        "field_index":4,
                        "field_type":"timestamp",
                        "field_category":"dimension",
                        "description":"平台内置时间字段，数据入库后转换为可查询字段，如dtEventTime/dtEventTimeStamp/localtime",
                        "field_constraint_content":[],
                        "field_clean_content":null,
                        "source_model_id":null,
                        "source_field_name":null,
                        "is_join_field": false,
                        "is_extended_field": false,
                        "join_field_name": null,
                    }
                ],
                "model_relation":[
                    {
                        "model_id": 1
                        "field_name":"channel_id",
                        "related_model_id":2,
                        "related_field_name":"channel_id",
                        "related_method":"left-join"
                    }
                ]
            }
        """
        # 主表详情
        model_id = convert_to_number("model_id", model_id)
        check_perm("datamodel.retrieve", model_id)
        with_time_field = params["with_time_field"]
        allow_field_type = params["allow_field_type"]
        with_details = params["with_details"]
        latest_version = params["latest_version"]
        # 返回草稿态主表信息
        master_table_dict = MasterTableManager.get_master_table_info(
            model_id, with_time_field, allow_field_type, with_details, latest_version
        )
        return Response(master_table_dict)


class FieldConstraintConfigViewSet(APIModelViewSet):
    def list(self, request):
        """
        @api {get} /datamanage/datamodel/field_constraint_configs/ *字段约束配置列表

        @apiVersion 3.5.0
        @apiGroup FieldConstraintConfig
        @apiName datamodel_field_constraint_config_list
        @apiDescription 字段约束配置列表

        @apiSuccess (返回) {String} data.constraint_id 字段约束英文名
        @apiSuccess (返回) {String} data.constraint_name 字段约束中文名
        @apiSuccess (返回) {String} data.constraint_value 字段约束内容/示例，例如:(100,200]
        @apiSuccess (返回) {Boolean} data.editable 字段约束内容是否可编辑
        @apiSuccess (返回) {String} data.constraint_type 字段约束类型:general:通用,specific:特定
        @apiSuccess (返回) {Json} data.validator 约束校验内容
        @apiSuccess (返回) {Boolean} data.description 约束说明
        @apiSuccess (返回) {List} data.allow_field_type 允许的数据类型
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "description": null,
                        "constraint_id": "start_with",
                        "editable": true,
                        "constraint_type": "general",
                        "constraint_value": "http",
                        "constraint_name": "开头是",
                        "validator": {
                            "content": null,
                            "type": "string_validator"
                        },
                        "allow_field_type": [
                            "string"
                        ]
                    }
                ]
            }
        """
        constraint_list = get_field_constraint_tree_list()
        return Response(constraint_list)


class FieldTypeConfigViewSet(APIModelViewSet):
    @params_valid(serializer=FieldTypeListSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/datamodel/field_type_configs/ *数据类型配置列表

        @apiVersion 3.5.0
        @apiGroup FieldConstraintConfig
        @apiName datamodel_field_type_config_list
        @apiDescription 数据类型配置列表

        @apiParam {List} include_field_type 额外返回的数据类型列表
        @apiParam {List} exclude_field_type 不返回的数据类型列表

        @apiSuccess (返回) {String} data.field_type 数据类型ID
        @apiSuccess (返回) {String} data.field_type_alias 数据类型中文名
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "field_type":"double",
                        "field_type_alias":"浮点型"
                    }
                ]
            }
        """

        include_field_type = params["include_field_type"]
        exclude_field_type = params["exclude_field_type"]

        field_type_list = get_field_type_configs(include_field_type, exclude_field_type)
        return Response(field_type_list)


class StandardFieldViewSet(APIModelViewSet):
    def list(self, request):
        """
        @api {get} /datamanage/datamodel/standard_fields/ 公共字段列表

        @apiVersion 3.5.0
        @apiGroup Datamodel_StandardField
        @apiName datamodel_standard_field_list
        @apiDescription 公共字段列表,后期要考虑按照模型tag、schema相似度做字段推荐

        @apiParam {String} [fuzzy] 模糊过滤
        @apiParam {String} [field_name] 字段名称

        @apiSuccess (返回) {String} data.field_name 字段名称
        @apiSuccess (返回) {String} data.field_alias 字段别名
        @apiSuccess (返回) {String} data.field_type 字段类型
        @apiSuccess (返回) {String} data.field_category 字段分类
        @apiSuccess (返回) {String} data.description 字段描述
        @apiSuccess (返回) {String} data.field_constraint_content 字段约束内容

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "field_name": "zone_id",
                        "field_alias": "大区ID",
                        "field_type": "string",
                        "field_category": "dimension",
                        "description": "大区ID",
                        "field_constraint_content": null
                    }
                ]
            }
        """
        return Response()


class ResultTableViewSet(APIViewSet):
    lookup_field = "rt_id"

    @detail_route(methods=["get"], url_path="fields")
    @params_valid(serializer=ResultTableFieldListSerializer)
    def fields(self, request, rt_id, params):
        """
        @api {get} /datamanage/datamodel/result_tables/:rt_id/fields/ rt字段列表
        @apiVersion 3.5.0
        @apiGroup ResultTableField
        @apiName datamodel_result_table_fields_list
        @apiDescription rt字段列表,默认不返回时间类型字段

        @apiParam {Boolean} [with_time_field] 是否返回时间类型字段，默认不返回

        @apiSuccess (返回) {String} data.field_name 字段名称
        @apiSuccess (返回) {String} data.field_alias 字段别名
        @apiSuccess (返回) {String} data.field_type 字段类型
        @apiSuccess (返回) {String} data.field_category 字段分类
        @apiSuccess (返回) {String} data.description 字段描述
        @apiSuccess (返回) {String} data.field_constraint_content 字段约束内容

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "field_type": "string",
                        "field_alias": "client_ip",
                        "description": "client_ip",
                        "roles": {
                            "event_time": false
                        },
                        "created_at": "2019-03-15 22:13:16",
                        "is_dimension": false,
                        "created_by": "admin",
                        "updated_at": "2019-03-15 22:13:16",
                        "origins": "",
                        "field_name": "client_ip",
                        "id": 11199,
                        "field_index": 2,
                        "updated_by": ""
                    }
                ]
            }
        """
        fields = MetaApi.result_tables.fields({"result_table_id": rt_id}, raise_exception=True).data
        if params["with_time_field"]:
            return Response(fields)
        filtered_fields = [
            field_dict
            for field_dict in fields
            if not (
                field_dict["field_type"] == TimeField.TIME_FIELD_TYPE
                or field_dict["field_name"].lower() in InnerField.INNER_FIELD_LIST
                or field_dict["field_name"] in InnerField.INNER_FIELD_LIST
            )
        ]
        filtered_fields.append(TimeField.TIME_FIELD_DICT)
        return Response(filtered_fields)
