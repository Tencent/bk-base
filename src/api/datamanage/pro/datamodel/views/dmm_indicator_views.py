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
    IndicatorManager,
)
from datamanage.pro.datamodel.dmm.object import get_object
from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom
from datamanage.pro.datamodel.serializers.indicator import (
    CalculationAtomCreateSerializer,
    CalculationAtomInfoSerializer,
    CalculationAtomListSerializer,
    CalculationAtomQuoteSerializer,
    CalculationAtomSerializer,
    CalculationAtomUpdateSerializer,
    IndicatorCreateSerializer,
    IndicatorInfoSerializer,
    IndicatorListSerializer,
    IndicatorModelIdSerializer,
    IndicatorUpdateSerializer,
)
from rest_framework.response import Response

from common.auth import check_perm
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIModelViewSet, APIViewSet


class IndicatorViewSet(APIViewSet):
    lookup_field = "indicator_name"

    @params_valid(serializer=IndicatorCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/datamodel/indicators/ 创建指标

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_create
        @apiDescription 创建指标,调度内容配置详见dataflow接口文档

        @apiParam {Int} model_id 模型id
        @apiParam {String} indicator_name 指标名称
        @apiParam {String} indicator_alias 指标别名
        @apiParam {String} description 指标描述
        @apiParam {String} calculation_atom_name 统计口径名称
        @apiParam {List} aggregation_fields 聚合字段
        @apiParam {String} filter_formula 过滤SQL
        @apiParam {Json} scheduling_content 调度内容
        @apiParam {String} parent_indicator_name 父指标名称

        @apiParamExample {Json} 离线参数样例
        {
            "model_id": 1,
            "indicator_name":"item_sales_amt_china_1d",
            "indicator_alias":"国内每天按大区统计道具销售额",
            "description":"国内每天按大区统计道具销售额",
            "calculation_atom_name":"item_sales_amt",
            "aggregation_fields":["channel_name"],
            "filter_formula": "os='android'",
            "scheduling_type": "batch",
            "scheduling_content":{
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
            },
            "parent_indicator_name": null,
        }

        @apiParamExample {Json} 实时参数样例
        {
            "model_id": 1,
            "indicator_name":"item_sales_amt_china_1d",
            "indicator_alias":"国内每天按大区统计道具销售额",
            "description":"国内每天按大区统计道具销售额",
            "calculation_atom_name":"item_sales_amt",
            "aggregation_fields":["channel_name"],
            "filter_formula": "os='android'",
            "scheduling_type": "stream",
            "scheduling_content":{
                "window_type": "slide",
                "count_freq": 30,
                "window_time": 1440,
                "waiting_time": 0,
                "window_lateness":{
                    "lateness_count_freq":60,
                    "allowed_lateness":True,
                    "lateness_time":6
                }
            },
            "parent_indicator_name": null,
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_id":1,
                    "indicator_name":"item_sales_amt_china_1d",
                    "indicator_alias":"国内每天按大区统计道具销售额",
                    "description":"国内每天按大区统计道具销售额",
                    "calculation_atom_name":"item_sales_amt",
                    "aggregation_fields":["channel_name"],
                    "filter_formula": "os='android'"
                    "scheduling_type": "batch",
                    "scheduling_content":{
                        "window_type": "fixed",
                        "count_freq": 1,
                        "schedule_period": "day"
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
                    },
                    "parent_indicator_name": null,
                    "step_id": 3,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                }
            }
        """
        bk_username = get_request_username()
        check_perm("datamodel.update", params["model_id"])
        # 创建指标
        indicator_dict = IndicatorManager.create_indicator(params, bk_username)
        return Response(indicator_dict)

    @params_valid(serializer=IndicatorUpdateSerializer)
    def update(self, request, indicator_name, params):
        """
        @api {put} /datamanage/datamodel/indicators/:indicator_name/ 修改指标

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_update
        @apiDescription 修改指标

        @apiParam {String} [indicator_alias] 指标别名
        @apiParam {String} [description] 指标描述
        @apiParam {String} [calculation_atom_name] 统计口径名称
        @apiParam {List} [aggregation_fields] 聚合字段
        @apiParam {String} [filter_formula] 过滤SQL
        @apiParam {Json} [scheduling_content] 调度内容

        @apiParamExample {json} 参数样例:
        {
            "indicator_alias":"国内每天按大区统计道具销售额",
            "description":"国内每天按大区统计道具销售额",
            "calculation_atom_name":"item_sales_amt",
            "aggregation_fields":["channel_name"],
            "filter_formula": "os='android'"
            "scheduling_type": "batch",
            "scheduling_content":{
                "window_type": "fixed",
                "count_freq": 1,
                "schedule_period": "day"
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
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_id":1,
                    "project_id": 3,
                    "indicator_name":"item_sales_amt_china_1d",
                    "indicator_alias":"国内每天按大区统计道具销售额",
                    "description":"国内每天按大区统计道具销售额",
                    "calculation_atom_name":"item_sales_amt",
                    "aggregation_fields":["channel_name"],
                    "filter_formula": "os='android'"
                    "scheduling_content":{
                        "window_type": "fixed",
                        "count_freq": 1,
                        "schedule_period": "day"
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
                    },
                    "parent_indicator_name": null,
                    "step_id": 3,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56"
                }
            }
        """
        bk_username = get_request_username()
        # 鉴权
        check_perm("datamodel.update", params["model_id"])
        # 修改指标
        indicator_dict = IndicatorManager.update_indicator(indicator_name, params, bk_username)
        return Response(indicator_dict)

    @params_valid(serializer=IndicatorModelIdSerializer)
    def delete(self, request, indicator_name, params):
        """
        @api {delete} /datamanage/datamodel/indicators/:indicator_name/ 删除指标

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_delete
        @apiDescription 删除指标,要判断有无sub_indicator

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
        bk_username = get_request_username()
        # 鉴权
        check_perm("datamodel.delete", params["model_id"])
        # 删除指标
        indicator_dict = IndicatorManager.delete_indicator(indicator_name, params["model_id"], bk_username)
        return Response(indicator_dict)

    @params_valid(serializer=IndicatorListSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/datamodel/indicators/ 指标列表

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_list
        @apiDescription 指标列表

        @apiParam {Int} model_id 模型id
        @apiParam {String} [calculation_atom_name] 统计口径名称
        @apiParam {String} [parent_indicator_name] 父指标名称
        @apiParam {Boolean} [with_sub_indicators] 是否显示子指标

        @apiSuccess (返回) {List} data.results 指标列表
        @apiSuccess (返回) {Int} data.results.model_id 模型ID
        @apiSuccess (返回) {String} data.results.indicator_name 指标名称
        @apiSuccess (返回) {String} data.results.indicator_alias 指标别名
        @apiSuccess (返回) {String} data.results.description 指标描述
        @apiSuccess (返回) {String} data.results.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {List} data.results.aggregation_fields 聚合字段
        @apiSuccess (返回) {List} data.results.aggregation_fields_alias 聚合字段中文名
        @apiSuccess (返回) {List} data.results.filter_formula 过滤条件
        @apiSuccess (返回) {List} data.results.filter_formula_stripped_comment 删除注释的过滤条件
        @apiSuccess (返回) {String} data.results.scheduling_content 调度内容
        @apiSuccess (返回) {String} data.results.parent_indicator_name 父指标名称
        @apiSuccess (返回) {String} data.results.created_by 创建人
        @apiSuccess (返回) {String} data.results.created_at 创建时间
        @apiSuccess (返回) {String} data.results.updated_by 更新人
        @apiSuccess (返回) {String} data.results.updated_at 更新时间
        @apiSuccess (返回) {Int} data.step_id 模型构建&发布完成步骤

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "results": [
                        {
                            "model_id":1,
                            "indicator_name":"item_sales_amt_china_1d",
                            "indicator_alias":"国内每天按大区统计道具销售额",
                            "description":"国内每天按大区统计道具销售额",
                            "calculation_atom_name":"item_sales_amt",
                            "aggregation_fields":["channel_name"],
                            "aggregation_fields_alias":["渠道名称"],
                            "filter_formula": "os='android'"
                            "scheduling_content":{
                                "window_type": "fixed",
                                "count_freq": 1,
                                "schedule_period": "day"
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
                            },
                            "parent_indicator_name": null,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56"
                        }
                    ],
                    "step_id": 3,
                }
            }
        """
        check_perm("datamodel.retrieve", params["model_id"])
        indicator_list, step_id, publish_status = IndicatorManager.get_indicator_list(params)
        return Response(
            {
                "results": indicator_list,
                "step_id": step_id,
                "publish_status": publish_status,
            }
        )

    @detail_route(methods=["get"], url_path="info")
    @params_valid(serializer=IndicatorInfoSerializer)
    def info(self, request, indicator_name, params):
        """
        @api {get} /datamanage/datamodel/indicators/:indicator_name/info/ 指标详情

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_info
        @apiDescription 指标详情

        @apiParam {Boolean} [with_sub_indicators] 是否显示子指标
        @apiParam {Boolean} model_id 是否显示子指标

        @apiSuccess (返回) {Number} data.model_id 模型ID
        @apiSuccess (返回) {Number} data.project_id 项目ID
        @apiSuccess (返回) {String} data.indicator_name 指标名称
        @apiSuccess (返回) {String} data.indicator_alias 指标别名
        @apiSuccess (返回) {String} data.description 指标描述
        @apiSuccess (返回) {String} data.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {String} data.aggregation_fields 聚合字段
        @apiSuccess (返回) {String} data.scheduling_content 调度内容
        @apiSuccess (返回) {String} data.parent_indicator_name 父指标名称
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {Number} data.step_id 模型构建&发布完成步骤

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "model_id":1,
                    "project_id": 3,
                    "indicator_name":"item_sales_amt_china_1d",
                    "indicator_alias":"国内每天按大区统计道具销售额",
                    "description":"国内每天按大区统计道具销售额",
                    "calculation_atom_name":"item_sales_amt",
                    "aggregation_fields":["channel_name"],
                    "filter_formula": "os='android'"
                    "scheduling_content":{
                        "window_type": "fixed",
                        "count_freq": 1,
                        "schedule_period": "day"
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
                    },
                    "parent_indicator_name": null,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "step_id": 3
                }
            }
        """
        # 鉴权
        check_perm("datamodel.retrieve", params["model_id"])
        # 获取指标信息
        indicator_dict = IndicatorManager.get_indicator_info(indicator_name, params["model_id"], params)
        return Response(indicator_dict)

    @detail_route(methods=["get"], url_path="fields")
    @params_valid(serializer=IndicatorModelIdSerializer)
    def fields(self, request, indicator_name, params):
        """
        @api {get} /datamanage/datamodel/indicators/:indicator_name/fields/ 指标字段

        @apiVersion 3.5.0
        @apiGroup DataModel_Indicator
        @apiName datamodel_indicator_fields
        @apiDescription 指标字段

        @apiSuccess (返回) {List} data.fields 字段列表
        @apiSuccess (返回) {String} data.fields.field_name 字段名称
        @apiSuccess (返回) {String} data.fields.field_alias 字段别名
        @apiSuccess (返回) {String} data.fields.field_type 数据类型
        @apiSuccess (返回) {String} data.fields.field_category 字段类型
        @apiSuccess (返回) {String} data.fields.description 字段描述

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "fields":[]
                }
            }
        """
        # 鉴权
        check_perm("datamodel.retrieve", params["model_id"])
        # 获取指标字段
        fields = IndicatorManager.get_indicator_fields(indicator_name, model_id=params["model_id"])
        return Response({"fields": fields})


class CalculationAtomViewSet(APIViewSet):
    lookup_field = "calculation_atom_name"

    @params_valid(serializer=CalculationAtomCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/datamodel/calculation_atoms/ 创建统计口径

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_create
        @apiDescription 创建统计口径

        @apiParam {Int} model_id 模型id
        @apiParam {String} calculation_atom_name 统计口径名称
        @apiParam {String} calculation_atom_alias 统计口径别名
        @apiParam {String} description 统计口径描述
        @apiParam {String} field_type 数据类型
        @apiParam {Json} calculation_content 统计方式

        @apiParamExample {json} 表单提交参数样例:
        {
            "model_id": 1,
            "calculation_atom_name":"item_sales_amt",
            "calculation_atom_alias":"item_sales_amt",
            "description":"item_sales_amt",
            "field_type":"long",
            "calculation_content":{
                "option":"TABLE",
                "content":{
                    "calculation_field":"price",
                    "calculation_function":"sum"
                }
            }
        }
        @apiParamExample {json} SQL提交参数样例:
        {
            "model_id": 1,
            "calculation_atom_name":"item_sales_amt",
            "calculation_atom_alias":"item_sales_amt",
            "description":"item_sales_amt",
            "field_type":"long",
            "calculation_content":{
                "option":"SQL",
                "content":{
                    "calculation_formula": "sum(price)"
                }
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
                    "model_id":1,
                    "project_id":3,
                    "calculation_atom_name":"item_sales_amt",
                    "calculation_atom_alias":"item_sales_amt",
                    "calculation_atom_type":"create",
                    "description":"item_sales_amt",
                    "field_type":"long",
                    "calculation_content":{
                        "option":"Table",
                        "content":{
                            "calculation_field":"price",
                            "calculation_function":"sum",
                        }
                    },
                    "calculation_formula":"sum(price)",
                    "step_id": 2,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56"
                }
            }
        """
        bk_username = get_request_username()
        check_perm("datamodel.update", params["model_id"])
        # 创建统计口径
        calculation_atom_dict = CalculationAtomManager.create_calculation_atom(params, bk_username)
        return Response(calculation_atom_dict)

    @params_valid(serializer=CalculationAtomUpdateSerializer)
    def update(self, request, calculation_atom_name, params):
        """
        @api {put} /datamanage/datamodel/calculation_atoms/:calculation_atom_name/ 修改统计口径

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_update
        @apiDescription 修改统计口径

        @apiParam {String} model_id 模型id
        @apiParam {String} [calculation_atom_alias] 统计口径别名
        @apiParam {String} [description] 统计口径描述
        @apiParam {String} [field_type] 数据类型
        @apiParam {Json} [calculation_content] 统计方式

        @apiParamExample {json} 参数样例:
        {
            "model_id": 1,
            "calculation_atom_alias":"item_sales_amt",
            "description":"item_sales_amt",
            "field_type":"long",
            "calculation_content":{
                "option":"TABLE",
                "content":{
                    "calculation_field":"price",
                    "calculation_function":"sum",
                    "calculation_formula": "sum(price)"
                }
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
                            "calculation_function":"sum",
                        }
                    },
                    "calculation_formula":"sum(price)",
                    "step_id": 2,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56"
                }
            }
        """
        bk_username = get_request_username()
        check_perm("datamodel.update", params["model_id"])
        # 修改统计口径
        calc_atom_dict = CalculationAtomManager.update_calculation_atom(calculation_atom_name, params, bk_username)
        return Response(calc_atom_dict)

    @params_valid(serializer=CalculationAtomSerializer)
    def delete(self, request, calculation_atom_name, params):
        """
        @api {delete} /datamanage/datamodel/calculation_atoms/:calculation_atom_name/ 删除统计口径

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_delete
        @apiDescription 删除统计口径

        @apiParam {Int} model_id 模型id

        @apiParamExample {json} 参数样例:
        {
            "model_id": 1
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "step_id": 2,
                }
            }
        """
        # 用户名
        bk_username = get_request_username()
        check_perm("datamodel.delete", params["model_id"])
        # 统计口径删除
        calc_atom_dict = CalculationAtomManager.delete_calculation_atom(
            calculation_atom_name, params["model_id"], bk_username
        )
        return Response(calc_atom_dict)

    @list_route(methods=["post"], url_path="quote")
    @params_valid(serializer=CalculationAtomQuoteSerializer)
    def quote(self, request, params):
        """
        @api {post} /datamanage/datamodel/calculation_atoms/quote/ 引用集市统计口径

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_quote
        @apiDescription 引用集市统计口径

        @apiParam {Int} model_id 模型id
        @apiParam {List} calculation_atom_names 引用的统计口径名称列表

        @apiParamExample {json} 参数样例:
        {
            "model_id":3,
            "calculation_atom_names":["item_sales_amt"]
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
        # 用户名
        bk_username = get_request_username()
        check_perm("datamodel.update", params["model_id"])
        # 统计口径引用
        calculation_atom_image_dict = CalculationAtomManager.quote_calculation_atoms(params, bk_username)
        return Response(calculation_atom_image_dict)

    @params_valid(serializer=CalculationAtomListSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/datamodel/calculation_atoms/ 统计口径列表

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_list
        @apiDescription 统计口径列表

        @apiParam {Int} [model_id] 模型id
        @apiParam {Boolean} [with_indicators] 是否返回指标信息,默认不返回

        @apiSuccess (返回) {List} data.results 模型统计口径
        @apiSuccess (返回) {Int} data.results.model_id 模型ID
        @apiSuccess (返回) {String} data.results.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {String} data.results.calculation_atom_alias 统计口径别名
        @apiSuccess (返回) {String} data.results.calculation_atom_type 统计口径类型,create:模型中创建/quote:模型中引用
        @apiSuccess (返回) {String} data.results.description 统计口径描述
        @apiSuccess (返回) {String} data.results.field_type 数据类型
        @apiSuccess (返回) {String} data.results.calculation_content 统计方式
        @apiSuccess (返回) {String} data.results.calculation_formula 统计SQL
        @apiSuccess (返回) {String} data.results.calculation_formula_stripped_comment 删除注释的统计SQL
        @apiSuccess (返回) {Int} data.results.indicator_count 指标数量
        @apiSuccess (返回) {Int} data.results.deletable 统计口径是否可以被删除
        @apiSuccess (返回) {Int} data.results.editable 统计口径是否可以被编辑
        @apiSuccess (返回) {Int} data.results.is_applied_by_indicators 统计口径是否被指标应用
        @apiSuccess (返回) {Int} data.results.is_quoted_by_other_models 统计口径是否被其他模型引用
        @apiSuccess (返回) {String} data.results.created_by 创建人
        @apiSuccess (返回) {String} data.results.created_at 创建时间
        @apiSuccess (返回) {String} data.results.updated_by 更新人
        @apiSuccess (返回) {String} data.results.updated_at 更新时间
        @apiSuccess (返回) {List} data.results.indicators 指标信息,在with_indicators为true时返回
        @apiSuccess (返回) {Int} data.step_id 模型构建&发布完成步骤

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "results":[
                        {
                            "model_id":1,
                            "project_id": 3,
                            "calculation_atom_name":"item_sales_amt",
                            "calculation_atom_alias":"item_sales_amt",
                            "calculation_atom_type":"create",
                            "description":"item_sales_amt",
                            "field_type":"long",
                            "calculation_content":{
                                "option":"table",
                                "content":{
                                    "calculation_field":"price",
                                    "calculation_function":"sum"
                                }
                            },
                            "calculation_formula_stripped_comment": "sum(price)"
                            "calculation_formula":"sum(price)",
                            "indicator_count": 2,
                            "editable": True,
                            "deletable": False,
                            "is_applied_by_indicators": True,
                            "is_quoted_by_other_models": False,
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56",
                            "indicators":[
                                {
                                    "indicator_name":"item_sales_amt_china_1d",
                                    "indicator_alias":"国内每天按大区统计道具销售额",
                                    "sub_indicators":[
                                        {
                                            "indicator_name":"item_sales_amt_china_1d",
                                            "indicator_alias":"国内每天按大区统计道具销售额"
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                    "step_id": 3,
                }
            }
        """
        (
            calculation_atom_list,
            step_id,
            publish_status,
        ) = CalculationAtomManager.get_calculation_atom_list(params)
        return Response(
            {
                "results": calculation_atom_list,
                "step_id": step_id,
                "publish_status": publish_status,
            }
        )

    @list_route(methods=["get"], url_path="can_be_quoted")
    @params_valid(serializer=CalculationAtomSerializer)
    def get_cal_atom_list_can_be_quoted(self, request, params):
        """
        @api {get} /datamanage/datamodel/calculation_atoms/can_be_quoted/ 可以被引用的统计口径列表

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_list_can_be_quoted
        @apiDescription 可以被引用的统计口径列表

        @apiParam {Int} [model_id] 模型id

        @apiSuccess (返回) {List} data.results 模型统计口径
        @apiSuccess (返回) {Int} data.results.model_id 模型ID
        @apiSuccess (返回) {String} data.results.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {String} data.results.calculation_atom_alias 统计口径别名
        @apiSuccess (返回) {String} data.results.calculation_atom_type 统计口径类型,create:模型中创建/quote:模型中引用
        @apiSuccess (返回) {String} data.results.description 统计口径描述
        @apiSuccess (返回) {String} data.results.field_type 数据类型
        @apiSuccess (返回) {String} data.results.calculation_content 统计方式
        @apiSuccess (返回) {String} data.results.calculation_formula 统计SQL
        @apiSuccess (返回) {Int} data.results.quoted_count 引用数量
        @apiSuccess (返回) {String} data.results.created_by 创建人
        @apiSuccess (返回) {String} data.results.created_at 创建时间
        @apiSuccess (返回) {String} data.results.updated_by 更新人
        @apiSuccess (返回) {String} data.results.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "results":[
                        {
                            "model_id":1,
                            "project_id": 3,
                            "calculation_atom_name":"item_sales_amt",
                            "calculation_atom_alias":"item_sales_amt",
                            "calculation_atom_type":"create",
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
                            "created_by":"admin",
                            "created_at":"2020-10-18 15:38:56",
                            "updated_by":"admin",
                            "updated_at":"2020-10-18 15:38:56",
                        }
                    ]
                }
            }
        """
        calculation_atom_list = CalculationAtomManager.get_cal_atom_list_can_be_quoted(params["model_id"])
        return Response({"results": calculation_atom_list})

    @detail_route(methods=["get"], url_path="info")
    @params_valid(serializer=CalculationAtomInfoSerializer)
    def info(self, request, calculation_atom_name, params):
        """
        @api {get} /datamanage/datamodel/calculation_atoms/:calculation_atom_name/info/ 统计口径详情

        @apiVersion 3.5.0
        @apiGroup DataModel_CalculationAtom
        @apiName datamodel_calculation_atom_info
        @apiDescription 统计口径详情

        @apiParam {Boolean} [with_indicators] 是否返回指标信息,默认不返回

        @apiSuccess (返回) {Int} data.model_id 模型ID
        @apiSuccess (返回) {String} data.calculation_atom_name 统计口径名称
        @apiSuccess (返回) {String} data.calculation_atom_alias 统计口径别名
        @apiSuccess (返回) {String} data.calculation_atom_type 统计口径类型
        @apiSuccess (返回) {String} data.description 统计口径描述
        @apiSuccess (返回) {String} data.field_type 数据类型
        @apiSuccess (返回) {String} data.calculation_content 统计方式
        @apiSuccess (返回) {String} data.calculation_formula 统计SQL
        @apiSuccess (返回) {Int} data.indicator_count 指标数量
        @apiSuccess (返回) {String} data.created_by 创建人
        @apiSuccess (返回) {String} data.created_at 创建时间
        @apiSuccess (返回) {String} data.updated_by 更新人
        @apiSuccess (返回) {String} data.updated_at 更新时间
        @apiSuccess (返回) {List} data.indicators 指标信息,在with_indicators为true时返回
        @apiSuccess (返回) {Int} data.step_id 模型构建&发布完成步骤

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":{},
                "message":"ok",
                "code":"1500200",
                "result":true,
                "data":{
                    "model_id":1,
                    "project_id": 3,
                    "calculation_atom_name":"item_sales_amt",
                    "calculation_atom_alias":"item_sales_amt",
                    "calculation_atom_type":"create",
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
                    "indicator_count": 2,
                    "created_by":"admin",
                    "created_at":"2020-10-18 15:38:56",
                    "updated_by":"admin",
                    "updated_at":"2020-10-18 15:38:56",
                    "indicators":[
                        {
                            "indicator_name":"item_sales_amt_china_1d",
                            "indicator_alias":"国内每天按大区统计道具销售额",
                            "sub_indicators":[
                                {
                                    "indicator_name":"item_sales_amt_china_1d",
                                    "indicator_alias":"国内每天按大区统计道具销售额"
                                }
                            ]
                        }
                    ],
                    "step_id": 3,
                }
            }
        """
        # 鉴权
        calculation_atom_object = get_object(DmmModelCalculationAtom, calculation_atom_name=calculation_atom_name)
        check_perm("datamodel.retrieve", calculation_atom_object.model_id)
        # 获取统计口径详情
        calculation_atom_dict = CalculationAtomManager.get_calculation_atom_info(
            calculation_atom_name, params["with_indicators"]
        )
        return Response(calculation_atom_dict)


class CalculationFunctionViewSet(APIModelViewSet):
    def list(self, request):
        """
        @api {get} /datamanage/datamodel/calculation_functions/ SQL统计函数列表

        @apiVersion 3.5.0
        @apiGroup CalculationFunction
        @apiName datamodel_calculation_function_list
        @apiDescription SQL统计函数列表

        @apiSuccess (返回) {String} data.function_name SQL统计函数名称
        @apiSuccess (返回) {String} data.output_type 输出字段类型

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "function_name": "sum",
                        "output_type": "long"
                    }
                ]
            }
        """
        calculation_functions = CalculationAtomManager.get_calculation_functions()
        return Response(calculation_functions)
