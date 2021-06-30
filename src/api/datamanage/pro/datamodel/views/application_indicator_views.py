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


from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.application.jobs.console import Console
from datamanage.pro.datamodel.dmm.model_instance_manager import ModelInstanceManager
from datamanage.pro.datamodel.mixins.model_instance_mixins import ModelInstanceMixin
from datamanage.pro.datamodel.models.application import DmmModelInstanceIndicator
from datamanage.pro.datamodel.models.model_dict import SCHEDULING_ENGINE_MAPPINGS
from datamanage.pro.datamodel.serializers.application_model import (
    ApplicationIndicatorCreateSerializer,
    ApplicationIndicatorFieldsSerializer,
    ApplicationIndicatorMenuSerializer,
    ApplicationIndicatorUpdateSerializer,
    ApplicationModelInstanceRollback,
)
from datamanage.utils.api import MetaApi
from datamanage.utils.rollback import create_temporary_reversion, rollback_by_id
from rest_framework.response import Response

from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet


class ApplicationInstanceIndicatorViewSet(ModelInstanceMixin, APIModelViewSet):
    """数据模型指标节点应用接口集"""

    lookup_field = "result_table_id"

    @params_valid(serializer=ApplicationIndicatorCreateSerializer)
    def create(self, request, model_instance_id, params):
        """
        @api {post} /datamanage/datamodel/instances/{model_instance_id}/indicators/ 模型实例指标创建

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_create
        @apiDescription 模型实例指标创建

        @apiParam (指标配置) {Number} bk_biz_id 业务ID
        @apiParam (指标配置) {String} result_table_id 指标结果表ID
        @apiParam (指标配置) {String} parent_result_table_id 来源结果表ID（默认为空，直接从主表继承，不为空表示来源于其它指标实例ID）
        @apiParam (关联信息) {Number} flow_node_id 原DataFlow节点ID
        @apiParam (指标配置) {String} calculation_atom_name 统计口径名称
        @apiParam (指标配置) {String[]} [aggregation_fields] 聚合字段列表，允许为空
        @apiParam (指标配置) {String} filter_formula 过滤SQL
        @apiParam (任务配置) {String=stream,batch} scheduling_type 计算类型
        @apiParam (任务配置) {Object} scheduling_content 调度内容
        @apiParam (stream任务配置) {Object} scheduling_content 调度内容
        @apiParam (stream任务配置) {String} scheduling_content.window_type 窗口类型
        @apiParam (stream任务配置) {Number} scheduling_content.count_freq 统计频率
        @apiParam (stream任务配置) {Number} scheduling_content.window_time 窗口长度
        @apiParam (stream任务配置) {Number} scheduling_content.waiting_time 等待时间
        @apiParam (stream任务配置) {Number} scheduling_content.session_gap 会话间隔时间
        @apiParam (stream任务配置) {Number} scheduling_content.expired_time 会话过期时间
        @apiParam (stream任务配置) {Object} scheduling_content.window_lateness 延迟计算参数
        @apiParam (stream任务配置) {Boolean} scheduling_content.window_lateness.allowed_lateness 是否统计延迟数据
        @apiParam (stream任务配置) {Number} scheduling_content.window_lateness.lateness_count_freq 统计是延迟频率
        @apiParam (stream任务配置) {Number} scheduling_content.window_lateness.lateness_time 统计延迟时间
        @apiParam (batch任务配置) {Object} scheduling_content 调度内容
        @apiParam (batch任务配置) {String} scheduling_content.window_type 窗口类型
        @apiParam (batch任务配置) {Boolean} scheduling_content.accumulate 是否累加
        @apiParam (batch任务配置) {Number} scheduling_content.data_start 累加窗口数据起点
        @apiParam (batch任务配置) {Number} scheduling_content.data_end 累加窗口数据终点
        @apiParam (batch任务配置) {Number} scheduling_content.delay 累加窗口延迟时间
        @apiParam (batch任务配置) {Number} scheduling_content.fallback_window 固定窗口长度
        @apiParam (batch任务配置) {Number} scheduling_content.fixed_delay 固定窗口延迟
        @apiParam (batch任务配置) {Number} scheduling_content.count_freq 固定窗口统计频率
        @apiParam (batch任务配置) {Number} scheduling_content.schedule_period 固定窗口统计频率单位
        @apiParam (batch任务配置) {String=unified} scheduling_content.dependency_config_type 依赖配置类型
        @apiParam (batch任务配置) {Object} scheduling_content.unified_config 依赖统一配置
        @apiParam (batch任务配置) {Number} scheduling_content.unified_config.window_size 窗口长度
        @apiParam (batch任务配置) {String} scheduling_content.unified_config.window_size_period 窗口单位
        @apiParam (batch任务配置) {String} scheduling_content.unified_config.dependency_rule 依赖策略
        @apiParam (batch任务配置) {Object} scheduling_content.advanced 高级配置
        @apiParam (batch任务配置) {Boolean} scheduling_content.advanced.recovery_enable 是否失败重试
        @apiParam (batch任务配置) {Number} scheduling_content.advanced.recovery_times 失败重试次数
        @apiParam (batch任务配置) {String} scheduling_content.advanced.recovery_interval 失败重试间隔

        @apiParamExample {json} Request-Example:
            {
                "model_instance_id": 1,
                "bk_biz_id": 591,
                "result_table_id": "591_indicator1",
                "parent_result_table_id": "591_table1",
                "flow_node_id": 1,
                "calculation_atom_name": "login",
                "aggregation_fields": ["dim1", "dim2"],
                "filter_formula": "dim3 = 'haha'",
                "scheduling_type": "stream",
                "scheduing_content": {}
            }

        @apiParamExample {json} 实时计算指标
            {
                "model_instance_id": 1,
                "bk_biz_id": 591,
                "parent_result_table_id": "591_table1",
                "flow_node_id": 1,
                "calculation_atom_name": "login",
                "aggregation_fields": ["dim1", "dim2"],
                "filter_formula": "dim3 = 'haha'",
                "scheduling_type": "stream",
                "scheduing_content": {
                    "window_type": "slide",           # 窗口类型
                    "window_time": 0,                 # 窗口长度
                    "waiting_time": 0,                # 延迟时间
                    "count_freq": 30,                 # 统计频率
                    "window_lateness": {
                        "allowed_lateness": true,        # 是否计算延迟数据
                        "lateness_time": 1,              # 延迟时间
                        "lateness_count_freq": 60        # 延迟统计频率
                    },
                    "session_gap": null,              # 会话窗口时间
                    "expired_time": 60                # 会话过期时间
                }
            }

        @apiParamExample {json} 离线计算指标
            {
                "model_instance_id": 1,
                "bk_biz_id": 591,
                "parent_result_table_id": "591_table1",
                "flow_node_id": 1,
                "calculation_atom_name": "login",
                "aggregation_fields": ["dim1", "dim2"],
                "filter_formula": "dim3 = 'haha'",
                "scheduling_type": "batch",
                "scheduing_content": {
                    "window_type": "fixed",
                    "accumulate": false,
                    "data_start": 0,                   # 累加窗口数据起点
                    "data_end": 23,                    # 累加窗口数据终点
                    "delay": 0,                        # 累加窗口延迟时间
                    "fallback_window": 0,              # 固定窗口长度
                    "fixed_delay": 0,                  # 固定窗口延迟
                    "count_freq": 30,                  # 固定窗口统计频率
                    "schedule_period": "hour",         # 固定窗口统计频率单位
                    "dependency_config_type": "unified" | "custom",
                    "unified_config": {
                        "window_size": 1,
                        "window_size_period": 'hour',
                        "dependency_rule": "all_finished"
                    },
                    "advanced": {
                        "recovery_enable": true,
                        "recovery_times": 1,
                        "recovery_interval": "5m"
                    }
                }
            }

        @apiSuccess (200) {Number} data.model_instance_id 模型应用实例ID
        @apiSuccess (200) {String} data.data_model_sql 数据模型转换后的SQL
        @apiSuccess (200) {String} data.rollback_id 回滚ID
        @apiSuccess (200) {String} data.scheduling_type 计算类型
        @apiSuccess (200) {Object} data.scheduling_content 调度内容（参考请求参数的结构）

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_instance_id": 1,
                    "data_model_sql": "SELECT * FROM 591_source_table",
                    "scheduling_type": "stream",
                    "scheduling_content": {}
                }
            }
        """
        bk_username = get_request_username()
        result_table_id = params["result_table_id"]

        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        # 判断当前数据模型指标所用结果表ID是否已经被占用
        try:
            ModelInstanceManager.get_model_instance_indicator_by_rt(result_table_id)
            raise dm_pro_errors.InstanceIndicatorTableBeenUsedError(message_kv={"result_table_id": result_table_id})
        except dm_pro_errors.InstanceIndicatorNotExistError:
            pass

        watching_models = [DmmModelInstanceIndicator]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                # 创建实例指标
                model_instance_indicator = DmmModelInstanceIndicator.objects.create(
                    result_table_id=result_table_id,
                    bk_biz_id=params["bk_biz_id"],
                    project_id=model_instance.project_id,
                    instance=model_instance,
                    model=model_instance.model,
                    parent_result_table_id=params["parent_result_table_id"],
                    flow_node_id=params["flow_node_id"],
                    calculation_atom_name=params["calculation_atom_name"],
                    aggregation_fields=",".join(params["aggregation_fields"]),
                    filter_formula=params["filter_formula"],
                    scheduling_type=params["scheduling_type"],
                    scheduling_content=params["scheduling_content"],
                    created_by=bk_username,
                )

                # 判断当前数据模型实例是否有相同含义的指标
                ModelInstanceManager.validate_same_indicator(model_instance, model_instance_indicator)

                # 根据模型应用主表信息和模型应用字段映射规则生成数据模型的SQL
                sql_generate_params = self.prepare_ind_sql_generate_params(model_instance, model_instance_indicator)
                engine = SCHEDULING_ENGINE_MAPPINGS[params["scheduling_type"]].value
                data_model_sql = Console(sql_generate_params).build(engine=engine)

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "data_model_sql": data_model_sql,
                "rollback_id": rollback_id,
                "scheduling_type": params["scheduling_type"],
                "scheduling_content": params["scheduling_content"],
            }
        )

    @params_valid(serializer=ApplicationIndicatorUpdateSerializer)
    def update(self, request, model_instance_id, result_table_id, params):
        """
        @api {put} /datamanage/datamodel/instances/{model_instance_id}/indicators/{result_table_id}/ 模型实例指标更新

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_update
        @apiDescription 模型实例指标更新

        @apiParam (指标配置) {String} [parent_result_table_id] 来源结果表ID（默认为空，直接从主表继承，不为空表示来源于其它指标实例ID）？
        @apiParam (指标配置) {String} [calculation_atom_name] 统计口径名称
        @apiParam (指标配置) {String[]} [aggregation_fields] 聚合字段列表，允许为空
        @apiParam (指标配置) {String} [filter_formula] 过滤SQL
        @apiParam (任务配置) {String} [scheduling_type] 计算类型
        @apiParam (任务配置) {Object} [scheduling_content] 调度内容（参考创建请求参数的结构）

        @apiParamExample {json} Request-Example:
            {
                "parent_result_table_id": "591_table1",
                "calculation_atom_name": "login",
                "aggregation_fields": ["dim1", "dim2"],
                "filter_formula": "dim3 = 'haha'",
                "scheduling_type": "stream",
                "scheduing_content": {}
            }

        @apiSuccess (200) {Number} data.model_instance_id 模型应用实例ID
        @apiSuccess (200) {String} data.data_model_sql 数据模型转换后的SQL
        @apiSuccess (200) {String} data.rollback_id 回滚ID
        @apiSuccess (200) {String} data.scheduling_type 计算类型
        @apiSuccess (200) {Object} data.scheduling_content 调度内容（参考创建请求参数的结构）

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_instance_id": 1,
                    "data_model_sql": "SELECT * FROM 591_source_table",
                    "scheduling_type": "stream",
                    "scheduling_content": {}
                }
            }
        """
        bk_username = get_request_username()

        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        watching_models = [DmmModelInstanceIndicator]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                # 更新实例指标
                params["updated_by"] = bk_username
                if "aggregation_fields" in params and isinstance(params["aggregation_fields"], list):
                    params["aggregation_fields"] = ",".join(params["aggregation_fields"])

                try:
                    instance_indicator = DmmModelInstanceIndicator.objects.filter(
                        instance=model_instance,
                        result_table_id=result_table_id,
                    ).get()
                    for row_key, row_value in list(params.items()):
                        setattr(instance_indicator, row_key, row_value)
                    instance_indicator.save()
                except DmmModelInstanceIndicator.DoesNotExist:
                    raise dm_pro_errors.InstanceIndicatorNotExistError(
                        message_kv={
                            "result_table_id": result_table_id,
                        }
                    )

                # 判断当前数据模型实例是否有相同含义的指标
                ModelInstanceManager.validate_same_indicator(model_instance, instance_indicator)

                # 根据模型应用主表信息和模型应用字段映射规则生成数据模型的SQL
                sql_generate_params = self.prepare_ind_sql_generate_params(model_instance, instance_indicator)
                engine = SCHEDULING_ENGINE_MAPPINGS[params["scheduling_type"]].value
                data_model_sql = Console(sql_generate_params).build(engine=engine)

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "data_model_sql": data_model_sql,
                "rollback_id": rollback_id,
                "scheduling_type": params["scheduling_type"],
                "scheduling_content": params["scheduling_content"],
            }
        )

    def retrieve(self, request, model_instance_id, result_table_id):
        """
        @api {get} /datamanage/datamodel/instances/{model_instance_id}/indicators/{result_table_id}/ 模型实例指标获取

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_retrieve
        @apiDescription 模型应用实例获取

        @apiSuccess (200) {Number} data.model_instance_id 模型实例ID
        @apiSuccess (200) {Number} data.bk_biz_id 业务ID
        @apiSuccess (200) {String} data.result_table_id 指标结果表ID
        @apiSuccess (200) {String} data.parent_result_table_id 来源结果表ID
        @apiSuccess (200) {String} calculation_atom_name 统计口径名称
        @apiSuccess (200) {String} aggregation_fields 聚合字段列表，允许为空
        @apiSuccess (200) {String} filter_formula 过滤SQL
        @apiSuccess (200) {String} scheduling_type 计算类型
        @apiSuccess (200) {Object} scheduling_content 调度内容（参考创建请求参数的结构）

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_instance_id": 1,
                    "bk_biz_id": 591,
                    "result_table_id": "591_indicator_table1",
                    "parent_result_table_id": "591_table1",
                    "calculation_atom_name": "login",
                    "aggregation_fields": ["dim1", "dim2"],
                    "filter_formula": "dim3 = 'haha'",
                    "flow_node_id": 1,
                    "scheduling_type": "stream",
                    "scheduing_content": {}
                }
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        try:
            instance_indicator = DmmModelInstanceIndicator.objects.filter(
                instance=model_instance,
                result_table_id=result_table_id,
            ).get()
        except DmmModelInstanceIndicator.DoesNotExist:
            raise dm_pro_errors.InstanceIndicatorNotExistError(
                message_kv={
                    "result_table_id": result_table_id,
                }
            )

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "bk_biz_id": instance_indicator.bk_biz_id,
                "result_table_id": instance_indicator.result_table_id,
                "parent_result_table_id": instance_indicator.parent_result_table_id,
                "calculation_atom_name": instance_indicator.calculation_atom_name,
                "aggregation_fields": instance_indicator.aggregation_fields.split(","),
                "filter_formula": instance_indicator.filter_formula,
                "flow_node_id": instance_indicator.flow_node_id,
                "scheduling_type": instance_indicator.scheduling_type,
                "scheduling_content": instance_indicator.scheduling_content,
            }
        )

    def destroy(self, request, model_instance_id, result_table_id):
        """
        @api {delete} /datamanage/datamodel/instances/{model_instance_id}/indicators/{result_table_id}/ 模型实例指标删除

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_delete
        @apiDescription 模型应用实例删除

        @apiSuccess (200) {Number} data.model_instance_id 数据模型应用实例
        @apiSuccess (200) {String} data.result_table_id 应用实例指标结果表ID
        @apiSuccess (200) {String} data.rollback_id 回滚ID

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_instance_id": 1,
                    "result_table_id": "591_indicator_table1"
                }
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        watching_models = [DmmModelInstanceIndicator]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                try:
                    DmmModelInstanceIndicator.objects.filter(
                        instance=model_instance,
                        result_table_id=result_table_id,
                    ).delete()
                except DmmModelInstanceIndicator.DoesNotExist:
                    raise dm_pro_errors.InstanceIndicatorNotExistError(
                        message_kv={
                            "result_table_id": result_table_id,
                        }
                    )

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "result_table_id": result_table_id,
                "rollback_id": rollback_id,
            }
        )

    @detail_route(methods=["post"], url_path="rollback")
    @params_valid(serializer=ApplicationModelInstanceRollback)
    def rollback(self, request, model_instance_id, result_table_id, params):
        """
        @api {post} /datamanage/datamodel/instances/{model_instance_id}/indicators/{result_table_id}/rollback/
            模型实例指标操作回滚

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_rollback
        @apiDescription 模型实例指标操作回滚

        @apiParam (实例) {String} rollback_id 回滚ID，创建、更新、删除接口都会返回

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {}
            }
        """
        rollback_by_id(params["rollback_id"])
        return Response(model_instance_id)

    @list_route(methods=["get"], url_path="calculation_atoms")
    def calculation_atoms(self, request, model_instance_id):
        """
        @api {get} /datamanage/datamodel/instances/{model_instance_id}/indicators/calculation_atoms/ 数据模型实例指标可用统计口径列表

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_calculation_atoms
        @apiDescription 数据模型实例指标可用统计口径列表

        @apiSuccess (200) {Object[]} data 统计口径列表
        @apiSuccess (200) {String} data.calculation_atom_name 统计口径英文名
        @apiSuccess (200) {String} data.calculation_atom_alias 统计口径中文名
        @apiSuccess (200) {String} data.calculation_formula 统计口径格式化展示
        @apiSuccess (200) {String} data.atadescription 统计口径描述

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "calculation_atom_name": "sum_metric1",
                        "calculation_atom_alias": "汇总字段1",
                        "calculation_formula": "sum(metric1)",
                        "description": ""
                    },
                    {
                        "calculation_atom_name": "country_count",
                        "calculation_atom_alias": "国家数量",
                        "calculation_formula": "count(distinct country)",
                        "description": ""
                    }
                ]
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_id=model_instance.model_id,
            version_id=model_instance.version_id,
        )

        instance_calculation_atoms = []
        for calculation_atom_info in model_release.model_content.get("model_detail", {}).get("calculation_atoms", []):
            instance_calculation_atoms.append(
                {
                    "calculation_atom_name": calculation_atom_info.get("calculation_atom_name"),
                    "calculation_atom_alias": calculation_atom_info.get("calculation_atom_alias"),
                    "calculation_formula": calculation_atom_info.get("calculation_formula"),
                    "description": calculation_atom_info.get("description"),
                }
            )

        return Response(instance_calculation_atoms)

    @list_route(methods=["get"], url_path="optional_fields")
    @params_valid(serializer=ApplicationIndicatorFieldsSerializer)
    def optional_fields(self, request, model_instance_id, params):
        """
        @api {get} /datamanage/datamodel/instances/{model_instance_id}/indicators/optional_fields/ 数据模型实例指标可选字段列表

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_optional_fields
        @apiDescription 数据模型实例指标可选字段列表

        @apiParam {String} parent_result_table_id 上游结果表ID
        @apiParam {String=dimension,measure} field_category 字段分类

        @apiSuccess (200) {Object[]} data 可选字段列表
        @apiSuccess (200) {String} data.field_name 可选字段名称
        @apiSuccess (200) {String} data.field_alias 可选字段中文名
        @apiSuccess (200) {String} data.description 可选字段描述

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "field_name": "field1",
                        "field_alias": "字段1",
                        "description": ""
                    },
                    {
                        "field_name": "field2",
                        "field_alias": "字段2",
                        "description": ""
                    }
                ]
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_id=model_instance.model_id,
            version_id=model_instance.version_id,
        )

        result_table_fields = MetaApi.result_tables.fields(
            {"result_table_id": params["parent_result_table_id"]},
            raise_exception=True,
        ).data
        parent_fields = {item.get("field_name") for item in result_table_fields}

        optional_fields = []
        for instance_field_info in model_release.model_content.get("model_detail", {}).get("fields", []):
            # 如果上游结果表中不包含当前字段，说明数据模型应用过程中，实例的字段被聚合掉了
            if instance_field_info.get("field_name") not in parent_fields:
                continue

            # 如果需要对字段分类进行过滤且不满足，则忽略该字段
            if "field_category" in params and params["field_category"]:
                if instance_field_info.get("field_category") != params["field_category"]:
                    continue

            optional_fields.append(
                {
                    "field_name": instance_field_info.get("field_name"),
                    "field_alias": instance_field_info.get("field_alias"),
                    "description": instance_field_info.get("description"),
                }
            )

        return Response(optional_fields)


class ApplicationIndicatorViewSet(ModelInstanceMixin, APIModelViewSet):
    """数据模型应用指标通用接口集（不需要指定实例ID）"""

    lookup_field = "result_table_id"

    @list_route(methods=["get"], url_path="menu")
    @params_valid(serializer=ApplicationIndicatorMenuSerializer)
    def indicator_menu(self, request, params):
        """
        @api {get} /datamanage/datamodel/instances/indicators/menu/ 数据模型应用指标目录

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_indicator_menu
        @apiDescription 数据模型应用指标目录

        @apiParam {String} [model_instance_id] 数据模型实例ID
        @apiParam {String} [node_id] 数据流节点ID（数据模型实例ID和节点ID需要至少有其中一个作为参数）

        @apiSuccess (200) {Object} data 数据模型指标目录结构
        @apiSuccess (200) {Object[]} data.tree 指标目录结构
        @apiSuccess (200) {String} data.tree.id 目录结构条目唯一ID
        @apiSuccess (200) {String} data.tree.alias 目录结构条目中文名
        @apiSuccess (200) {String} data.tree.name 目录结构条目英文名
        @apiSuccess (200) {String} data.tree.type 目录结构条目类型
        @apiSuccess (200) {Boolean} data.tree.folder 目录结构条目是否文件夹
        @apiSuccess (200) {Number} data.tree.count 目录结构文件夹指标数
        @apiSuccess (200) {Object[]} data.tree.children 目录结构子条目
        @apiSuccess (200) {String[]} data.selected_nodes 已使用的指标节点

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "tree": [
                        {
                            "id": "xxx",
                            "alias": "中文",
                            "name": "英文名",
                            "type": "data_model",
                            "folder": true,
                            "count": 2,
                            "children": [
                                {
                                    "id": "yyy",
                                    "alias": "中文",
                                    "name": "英文名",
                                    "type": "calculation_atom",
                                    "folder": true,
                                    "count": 2,
                                    "children": [
                                        {
                                            "id": "abc",
                                            "alias": "中文",
                                            "name": "英文名",
                                            "type": "batch_indicator",
                                            "folder": false,
                                            "count": 1,
                                            "children": []
                                        },
                                        {
                                            "id": "def",
                                            "alias": "中文",
                                            "name": "英文名",
                                            "type": "stream_indicator",
                                            "folder": false,
                                            "count": 1,
                                            "children": []
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                    "selected_nodes": ["abc", "def"]
                }
            }
        """
        if "model_instance_id" in params:
            model_instance = ModelInstanceManager.get_model_instance_by_id(params["model_instance_id"])
        elif "node_id" in params:
            model_instance = ModelInstanceManager.get_model_instance_by_node_id(params["node_id"])

        # 获取该模型下的所有指标
        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_id=model_instance.model_id,
            version_id=model_instance.version_id,
        )
        model_indicators = model_release.model_content.get("model_detail", {}).get("indicators", [])

        # 获取当前模型实例已有的指标
        instance_indicators = ModelInstanceManager.get_model_instance_indicators(model_instance)

        selected_indicator_names = ModelInstanceManager.filter_inds_by_instance_inds(
            model_indicators, instance_indicators
        )

        menu_content = ModelInstanceManager.generate_indicators_menu(
            model_release.model_content, selected_indicator_names
        )

        menu_content.update(
            {
                "project_id": model_instance.model.project_id,
                "model_id": model_instance.model.model_id,
            }
        )
        return Response(menu_content)
