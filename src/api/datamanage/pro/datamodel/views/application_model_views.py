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


import copy
import json

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.application.jobs.console import Console
from datamanage.pro.datamodel.dmm.model_instance_flow_manager import (
    ModelInstanceFlowManager,
    VirtualNodeGenerator,
)
from datamanage.pro.datamodel.dmm.model_instance_manager import ModelInstanceManager
from datamanage.pro.datamodel.mixins.model_instance_mixins import ModelInstanceMixin
from datamanage.pro.datamodel.models.application import (
    DmmModelInstance,
    DmmModelInstanceField,
    DmmModelInstanceIndicator,
    DmmModelInstanceRelation,
    DmmModelInstanceSource,
    DmmModelInstanceTable,
)
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo
from datamanage.pro.datamodel.models.model_dict import (
    RELATION_STORAGE_CLUSTER_TYPE,
    DataModelActiveStatus,
    DataModelPublishStatus,
    DataModelType,
    InstanceInputType,
    ModelTypeConfigs,
    SchedulingEngineType,
)
from datamanage.pro.datamodel.serializers.application_model import (
    ApplicationDataflowCheckSerializer,
    ApplicationFieldsMappingsSerializer,
    ApplicationInstanceByTableSerializer,
    ApplicationInstanceDataflowSerializer,
    ApplicationInstanceInitSerializer,
    ApplicationModelCreateSerializer,
    ApplicationModelInstanceRollback,
    ApplicationModelUpdateSerializer,
    ApplicationReleasedModelListSerializer,
)
from datamanage.utils.api import DataflowApi, MetaApi
from datamanage.utils.rollback import create_temporary_reversion, rollback_by_id
from django.db import IntegrityError
from django.db.models import Q
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from common.decorators import detail_route, list_route, params_valid
from common.exceptions import ApiResultError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet


class ApplicationModelInstanceViewSet(ModelInstanceMixin, APIModelViewSet):
    """数据模型节点应用接口集"""

    lookup_field = "model_instance_id"

    @params_valid(serializer=ApplicationModelCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/datamodel/instances/ 模型应用实例创建

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_create
        @apiDescription 模型应用实例创建

        @apiParam (实例) {Number} model_id 数据模型ID
        @apiParam (实例) {String} version_id 数据模型版本ID
        @apiParam (实例) {Number} [model_instance_id] 数据模型实例ID（允许先创建数据模型实例，再创建任务）
        @apiParam (实例) {Number} project_id 项目ID
        @apiParam (主表) {Number} bk_biz_id 业务ID
        @apiParam (主表) {String} result_table_id 主表结果表ID
        @apiParam (实例) {Number} flow_id 当前应用数据流ID
        @apiParam (主表) {Number} flow_node_id 原Flow节点ID
        @apiParam (主表) {Object[]} from_result_tables 输入结果表列表
        @apiParam (主表) {String} flow_result_tables.result_table_id 输入结果表ID
        @apiParam (主表) {String} flow_result_tables.node_type 输入结果表所在节点类型
        @apiParam (字段) {Object[]} fields 主表字段映射
        @apiParam (字段) {String} fields.field_name 输出字段
        @apiParam (字段) {String} [fields.input_result_table_id] 输入结果表
        @apiParam (字段) {String} [fields.input_field_name] 输入字段名
        @apiParam (字段) {String} [fields.input_table_field] 数据结果表ID/输入字段名
        @apiParam (字段) {Object} fields.application_clean_content 应用阶段清洗规则
        @apiParam (字段) {Object} [fields.relation] 维表关联信息
        @apiParam (字段) {Number} feilds.relation.related_model_id 维表关联模型ID
        @apiParam (字段) {String} [fields.relation.input_result_table_id] 维表关联输入结果表
        @apiparam (字段) {String} [fields.relation.input_field_name] 维表关联输入字段名
        @apiparam (字段) {String} [fields.relation.input_table_field] 数据结果表ID/输入字段名

        @apiParamExample {json} Request-Example:
            {
                "model_id": 1,
                "version_id": "1.0",
                "project_id": 1,
                "bk_biz_id": 591,
                "result_table_id": "591_table1",
                "flow_id": 1,
                "flow_node_id": 100,
                "from_result_tables": [
                    {
                        "result_table_id": "591_source_table1",
                        "node_type": "realtime_source"
                    },
                    {
                        "result_table_id": 591_source_table2",
                        "node_type": "realtime_relation_source"
                    }
                ],
                "fields": [
                    {
                        "field_name": "field1",
                        "input_result_table_id": "591_source_table1",
                        "input_field_name": "source_field1",
                        "input_table_field": "591_source_table1/source_field1",
                        "application_clean_content": {}
                    },
                    {
                        "field_name": "field2",
                        "input_result_table_id": "591_source_table2",
                        "input_field_name": "source_dim1",
                        "input_table_field": "591_source_table2/source_dim1",
                        "application_clean_content": {},
                        "relation": {
                            "related_model_id": 2,
                            "input_result_table_id": "591_dim_table1",
                            "input_field_name": "source_dim1",
                            "input_table_field": "591_dim_table1/source_dim1"
                        }
                    }
                ]
            }

        @apiSuccess (200) {Number} data.model_instance_id 模型应用实例ID
        @apiSuccess (200) {String} data.data_model_sql 数据模型转换后的SQL
        @apiSuccess (200) {String} data.rollback_id 回滚ID
        @apiSuccess (200) {String} data.scheduling_type 计算类型
        @apiSuccess (200) {Object} data.scheduling_content 调度内容
        @apiSuccess (200) {String} data.scheduling_content.window_type 窗口类型
        @apiSuccess (200) {Number} data.scheduling_content.count_freq 统计频率
        @apiSuccess (200) {Number} data.scheduling_content.window_time 窗口长度
        @apiSuccess (200) {Number} data.scheduling_content.waiting_time 等待时间
        @apiSuccess (200) {Number} data.scheduling_content.session_gap 会话间隔时间
        @apiSuccess (200) {Number} data.scheduling_content.expired_time 会话过期时间
        @apiSuccess (200) {Object} data.scheduling_content.window_lateness 延迟计算参数
        @apiSuccess (200) {Boolean} data.scheduling_content.window_lateness.allowed_lateness 是否统计延迟数据
        @apiSuccess (200) {Number} data.scheduling_content.window_lateness.lateness_count_freq 统计是延迟频率
        @apiSuccess (200) {Number} data.scheduling_content.window_lateness.lateness_time 统计延迟时间

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
                    "rollback_id": "xxx",
                    "scheduling_type": "stream",
                    "scheduling_content": {
                        "window_type": "none"
                    }
                }
            }
        """
        model_id = params["model_id"]
        bk_username = get_request_username()

        watching_models = [
            DmmModelInstance,
            DmmModelInstanceTable,
            DmmModelInstanceSource,
            DmmModelInstanceField,
            DmmModelInstanceRelation,
        ]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                model_release = ModelInstanceManager.get_model_and_check_latest_version(model_id, params["version_id"])

                # 如果参数中传入已经创建的模型实例ID，则可以基于该实例创建主表和字段映射
                if "model_instance_id" in params and params["model_instance_id"]:
                    model_instance = ModelInstanceManager.get_model_instance_by_id(params["model_instance_id"])
                else:
                    # 根据最新模型版本ID创建模型实例
                    model_instance = DmmModelInstance.objects.create(
                        project_id=params["project_id"],
                        model_id=model_id,
                        version_id=model_release.version_id,
                        flow_id=params["flow_id"],
                        created_by=bk_username,
                    )

                instance_sources = ModelInstanceManager.save_model_instance_sources(
                    model_instance, params["from_result_tables"]
                )

                # 创建模型实例主表
                try:
                    DmmModelInstanceTable.objects.create(
                        result_table_id=params["result_table_id"],
                        bk_biz_id=params["bk_biz_id"],
                        instance=model_instance,
                        model_id=model_id,
                        flow_node_id=params["flow_node_id"],
                        created_by=bk_username,
                    )
                except IntegrityError:
                    raise dm_pro_errors.ModelInstanceTableBeenUsedError(
                        message_kv={
                            "result_table_id": params["result_table_id"],
                        }
                    )

                # 创建模型应用时字段映射
                instance_table_fields = ModelInstanceManager.create_instance_table_fields(
                    model_instance,
                    params["fields"],
                )

                # 输入信息校验，详情见各个校验函数的docstring
                ModelInstanceManager.validate_model_structure(instance_table_fields, model_release.model_content)
                ModelInstanceManager.validate_input_table(
                    instance_table_fields,
                    instance_sources,
                    model_release.model_content,
                )
                ModelInstanceManager.validate_instance_table_fields(instance_table_fields, instance_sources)

                # 校验组装SQL的内容是否能生成出合法的SQL
                ModelInstanceManager.validate_data_model_sql(
                    params["bk_biz_id"],
                    instance_sources,
                    params["result_table_id"],
                    instance_table_fields,
                    model_release.model_content,
                )

                # 根据模型应用主表信息和模型应用字段映射规则生成数据模型的SQL
                sql_generate_params = self.prepare_fact_sql_generate_params(
                    model_instance=model_instance,
                    model_info=model_release.model_content,
                    instance_sources=instance_sources,
                    instance_table_fields=instance_table_fields,
                )
                data_model_sql = Console(sql_generate_params).build(engine=SchedulingEngineType.FLINK.value)
                logger.info("Generate data model sql: {}".format(data_model_sql))

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "rollback_id": rollback_id,
                "data_model_sql": data_model_sql,
                "scheduling_type": "stream",
                "scheduling_content": {"window_type": "none"},
            }
        )

    @params_valid(serializer=ApplicationModelUpdateSerializer)
    def update(self, request, model_instance_id, params):
        """
        @api {put} /datamanage/datamodel/instances/{model_instance_id}/ 模型应用实例更新

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_update
        @apiDescription 模型应用实例更新

        @apiParam (实例) {Number} [model_id] 数据模型ID
        @apiParam (实例) {String} [version_id] 数据模型版本ID
        @apiParam (实例) {Boolean} [upgrade_version=false] 是否更新模型版本到最新版本
        @apiParam (主表) {Object[]} from_result_tables 输入结果表列表
        @apiParam (主表) {String} flow_result_tables.result_table_id 输入结果表ID
        @apiParam (主表) {String} flow_result_tables.node_type 输入结果表所在节点类型
        @apiParam (字段) {Object[]} fields 主表字段映射
        @apiParam (字段) {String} fields.field_name 输出字段
        @apiParam (字段) {String} [fields.input_result_table_id] 输入结果表
        @apiParam (字段) {String} [fields.input_field_name] 输入字段名
        @apiParam (字段) {String} [fields.input_table_field] 数据结果表ID/输入字段名
        @apiParam (字段) {Object} fields.application_clean_content 应用阶段清洗规则
        @apiParam (字段) {Object} [fields.relation] 维表关联信息
        @apiParam (字段) {Number} feilds.relation.model_id 维表关联模型ID
        @apiParam (字段) {String} [fields.relation.input_result_table_id] 维表关联输入结果表
        @apiparam (字段) {String} [fields.relation.input_field_name] 维表关联输入字段名
        @apiparam (字段) {String} [fields.relation.input_table_field] 数据结果表ID/输入字段名

        @apiParamExample {json} Request-Example:
            {
                "model_id": 2,
                "version_id": "2.0",
                "upgrade_version": false,
                "from_result_tables": [
                    {
                        "result_table_id": "591_source_table1",
                        "node_type": "realtime_source"
                    },
                    {
                        "result_table_id": 591_source_table2",
                        "node_type": "realtime_relation_source"
                    }
                ],
                "fields": [
                    {
                        "field_name": "field1",
                        "input_result_table_id": "591_source_table1",
                        "input_field_name": "source_field1",
                        "input_table_field": "591_source_table1/source_field1",
                        "application_clean_content": {}
                    },
                    {
                        "field_name": "field2",
                        "input_result_table_id": "591_source_table2",
                        "input_field_name": "source_dim1",
                        "input_table_field": "591_source_table2/source_dim1",
                        "application_clean_content": {},
                        "relation": {
                            "related_model_id": 2,
                            "input_result_table_id": "591_dim_table1",
                            "input_field_name": "source_dim1",
                            "input_table_field": "591_dim_table1/source_dim1"
                        }
                    }
                ]
            }

        @apiSuccess (200) {Number} data.model_instance_id 模型应用实例ID
        @apiSuccess (200) {String} data.data_model_sql 数据模型转换后的SQL
        @apiSuccess (200) {String} data.rollback_id 回滚ID
        @apiSuccess (200) {String} data.scheduling_type 计算类型
        @apiSuccess (200) {Object} data.scheduling_content 调度内容（参考创建请求返回的结构）

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
                    "rollback_id": "xxx",
                    "scheduling_type": "stream",
                    "scheduling_content": {}
                }
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)
        instance_table = ModelInstanceManager.get_model_instance_table_by_id(model_instance_id)

        # 准备构建数据模型SQL需要的参数
        fact_sql_base_params = {
            "model_instance": model_instance,
            "instance_sources": None,
            "model_info": None,
            "instance_table_fields": None,
        }
        watching_models = [
            DmmModelInstance,
            DmmModelInstanceTable,
            DmmModelInstanceSource,
            DmmModelInstanceField,
            DmmModelInstanceRelation,
        ]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                # 更新模型实例信息（模型ID和模型版本ID）
                if "model_id" in params or "version_id" in params:
                    ModelInstanceManager.update_model_instance(model_instance, params, fact_sql_base_params)

                if "model_id" in params:
                    ModelInstanceManager.update_model_instance_table(model_instance, params["model_id"])

                # 更新模型输入表信息
                if "from_result_tables" in params:
                    fact_sql_base_params["instance_sources"] = ModelInstanceManager.save_model_instance_sources(
                        model_instance, params["from_result_tables"]
                    )
                else:
                    fact_sql_base_params["instance_sources"] = ModelInstanceManager.get_instance_sources_by_instance(
                        model_instance
                    )

                # 更新模型字段映射信息
                if "fields" in params:
                    fact_sql_base_params["instance_table_fields"] = ModelInstanceManager.update_instance_table_fields(
                        model_instance, params["fields"]
                    )
                else:
                    fact_sql_base_params["instance_table_fields"] = ModelInstanceManager.get_inst_table_fields_by_inst(
                        model_instance
                    )

                # 由于校验信息和生成SQL都依赖模型发布版本的信息，如果在上面更新模型实例时没有获取到，则另外获取
                if not fact_sql_base_params["model_info"]:
                    model_release = ModelInstanceManager.get_model_release_by_id_and_version(
                        model_instance.model_id,
                        model_instance.version_id,
                    )
                    fact_sql_base_params["model_info"] = model_release.model_content

                # 输入信息校验，详情见各个校验函数的docstring
                ModelInstanceManager.validate_model_structure(
                    fact_sql_base_params["instance_table_fields"],
                    fact_sql_base_params["model_info"],
                )
                ModelInstanceManager.validate_input_table(
                    fact_sql_base_params["instance_table_fields"],
                    fact_sql_base_params["instance_sources"],
                    fact_sql_base_params["model_info"],
                )
                ModelInstanceManager.validate_instance_table_fields(
                    fact_sql_base_params["instance_table_fields"],
                    fact_sql_base_params["instance_sources"],
                )

                # 校验组装SQL的内容是否能生成出合法的SQL
                ModelInstanceManager.validate_data_model_sql(
                    instance_table.bk_biz_id,
                    fact_sql_base_params["instance_sources"],
                    instance_table.result_table_id,
                    fact_sql_base_params["instance_table_fields"],
                    fact_sql_base_params["model_info"],
                )

                # 根据模型应用主表信息和模型应用字段映射规则生成数据模型的SQL
                sql_generate_params = self.prepare_fact_sql_generate_params(**fact_sql_base_params)
                data_model_sql = Console(sql_generate_params).build(engine=SchedulingEngineType.FLINK.value)
                logger.info("Update data model sql: {}".format(data_model_sql))

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "data_model_sql": data_model_sql,
                "rollback_id": rollback_id,
                "scheduling_type": "stream",
                "scheduling_content": {},
            }
        )

    def retrieve(self, request, model_instance_id):
        """
        @api {get} /datamanage/datamodel/instances/{model_instance_id}/ 模型应用实例获取

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_retrieve
        @apiDescription 模型应用实例获取

        @apiSuccess (200) {Number} data.instance_id 数据模型应用实例ID
        @apiSuccess (200) {Number} data.model_id 数据模型ID
        @apiSuccess (200) {String} data.version_id 数据模型版本ID
        @apiSuccess (200) {Number} data.bk_biz_id 业务ID
        @apiSuccess (200) {String} data.result_table_id 主表结果表ID
        @apiSuccess (200) {Object[]} fields 主表字段映射
        @apiSuccess (200) {String} fields.field_name 输出字段
        @apiSuccess (200) {String} fields.input_result_table_id 输入结果表
        @apiSuccess (200) {String} fields.input_field_name 输入字段名
        @apiSuccess (200) {String} fields.input_table_field 输入结果表ID/输入字段名
        @apiSuccess (200) {Object} fields.application_clean_content 应用阶段清洗规则
        @apiSuccess (200) {Object} [fields.relation] 维表关联信息
        @apiSuccess (200) {Number} feilds.relation.model_id 维表关联模型ID
        @apiSuccess (200) {String} fields.relation.input_result_table_id 维表关联输入结果表
        @apiSuccess (200) {String} fields.relation.input_field_name 维表关联输入字段名
        @apiSuccess (200) {String} fields.relation.input_table_field 维度关联输入表/字段名

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "instance_id": 1,
                    "model_id": 1,
                    "version_id": "1.0",
                    "bk_biz_id": 591,
                    "result_table_id": "591_table1",
                    "fields": [
                        {
                            "field_name": "field1",
                            "input_result_table_id": "591_source_table1",
                            "input_field_name": "source_field1",
                            "input_table_field": "591_source_table1/source_field1",
                            "application_clean_content": {}
                        },
                        {
                            "field_name": "field2",
                            "input_result_table_id": "591_source_table2",
                            "input_field_name": "source_dim1",
                            "input_table_field": "591_source_table2/source_dim1",
                            "application_clean_content": {},
                            "relation": {
                                "related_model_id": 2,
                                "input_result_table_id": "591_dim_table1",
                                "input_field_name": "source_dim1",
                                "input_table_field": "591_dim_table1/source_dim1"
                            }
                        }
                    ]
                }
            }
        """
        model_instance = ModelInstanceManager.get_model_instance_by_id(model_instance_id)

        model_instance_table = ModelInstanceManager.get_model_instance_table_by_id(model_instance_id)

        fields = ModelInstanceManager.generate_instance_fields_mappings(model_instance)

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "model_id": model_instance.model_id,
                "version_id": model_instance.version_id,
                "bk_biz_id": model_instance_table.bk_biz_id,
                "result_table_id": model_instance_table.result_table_id,
                "fields": fields,
            }
        )

    def destroy(self, request, model_instance_id):
        """
        @api {delete} /datamanage/datamodel/instances/{model_instance_id}/ 模型应用实例删除

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_delete
        @apiDescription 模型应用实例删除

        @apiSuccess (200) {Number} data.model_instance_id 数据模型实例ID

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    'model_instance_id': 1,
                    'rollback_id': 'xxx'
                }
            }
        """
        indicators = DmmModelInstanceIndicator.objects.filter(instance_id=model_instance_id)
        if indicators.count() > 0:
            raise dm_pro_errors.InstanceStillHasIndicatorsError()

        watching_models = [
            DmmModelInstance,
            DmmModelInstanceTable,
            DmmModelInstanceSource,
            DmmModelInstanceField,
            DmmModelInstanceRelation,
        ]
        with create_temporary_reversion(watching_models, expired_time=300) as rollback_id:
            with auto_meta_sync(using="bkdata_basic"):
                DmmModelInstanceRelation.objects.filter(instance_id=model_instance_id).delete()
                DmmModelInstanceField.objects.filter(instance_id=model_instance_id).delete()
                DmmModelInstanceTable.objects.filter(instance_id=model_instance_id).delete()
                DmmModelInstanceSource.objects.filter(instance_id=model_instance_id).delete()
                DmmModelInstance.objects.filter(instance_id=model_instance_id).delete()

        return Response(
            {
                "model_instance_id": int(model_instance_id),
                "rollback_id": rollback_id,
            }
        )

    @detail_route(methods=["post"], url_path="rollback")
    @params_valid(serializer=ApplicationModelInstanceRollback)
    def rollback(self, request, model_instance_id, params):
        """
        @api {post} /datamanage/datamodel/instances/{model_instance_id}/rollback/ 模型应用实例操作回滚

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_rollback
        @apiDescription 模型应用实例操作回滚

        @apiParam {String} rollback_id 回滚ID，创建、更新、删除接口都会返回

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

    @list_route(methods=["get"], url_path="released_models")
    @params_valid(serializer=ApplicationReleasedModelListSerializer)
    def released_models(self, request, params):
        """
        @api {get} /datamanage/datamodel/instances/released_models/ 已发布模型列表

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_released_models
        @apiDescription 已发布模型列表

        @apiParam {String} project_id 项目ID
        @apiParam {Boolean} is_open 是否公开

        @apiSuccess (200) {Object[]} data 已分组的已发布模型列表
        @apiSuccess (200) {String} data.model_type 数据模型类型
        @apiSuccess (200) {String} data.model_type_alias 数据模型类型别名
        @apiSuccess (200) {Object[]} data.models 数据模型分类下已发布的模型信息列表
        @apiSuccess (200) {Number} data.models.model_id 模型ID
        @apiSuccess (200) {String} data.models.model_name 模型磨成
        @apiSuccess (200) {String} data.models.model_alias 模型别名
        @apiSuccess (200) {String} data.models.model_type 模型类型
        @apiSuccess (200) {String} data.models.latest_version_id 模型最新版本ID

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "model_type": "fact_table",
                        "model_type_alias": "事实表数据模型",
                        "models": [
                            {
                                "model_id": 1,
                                "model_name": "danny_fact_model",
                                "model_alias": "danny事实表模型",
                                "model_type": "fact_table",
                                "latest_version_id": "xxxx"
                            },
                        ],
                    },
                    {
                        "model_type": "dimension_table",
                        "model_type_alias": "维度表数据模型",
                        "models": [
                            {
                                "model_id": 2,
                                "model_name": "danny_dim_model",
                                "model_alias": "danny维度表模型",
                                "model_type": "dimension_table",
                                "latest_version_id": "yyyy"
                            }
                        ]
                    }
                ]
            }
        """
        released_models = copy.deepcopy(ModelTypeConfigs)
        for model_type_info in list(released_models.values()):
            model_type_info["models"] = []

        model_queryset = (
            DmmModelInfo.objects.filter(active_status=DataModelActiveStatus.ACTIVE.value)
            .filter(
                Q(publish_status=DataModelPublishStatus.PUBLISHED.value)
                | Q(publish_status=DataModelPublishStatus.REDEVELOPING.value)
            )
            .order_by("-created_by")
        )

        project_id = params.get("project_id")

        # 如果需要在某个项目下查看公开的模型，需要把当前项目的模型过滤掉
        if "is_open" in params and params["is_open"] and project_id:
            model_queryset.exclude(project_id=project_id)
        elif project_id:
            model_queryset = model_queryset.filter(project_id=params["project_id"])

        for model in model_queryset:
            if model.model_type not in released_models:
                logger.error(
                    _("模型({model_id})的模型类型({model_type})无效").format(
                        model_id=model.model_id,
                        model_type=model.model_type,
                    )
                )
                continue

            released_models[model.model_type]["models"].append(
                {
                    "model_id": model.model_id,
                    "model_name": model.model_name,
                    "model_alias": model.model_alias,
                    "model_type": model.model_type,
                    "latest_version_id": model.latest_version_id,
                }
            )

        return Response(sorted(list(released_models.values()), key=lambda x: x["order"]))

    @list_route(methods=["get"], url_path="fields_mappings")
    @params_valid(serializer=ApplicationFieldsMappingsSerializer)
    def fields_mappings(self, request, params):
        """
        @api {get} /datamanage/datamodel/instances/fields_mappings/ 获取模型发布版本对应的字段映射列表

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_release_fields_mappings
        @apiDescription 获取模型发布版本对应的字段映射列表

        @apiParam {Number} model_id 数据模型ID
        @apiParam {String} version_id 数据模型版本ID
        @apiParam {json} from_result_tables JSON序列化后的输入结果表列表
        @apiParam {String} flow_result_tables.result_table_id 输入结果表ID
        @apiParam {String} flow_result_tables.node_type 输入结果表所在节点类型

        @apiParamExample {querystring} Request-Example:
            /?model_id=1&version_id=2.0&from_result_tables=
            [{"result_table_id":"591_source_table1","node_type":"realtime_source"},
            {"result_table_id":"591_source_table2","node_type":"realtime_relation_source"}]

        @apiSuccess (200) {Object[]} data 字段映射结构列表
        @apiSuccess (200) {String} data.field_name 字段名称
        @apiSuccess (200) {String} data.field_alias 字段别名
        @apiSuccess (200) {String} data.description 字段描述
        @apiSuccess (200) {String} data.field_type 字段类型
        @apiSuccess (200) {String} data.field_category 字段分类
        @apiSuccess (200) {Boolean} data.is_primary_key 是否主键
        @apiSuccess (200) {Boolean} data.is_join_field 是否关联字段
        @apiSuccess (200) {Boolean} data.is_extended_field 是否扩展维度字段
        @apiSuccess (200) {String} data.join_field_name 扩展维度关联字段（当且仅当is_extended_field为true时才有非空值）
        @apiSuccess (200) {String} data.input_result_table_id 输入主表结果表ID
        @apiSuccess (200) {String} data.input_field_name 输入主表字段
        @apiSuccess (200) {String} data.input_table_field 输入主表ID/字段名
        @apiSuccess (200) {Object} data.relation 字段关联关系（当且仅当is_join_field为true时才有relation）
        @apiSuccess (200) {Number} data.relation.related_model_id 关联模型ID
        @apiSuccess (200) {String} data.relation.input_result_table_id 关联字段输入来源结果表ID
        @apiSuccess (200) {String} data.relation.input_field_name 关联字段输入来源字段
        @apiSuccess (200) {String} data.relation.input_table_field 关联维表ID/字段名
        @apiSuccess (200) {Boolean} data.is_generated_field 是否生成字段
        @apiSuccess (200) {Object} data.field_constraint_content 值约束配置
        @apiSuccess (200) {Object} data.field_clean_content 字段构建加工逻辑
        @apiSuccess (200) {String} data.field_clean_content.clean_option 字段构建加工逻辑类型
        @apiSuccess (200) {String} data.field_clean_content.clean_content 字段构建加工逻辑内容
        @apiSuccess (200) {Boolean} data.is_opened_application_clean 是否开启字段应用加工逻辑
        @apiSuccess (200) {Object} data.application_clean_content 字段应用加工逻辑（结构同上”构建加工逻辑“）

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "field_name": "key_field1",
                        "field_alias": "主键字段一",
                        "description": "xxx",
                        "field_type": "int",
                        "field_category": "dimension",
                        "is_primary_key": true,
                        "is_join_field": false,
                        "is_extended_field": false,
                        "join_field_name": null,
                        "input_result_table_id": "",
                        "input_field_name": "",
                        "input_table_field": "",
                        "is_generated_field": false,
                        "field_constraint_content": {},
                        "field_clean_content": {
                            "clean_option": "SQL",
                            "clean_content": "xxxxxx"
                        },
                        "is_opened_application_clean": false,
                        "application_clean_content": {}
                    },
                    {
                        "field_name": "dim1",
                        "field_alias": "关联维度1",
                        "description": "xxx",
                        "field_type": "string",
                        "field_category": "dimension",
                        "is_primary_key": false,
                        "is_join_field": true,
                        "is_extended_field": false,
                        "join_field_name": null,
                        "input_result_table_id": "",
                        "input_field_name": "",
                        "input_table_field": "",
                        "relation": {
                            "related_model_id": 2,
                            "input_result_table_id": "",
                            "input_field_name": "",
                            "input_table_field": ""
                        },
                        "is_generated_field": false,
                        "field_constraint_content": {},
                        "field_clean_content": {
                            "clean_option": "SQL",
                            "clean_content": "xxxxxx"
                        },
                        "is_opened_application_clean": false,
                        "application_clean_content": {}
                    },
                    {
                        "field_name": "sub_dim1",
                        "field_alias": "扩展维度1",
                        "description": "xxx",
                        "field_type": "string",
                        "field_category": "dimension",
                        "is_primary_key": false,
                        "is_join_field": false,
                        "is_extended_field": true,
                        "join_field_name": "dim1",
                        "input_result_table_id": "",
                        "input_field_name": "",
                        "input_table_field": "",
                        "is_generated_field": false,
                        "field_constraint_content": {},
                        "field_clean_content": {
                            "clean_option": "SQL",
                            "clean_content": "xxxxxx"
                        },
                        "is_opened_application_clean": false,
                        "application_clean_content": {}
                    },
                    {
                        "field_name": "metric1",
                        "field_alias": "指标1",
                        "description": "xxx",
                        "field_type": "int",
                        "field_category": "measure",
                        "is_primary_key": false,
                        "is_join_field": false,
                        "is_extended_field": false,
                        "join_field_name": null,
                        "input_result_table_id": "",
                        "input_field_name": "",
                        "input_table_field": "",
                        "is_generated_field": false,
                        "field_constraint_content": {},
                        "field_clean_content": {
                            "clean_option": "SQL",
                            "clean_content": "xxxxxx"
                        },
                        "is_opened_application_clean": true,
                        "application_clean_content": {
                            "clean_option": "SQL",
                            "clean_content": "xxxxxx"
                        }
                    }
                ]
            }
        """
        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            params["model_id"],
            params["version_id"],
        )
        fields_mappings = ModelInstanceManager.generate_init_fields_mappings(model_release.model_content)

        # 如果参数包含来源结果表列表，则尝试进行字段映射自动填充，自动填充失败则返回原始字段映射列表
        if "from_result_tables" in params and params["from_result_tables"]:
            try:
                from_result_tables = json.loads(params["from_result_tables"])
                input_params = {
                    InstanceInputType.MAIN_TABLE.value: [],
                    InstanceInputType.DIM_TABLE.value: [],
                }
                for from_result_table_info in from_result_tables:
                    input_type = ModelInstanceManager.get_input_type_by_from_rt_info(
                        from_result_table_info,
                        len(from_result_tables),
                    )
                    input_result_table_id = from_result_table_info.get("result_table_id")
                    input_params[input_type].append(input_result_table_id)

                fields_mappings = ModelInstanceManager.autofill_input_table_field_by_param(
                    fields_mappings,
                    {
                        "main_table": input_params[InstanceInputType.MAIN_TABLE.value][0],
                        "dimension_tables": input_params[InstanceInputType.DIM_TABLE.value],
                    },
                )
            except Exception as e:
                logger.error("自动填充失败，继续返回原始字段映射列表，异常: %s" % e, exc_info=True)

        return Response(fields_mappings)

    @list_route(methods=["get"], url_path="by_table")
    @params_valid(serializer=ApplicationInstanceByTableSerializer)
    def by_table(self, request, params):
        """
        @api {get} /datamanage/datamodel/instances/by_table/ 根据结果表ID获取数据模型实例信息和表信息

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_information_by_table
        @apiDescription 根据结果表ID获取数据模型实例信息和表信息

        @apiParam {String} result_table_id 结果表ID

        @apiSuccess (200) {Object} data 模型实例信息和表信息
        @apiSuccess (200) {Number} data.model_instance_id 数据模型实例ID
        @apiSuccess (200) {Number} data.model_id 数据模型ID
        @apiSuccess (200) {String} data.model_name 数据模型名称
        @apiSuccess (200) {String} data.model_alias 数据模型中文名
        @apiSuccess (200) {String} data.result_table_name 结果表名称
        @apiSuccess (200) {String} data.result_table_name_alias 结果表中文名

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "model_instance_id": 1,
                    "model_id": 1,
                    "model_name": "model1",
                    "model_alias": "模型1",
                    "result_table_name": "table1",
                    "result_table_name_alias": "表1"
                }
        """
        result_table_id = params["result_table_id"]

        result_table_info = MetaApi.result_tables.retrieve(
            {"result_table_id": result_table_id}, raise_exception=True
        ).data

        model_instance = ModelInstanceManager.get_model_instance_by_rt(params["result_table_id"])

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_id=model_instance.model_id,
            version_id=model_instance.version_id,
        )

        return Response(
            {
                "model_instance_id": model_instance.instance_id,
                "model_id": model_instance.model_id,
                "model_name": model_release.model_content.get("model_name"),
                "model_alias": model_release.model_content.get("model_alias"),
                "result_table_name": result_table_info.get("result_table_name"),
                "result_table_name_alias": result_table_info.get("result_table_name_alias"),
            }
        )

    @list_route(methods=["post"], url_path="dataflow")
    @params_valid(serializer=ApplicationInstanceDataflowSerializer)
    def dataflow(self, request, params):
        """
        @api {post} /datamanage/datamodel/instances/dataflow/ 模型应用实例生成完整dataflow任务

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_generate_dataflow
        @apiDescription 模型应用实例生成完整dataflow任务

        @apiParam (实例) {Number} model_id 数据模型ID
        @apiParam (实例) {String} version_id 数据模型版本ID
        @apiParam (实例) {Number} project_id 项目ID
        @apiParam (实例) {Number} bk_biz_id 业务ID
        @apiParam (输入) {Object} input 数据模型应用输入
        @apiParam (输入) {String} input.main_table 输入来源明细表
        @apiParam (输入) {String[]} input.dimension_tables 输入来源维度表
        @apiParam (主表配置) {Object} [main_table] 数据模型主表配置
            （如果不配置main_table，所有参数都会通过默认规则生成，并且不会为该结果表构建存储）
        @apiParam (主表配置) {String} [main_table.table_name] 主表结果表英文名
        @apiParam (主表配置) {String} [main_table.table_alias] 主表结果表中文名
        @apiParam (主表配置) {Object[]} [main_table.fields] 主表字段映射
            如果不提供当前参数，默认会从输入主表和输入维表中搜寻同名字段来做字段映射，如果仍旧无法映射所有字段，则抛出异常
        @apiParam (主表配置) {String} main_table.fields.field_name 输出字段
        @apiParam (主表配置) {String} main_table.fields.input_result_table_id 输入结果表
        @apiParam (主表配置) {String} main_table.fields.input_field_name 输入字段名
        @apiParam (主表配置) {Object} [main_table.fields.application_clean_content] 应用阶段清洗规则
        @apiParam (主表配置) {String} main_table.fields.application_clean_content.clean_option 应用阶段清洗类型
        @apiParam (主表配置) {String} main_table.fields.application_clean_content.clean_content 应用节点清洗逻辑
        @apiParam (主表配置) {Object} [main_table.fields.relation] 维表关联信息
        @apiParam (主表配置) {String} main_table.fields.relation.input_result_table_id 维表关联输入结果表
        @apiParam (主表配置) {String} main_table.fields.relation.input_field_name 维表关联输入字段名
        @apiParam (主表配置) {Object[]} [main_table.storages] 主表存储配置
        @apiParam (主表配置) {String} main_table.storages.cluster_type 存储集群类型
        @apiParam (主表配置) {String} main_table.storages.cluster_name 存储集群名称
        @apiParam (主表配置) {String} main_table.storages.expires 存储过期时间
        @apiParam (主表配置) {Dict} [main_table.storages.specific_params] 存储特定存储参数，比如索引字段等
        @apiParam (指标公共配置) {Object[]} [default_indicators_storages] 指标默认存储配置
            （配置后该配置将作为所有指标的存储配置，如果需要更多存储的自定义配置，比如索引等，则需要在indicators结构中说明）
        @apiParam (指标配置) {Object[]} [indicators] 数据模型指标配置列表（如不提供，则使用公共配置中的参数生成所有默认指标）
        @apiParam (指标配置) {String} indicators.indicator_name 指标名称
        @apiParam (指标配置) {String} [indicators.table_custom_name] 指标表名自定义部分
        @apiParam (指标配置) {String} [indicators.table_alias] 指标表中文名
        @apiParam (指标配置) {Object[]} [indicators.storages] 存储配置
        @apiParam (指标配置) {String} indicators.storages.cluster_type 存储集群类型
        @apiParam (指标配置) {String} indicators.storages.cluster_name 存储集群名称
        @apiParam (指标配置) {Number} indicators.storages.expires 存储过期时间
        @apiParam (指标配置) {Dict} [indicators.storages.specific_params] 存储特定存储参数，比如索引字段等

        @apiParamExample {json} 简单请求参数示例
            {
                "model_id": 1,
                "project_id": 6530,
                "bk_biz_id": 591,
                "input": {
                    "main_table": "591_source_fact_table",
                    "dimension_tables": ["591_source_dim_table1"]
                },
                "default_indicators_storages": [
                    {
                        "cluster_type": "tspider",
                        "cluster_name": "default",
                        "expires": 7
                    }
                ]
            }

        @apiParamExample {json} 复杂请求参数示例，目前 storages.specific_params 定制化配置，只列举 tspider(mysql)、ignite，
                更多类型请参考 DataFlow 存储节点的参数说明
            {
                "model_id": 1,
                "version_id": "nl75jhbqtcpv2zes9u6yigf0dx38mw1a",
                "project_id": 6530,
                "bk_biz_id": 591,
                "input": {
                    "main_table": "591_source_fact_table",
                    "dimension_tables": ["591_source_dim_table1"]
                },
                "main_table": {
                    "table_name": "my_main_table",
                    "table_alias": "我的模型表",
                    "fields": [
                        {
                            "field_name": "province",
                            "input_result_table_id": "591_source_fact_table",
                            "input_field_name": "province",
                            "relation": {
                                "related_model_id": 2,
                                "input_result_table_id": "591_source_dim_table1",
                                "input_field_name": "province"
                            },
                            "application_clean_content": {}
                        },
                        {
                            "input_result_table_id": "591_source_dim_table1",
                            "field_name": "country",
                            "input_field_name": "country",
                            "application_clean_content": {}
                        }
                        {
                            "input_result_table_id": "591_source_fact_table",
                            "field_name": "metric1",
                            "input_field_name": "metric1",
                            "application_clean_content": {
                                "clean_option": "SQL",
                                "clean_content": "metric1 * 100"
                            }
                        }
                    ],
                    "storages": [
                        {
                            "cluster_type": "tspider",
                            "cluster_name": "default",
                            "expires": 7,
                            "specific_params": {
                                "indexed_fields": ["province"]
                            }
                        },
                        {
                            "cluster_type": "ignite",
                            "cluster_name": "default",
                            "expires": 7,
                            "specific_params": {
                                "indexed_fields": ["province"],
                                "max_records": 1000000,
                                "storage_keys": ["province"],
                                "storage_type": "join"
                            }
                        }
                    ]
                },
                "indicators": [
                    {
                        "indicator_name": "sum_metric1",
                        "table_custom_name": "my_custom_indicator",
                        "table_alias": "我的汇总指标1",
                        "storages": [
                            {
                                "cluster_type": "tspider",
                                "cluster_name": "default",
                                "expires": 7
                            },
                            {
                                "cluster_type": "hdfs",
                                "cluster_name": "default",
                                "expires": 30
                            }
                        ]
                    }
                ]
            }

        @apiSuccess (200) {Number} data.flow_id 数据开发任务ID
        @apiSuccess (200) {Number} data.model_instance_id 数据模型实例ID
        @apiSuccess (200) {Object} data.main_table 数据模型实例主表信息
        @apiSuccess (200) {String} data.main_table.result_table_id 数据模型实例主表ID
        @apiSuccess (200) {String} data.main_table.result_table_name_alias 数据模型实例主表中文名
        @apiSuccess (200) {String} data.main_table.fields 数据模型实例主表字段列表
        @apiSuccess (200) {String} data.main_table.fields.field_name 数据模型实例主表字段名
        @apiSuccess (200) {String} data.main_table.fields.field_type 数据模型实例主表字段类型
        @apiSuccess (200) {String} data.main_table.fields.field_alias 数据模型实例主表字段中文名
        @apiSuccess (200) {String} data.main_table.fields.description 数据模型实例主表字段描述
        @apiSuccess (200) {Object[]} data.indicators 数据模型实例指标表信息列表
        @apiSuccess (200) {String} data.indicators.indicator_name 数据模型实例指标名称
        @apiSuccess (200) {String} data.indicators.result_table_id 数据模型实例指标结果表ID
        @apiSuccess (200) {String} data.indicators.result_table_name_alias 数据模型实例指标结果表中文名
        @apiSuccess (200) {String} data.indicators.fields 数据模型实例指标表字段列表
        @apiSuccess (200) {String} data.indicators.fields.field_name 数据模型实例指标表字段名
        @apiSuccess (200) {String} data.indicators.fields.field_type 数据模型实例指标表字段类型
        @apiSuccess (200) {String} data.indicators.fields.field_alias 数据模型实例指标表字段中文名
        @apiSuccess (200) {String} data.indicators.fields.description 数据模型实例指标表字段描述

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "flow_id": 1,
                    "model_instance_id": 1,
                    "main_table": {
                        "node_id": 1,
                        "result_table_id": "591_my_main_table",
                        "result_table_name_alias": "我的模型表",
                        "fields": [
                            {
                                "field_name": "province",
                                "field_type": "string",
                                "field_alias": "省份",
                                "description": ""
                            },
                            {
                                "field_name": "country",
                                "field_type": "string",
                                "field_alias": "国家",
                                "description": ""
                            },
                            {
                                "field_name": "metric1",
                                "field_type": "int",
                                "field_alias": "指标1",
                                "description": ""
                            }
                        ]
                    },
                    "indicators": [
                        {
                            "node_id": 2,
                            "result_table_id": "591_sum_metric1_my_custom_indicator_1d",
                            "result_table_name_alias": "每天metric1总量",
                            "fields": [
                                {
                                    "field_name": "province",
                                    "field_type": "string",
                                    "field_alias": "省份",
                                    "description": ""
                                },
                                {
                                    "field_name": "country",
                                    "field_type": "string",
                                    "field_alias": "国家",
                                    "description": ""
                                },
                                {
                                    "field_name": "sum_metric1",
                                    "field_type": "int",
                                    "field_alias": "指标1总量",
                                    "description": ""
                                }
                            ]
                        }
                    ]
                }
            }
        """
        model_id = params["model_id"]
        version_id = params.get("version_id")
        project_id = params["project_id"]
        bk_biz_id = params["bk_biz_id"]
        input_params = params["input"]
        main_table_params = params.get("main_table", {})
        main_table_storages_params = main_table_params.get("storages", [])
        indicators_params = params.get("indicators", None)
        default_indicators_storages_params = params.get("default_indicators_storages", [])

        if version_id is None:
            model = ModelInstanceManager.get_model_by_id(model_id)
            model_release = ModelInstanceManager.get_model_release_by_id_and_version(model_id, model.latest_version_id)
        else:
            model_release = ModelInstanceManager.get_model_and_check_latest_version(model_id, version_id)
        model_content = model_release.model_content

        # 校验创建参数
        ModelInstanceFlowManager.validate_data_model_dataflow_params(
            main_table_params,
            indicators_params,
            model_content,
        )

        # 创建数据模型数据流任务
        flow_id = ModelInstanceFlowManager.create_data_model_dataflow(project_id, bk_biz_id, model_release)
        node_generator = VirtualNodeGenerator(bk_biz_id)
        nodes_params = []

        # 预创建数据模型实例
        try:
            model_instance = DmmModelInstance.objects.create(
                project_id=project_id,
                model_id=model_id,
                version_id=model_release.version_id,
                flow_id=flow_id,
                created_by=get_request_username(),
            )
        except Exception as e:
            DataflowApi.flows.delete({"flow_id": flow_id}, raise_exception=True)
            raise e

        try:
            # 生成所有数据流输入源节点的节点参数
            nodes_params.append(
                ModelInstanceFlowManager.generate_stream_source_node_params(
                    input_params["main_table"],
                    node_generator,
                )
            )
            for dimension_table in input_params["dimension_tables"]:
                nodes_params.append(
                    ModelInstanceFlowManager.generate_kv_source_node_params(dimension_table, node_generator)
                )

            # 生成数据模型节点的节点参数
            data_model_node_params = ModelInstanceFlowManager.generate_data_model_node_params(
                model_instance.instance_id,
                bk_biz_id,
                project_id,
                model_content,
                model_release.version_id,
                main_table_params,
                input_params,
                node_generator,
            )
            data_model_node_id = data_model_node_params["id"]
            nodes_params.append(data_model_node_params)

            # 生成数据模型应用存储节点的参数
            for storage_params in main_table_storages_params:
                nodes_params.append(
                    ModelInstanceFlowManager.generate_storage_node_params(
                        bk_biz_id,
                        node_generator.get_node(data_model_node_id),
                        storage_params,
                        node_generator,
                    )
                )

            # 创建指标节点
            indicators_nodes = ModelInstanceFlowManager.generate_model_inds_nodes_params(
                model_instance.instance_id,
                model_content,
                bk_biz_id,
                node_generator.get_node(data_model_node_id),
                node_generator.get_storage_nodes(data_model_node_id),
                indicators_params,
                node_generator,
            )
            nodes_params.extend(indicators_nodes)

            # 创建指标存储节点
            nodes_params.extend(
                ModelInstanceFlowManager.generate_inds_storages_nodes_params(
                    bk_biz_id,
                    indicators_nodes,
                    indicators_params,
                    default_indicators_storages_params,
                    node_generator,
                )
            )
        except Exception as e:
            self.delete_flow_instance(flow_id, model_instance)
            raise e

        # 调用dataflow接口创建所有节点
        logger.info("Create data model dataflow by params: {}".format(json.dumps(nodes_params)))
        create_response = DataflowApi.flows.create_by_nodes({"nodes": nodes_params, "flow_id": flow_id})
        if not create_response.is_success():
            self.delete_flow_instance(flow_id, model_instance)
            raise ApiResultError(create_response.message)

        return Response(create_response.data)

    def delete_flow_instance(self, flow_id, model_instance):
        """创建失败时删除已创建的flow和模型实例"""
        try:
            DataflowApi.flows.delete({"flow_id": flow_id}, raise_exception=True)
            model_instance.delete()
        except Exception as e:
            logger.error("Delete flow and model_instance error: {}".format(e), exc_info=True)

    @list_route(methods=["get"], url_path="dataflow/check")
    @params_valid(serializer=ApplicationDataflowCheckSerializer)
    def dataflow_check(self, request, params):
        """
        @api {post} /datamanage/datamodel/instances/dataflow/check/ 检查任务中的数据模型实例任务是否合法

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_model_instance_dataflow_check
        @apiDescription 检查任务中的数据模型实例任务是否合法

        @apiParam {Number} flow_id 数据流ID

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        return Response(ModelInstanceFlowManager.check_dataflow_instances(params["flow_id"]))

    @list_route(methods=["get"], url_path="init")
    @params_valid(serializer=ApplicationInstanceInitSerializer)
    def init_default_indicator_nodes(self, request, params):
        """
        @api {get} /datamanage/datamodel/instances/init/ 数据模型应用初始化节点

        @apiVersion 3.5.0
        @apiGroup DataModelApplication
        @apiName datamodel_instance_init
        @apiDescription 数据模型应用初始化节点

        @apiParam {String} [model_instance_id] 数据模型实例ID
        @apiParam {String} [node_id] 数据流节点ID（数据模型实例ID和节点ID需要至少有其中一个作为参数）
        @apiParam {String[]} [indicator_names] 指定生成的指标名称

        @apiSuccess (200) {Object[]} data 数据模型指标节点基础配置列表
        @apiSuccess (200) {String} data.node_type 数据模型指标节点类型
        @apiSuccess (200) {String} data.node_key 数据模型初始化节点唯一标识
        @apiSuccess (200) {String} data.parent_node_key 数据模型初始化节点父节点标识（为None时父节点为当前实例的数据模型节点）
        @apiSuccess (200) {Object} data.config 节点配置
        @apiSuccess (200) {Object} data.config.data_model_indicator 数据模型指标节点指标配置
        @apiSuccess (200) {Number} data.config.data_model_indicator.model_isntance_id 数据模型实例ID，从上游数据模型节点继承
        @apiSuccess (200) {String} data.config.data_model_indicator.calculation_atom_name 数据模型指标统计统计口径
        @apiSuccess (200) {String[]} data.config.data_model_indicator.aggregation_fields 数据模型指标聚合字段
        @apiSuccess (200) {String} data.config.data_model_indicator.filter_formula 数据模型指标过滤条件
        @apiSuccess (200) {String} data.config.data_model_indicator.scheduling_type 数据模型指标计算类型
        @apiSuccess (200) {String} data.config.data_model_indicator.scheduling_content 数据模型指标调度内容
        @apiSuccess (200) {Object[]} data.config.outputs 节点输出配置
        @apiSuccess (200) {Number} data.config.outputs.bk_biz_id 输出结果表业务ID
        @apiSuccess (200) {Object[]} data.config.outputs.fields 输出字段
        @apiSuccess (200) {String} data.config.outputs.table_name 输出表名
        @apiSuccess (200) {String} data.config.outputs.table_prefix_name 输出表前缀（用户不允许改）
        @apiSuccess (200) {String} data.config.outputs.table_suffix_name 输出表后缀（由统计频率生成）
        @apiSuccess (200) {String} data.config.outputs.table_custom_name 输出表自定义名称

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "node_key": "datamodel_batch_indicator_max_price_1d",
                        "parent_node_key": "hdfs_storage_max_price_1d",
                        "node_type": "datamodel_batch_indicator",
                        "config": {
                            "dedicated_config": {
                                "calculation_atom_name": "max_price",
                                "scheduling_type": "batch",
                                "filter_formula": "",
                                "aggregation_fields": [],
                                "model_instance_id": 1
                            },
                            "window_config": {
                                "window_type": "fixed",
                                "count_freq": 1,
                                "dependency_config_type": "unified",
                                "schedule_period": "day",
                                "fixed_delay": 0,
                                "unified_config": {
                                    "window_size": 1,
                                    "dependency_rule": "all_finished",
                                    "window_size_period": "day"
                                },
                                "advanced": {
                                    "recovery_enable": false,
                                    "recovery_times": 3,
                                    "recovery_interval": "60m"
                                }
                            },
                            "outputs": [
                                {
                                    "bk_biz_id": 591,
                                    "fields": [],
                                    "table_prefix_name": "max_price",
                                    "table_name": "max_price_1d",
                                    "table_custom_name": "",
                                    "table_suffix_name": "1d",
                                    "output_name": "1d最大价格"
                                }
                            ]
                        }
                    },
                    {
                        "node_key": "hdfs_storage_max_price_1d",
                        "parent_node_key": null,
                        "node_type": "hdfs_storage",
                        "config": {
                            "bk_biz_id": 591,
                            "name": "",
                            "expires": 3,
                            "from_result_table_ids": [],
                            "cluster": "",
                            "result_table_id": ""
                        }
                    }
                ]
            }
        """
        if "model_instance_id" in params:
            model_instance = ModelInstanceManager.get_model_instance_by_id(params["model_instance_id"])
        elif "node_id" in params:
            model_instance = ModelInstanceManager.get_model_instance_by_node_id(params["node_id"])

        instance_table = ModelInstanceManager.get_model_instance_table_by_id(model_instance.instance_id)

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_id=model_instance.model_id,
            version_id=model_instance.version_id,
        )

        indicator_node_configs = ModelInstanceManager.generate_inst_default_ind_nodes(
            model_instance.instance_id,
            model_release.model_content,
            instance_table.bk_biz_id,
            params.get("indicator_names", []),
        )

        dataflow_node_configs = ModelInstanceManager.attach_batch_channel_nodes(
            indicator_node_configs,
            instance_table.result_table_id,
            instance_table.bk_biz_id,
        )

        if model_release.model_content.get("model_type") == DataModelType.DIMENSION_TABLE.value:
            if not ModelInstanceManager.check_main_table_has_spec_storage(
                instance_table.result_table_id, RELATION_STORAGE_CLUSTER_TYPE
            ):
                dataflow_node_configs.append(
                    ModelInstanceManager.attach_relation_storage_node(
                        instance_table.result_table_id,
                        instance_table.bk_biz_id,
                    )
                )

        dataflow_node_configs_with_level = ModelInstanceManager.attach_node_level_info(dataflow_node_configs)

        return Response(dataflow_node_configs_with_level)
