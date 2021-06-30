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


import io
import json
import os
import time

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, TopicPartition
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.dataquality.config import (
    CORRECT_SQL_DATA_TYPES_MAPPINGS,
    CORRECTION_EXCLUDE_FIELDS,
    TEXT_FIELD_TYPES,
)
from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.models.correction import (
    DataQualityCorrectConditionTemplate,
    DataQualityCorrectConfig,
    DataQualityCorrectConfigItem,
    DataQualityCorrectHandlerTemplate,
)
from datamanage.pro.dataquality.serializers.base import DataQualityDataSetSerializer
from datamanage.pro.dataquality.serializers.correction import (
    CorrectConditionTemplateSerializer,
    CorrectConfigCreateSerializer,
    CorrectConfigUpdateSerializer,
    CorrectDebugResultSerializer,
    CorrectDebugSubmitSerializer,
    CorrectHandlerTemplateSerializer,
    CorrectSqlSerializer,
)
from datamanage.pro.pizza_settings import (
    CORRECTING_DEBUG_SESSION_KEY,
    DEBUG_VIRTUAL_TABLE,
    PARQUET_FILE_TMP_FOLDER,
    SESSION_GEOG_AREA_CODE,
)
from datamanage.utils.api import DataflowApi, MetaApi
from datamanage.utils.dbtools.hdfs_util import get_hdfs_client
from datamanage.utils.drf import DataPageNumberPagination
from django.db import transaction
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIModelViewSet, APIViewSet


class DataQualityCorrectConfigViewSet(BaseMixin, APIViewSet):
    lookup_field = "correct_config_id"
    SAMPLE_DATA_SOURCE_CLUSTER_TYPE = "kafka"
    VIRTUAL_TABLE_CLUSTER_TYPE = "hdfs"
    VIRTUAL_TABLE_TMP_FOLDER = "correct_debug_tmp_tables"
    DEBUG_DEFAULT_PROCESSING_TYPE = "batch"
    MAX_SAMPLE_DATA_COUNT = 100
    SAMPLE_DATA_TIMEOUT = 10

    @params_valid(serializer=CorrectConfigCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /datamanage/dataquality/correct_configs/ 创建修正规则配置

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_create
        @apiDescription 创建修正规则配置

        @apiParam {String} bk_username 用户名
        @apiParam {String} data_set_id 数据集ID
        @apiParam {Number} bk_biz_id 业务ID
        @apiParam {Number} flow_id 数据流ID
        @apiParam {Number} node_id 数据流节点ID
        @apiParam {String} source_sql 原始SQL
        @apiParam {String} generate_type 生成类型
        @apiParam {String} description 描述
        @apiParam {List} correct_configs 修正配置列表

        @apiSuccess (200) {Number} data.correct_config_id 修正配置ID
        @apiSuccess (200) {Number} data.data_set_id 数据集ID
        @apiSuccess (200) {Number} data.flow_id 数据流ID
        @apiSuccess (200) {Number} data.node_id 数据流节点ID
        @apiSuccess (200) {String} data.source_sql 原始SQL
        @apiSuccess (200) {String} data.correct_sql 修正SQL
        @apiSuccess (200) {String} data.generate_type 生成类型
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间
        @apiSuccess (200) {String} data.description 描述
        @apiSuccess (200) {List} data.correct_configs 修正详细配置项列表
        @apiSuccess (200) {Number} data.correct_configs.correct_config_item_id 修正配置ID
        @apiSuccess (200) {String} data.correct_configs.field 数据集字段
        @apiSuccess (200) {Object} data.correct_configs.correct_config_detail 修正配置
        @apiSuccess (200) {String} data.correct_configs.correct_config_alias 修正配置别名
        @apiSuccess (200) {String} data.correct_configs.created_by 创建人
        @apiSuccess (200) {String} data.correct_configs.created_at 创建时间
        @apiSuccess (200) {String} data.correct_configs.updated_by 更新人
        @apiSuccess (200) {String} data.correct_configs.updated_at 更新时间
        @apiSuccess (200) {String} data.correct_configs.description 规则配置描述

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "correct_config_id": 1,
                    "data_set_id": "591_table",
                    "bk_biz_id": 591,
                    "flow_id": 1,
                    "node_id": 1,
                    "source_sql": "xxx",
                    "correct_sql": "xxx",
                    "generate_type": "user",
                    "correct_configs": [
                        {
                            "correct_config_item_id": 1,
                            "field": "field1",
                            "correct_config_detail": {
                                "rules": [
                                    {
                                        "condition": {
                                            "condition_name": "custom_sql_condition",
                                            "condition_type": "custom",
                                            "condition_value": "field1 IS NOT NULL"
                                        },
                                        "handler": {
                                            "handler_name": "fixed_filling",
                                            "handler_type": "filling",
                                            "handler_value_type": "int",
                                            "handler_value": 100
                                        }
                                    }
                                ],
                                "output": {
                                    "generate_new_field": false,
                                    "new_field": ""
                                }
                            },
                            "correct_config_alias": "",
                            "created_by": "admin",
                            "created_at": "2020-07-27 10:30:00",
                            "updated_by": "admin",
                            "updated_at": "2020-07-27 10:31:00"
                        }
                    ],
                    "created_by": "admin",
                    "created_at": "2020-07-27 10:30:00",
                    "updated_by": "admin",
                    "updated_at": "2020-07-27 10:31:00",
                    "description": ""
                }
            }
        """
        correct_configs = []
        with transaction.atomic(using="bkdata_basic"):
            correct_config = DataQualityCorrectConfig.objects.create(
                data_set_id=params["data_set_id"],
                bk_biz_id=params["bk_biz_id"],
                flow_id=params["flow_id"],
                node_id=params["node_id"],
                source_sql=params["source_sql"],
                correct_sql=self.generate_correct_sql(
                    params["data_set_id"],
                    params["source_sql"],
                    params["correct_configs"],
                ),
                generate_type=params["generate_type"],
                created_by=get_request_username(),
                description=params["description"],
            )

            for item_params in params["correct_configs"]:
                correct_configs.append(self.create_correct_config_item(item_params, correct_config))

        return Response(
            {
                "correct_config_id": correct_config.id,
                "data_set_id": correct_config.data_set_id,
                "flow_id": correct_config.flow_id,
                "node_id": correct_config.node_id,
                "source_sql": correct_config.source_sql,
                "correct_sql": correct_config.correct_sql,
                "generate_type": correct_config.generate_type,
                "correct_configs": correct_configs,
                "created_by": correct_config.created_by,
                "created_at": correct_config.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_by": correct_config.updated_by,
                "updated_at": correct_config.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "description": correct_config.description,
            }
        )

    def create_correct_config_item(self, params, correct_config):
        correct_config_item = DataQualityCorrectConfigItem.objects.create(
            field=params["field"],
            correct_config=correct_config,
            correct_config_detail=json.dumps(params["correct_config_detail"]),
            correct_config_alias=params["correct_config_alias"],
            created_by=get_request_username(),
            description=params["description"],
        )

        return {
            "correct_config_item_id": correct_config_item.id,
            "field": correct_config_item.field,
            "correct_config_detail": params["correct_config_detail"],
            "correct_config_alias": params["correct_config_alias"],
            "created_by": correct_config_item.created_by,
            "created_at": correct_config_item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_by": correct_config_item.updated_by,
            "updated_at": correct_config_item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "description": correct_config_item.description,
        }

    def generate_correct_sql(self, data_set_id, source_sql, correct_configs, for_debug=False):
        result_table_info = MetaApi.result_tables.retrieve(
            {"result_table_id": data_set_id, "related": ["fields"]},
            raise_exception=True,
        ).data
        fields = result_table_info.get("fields", [])
        if for_debug:
            processing_type = self.DEBUG_DEFAULT_PROCESSING_TYPE
        else:
            processing_type = result_table_info.get("processing_type")

        source_fields = {}
        for field in fields:
            field_name = field.get("field_name")
            if field_name not in CORRECTION_EXCLUDE_FIELDS:
                source_fields[field_name] = field

        new_select_parts = self.generate_select_parts(source_fields, correct_configs, processing_type)
        correct_sql = "SELECT {} FROM ({})".format(", ".join(new_select_parts), source_sql)
        return correct_sql

    def generate_select_parts(self, fields, correct_configs, processing_type):
        select_parts = []
        for correct_config in correct_configs:
            field = correct_config.get("field")
            if field not in fields:
                continue
            field_type = fields[field].get("field_type")

            case_parts = []
            for rule in correct_config.get("correct_config_detail", {}).get("rules", []):
                case_parts.append(
                    "WHEN {} THEN {}".format(
                        rule.get("condition", {}).get("condition_value"),
                        self.generate_handler_value(rule.get("handler", {}), field_type),
                    )
                )
            select_parts.append(
                "CASE {} ELSE {} END AS {}".format(
                    "\n".join(case_parts),
                    self.generate_field_type_cast(field, field_type, processing_type),
                    field,
                )
            )

            del fields[field]

        for src_field in list(fields.keys()):
            select_parts.append("{} as {}".format(src_field, src_field))

        return select_parts

    def generate_handler_value(self, handler_config, field_type):
        if field_type in TEXT_FIELD_TYPES:
            return "'{}'".format(handler_config.get("handler_value"))
        else:
            return "{}".format(handler_config.get("handler_value"))

    def generate_field_type_cast(self, field, field_type, processing_type):
        return "CAST({} AS {})".format(
            field,
            CORRECT_SQL_DATA_TYPES_MAPPINGS.get(field_type, {}).get(processing_type, field_type),
        )

    @params_valid(serializer=CorrectConfigUpdateSerializer)
    def update(self, request, params, correct_config_id):
        """
        @api {put} /datamanage/dataquality/correct_configs/{correct_config_id}/ 修改修正规则配置

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_update
        @apiDescription 修改修正规则配置

        @apiParam {String} bk_username 用户名
        @apiParam {String} source_sql 原始SQL
        @apiParam {List} correct_configs 修正配置列表

        @apiSuccess (200) {Number} data.data_set_id 数据集ID
        @apiSuccess (200) {Number} data.flow_id 数据流ID
        @apiSuccess (200) {Number} data.node_id 数据流节点ID
        @apiSuccess (200) {String} data.source_sql 原始SQL
        @apiSuccess (200) {String} data.correct_sql 修正SQL
        @apiSuccess (200) {String} data.generate_type 生成类型
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间
        @apiSuccess (200) {String} data.description 描述
        @apiSuccess (200) {List} data.correct_configs 修正详细配置项列表
        @apiSuccess (200) {Number} data.correct_configs.correct_config_item_id 修正配置ID
        @apiSuccess (200) {String} data.correct_configs.field 数据集字段
        @apiSuccess (200) {Object} data.correct_configs.correct_config_detail 修正配置
        @apiSuccess (200) {String} data.correct_configs.correct_config_alias 修正配置别名
        @apiSuccess (200) {String} data.correct_configs.created_by 创建人
        @apiSuccess (200) {String} data.correct_configs.created_at 创建时间
        @apiSuccess (200) {String} data.correct_configs.updated_by 更新人
        @apiSuccess (200) {String} data.correct_configs.updated_at 更新时间
        @apiSuccess (200) {String} data.correct_configs.description 规则配置描述

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "correct_config_id": 1,
                    "data_set_id": "591_table",
                    "bk_biz_id": 591,
                    "flow_id": 1,
                    "node_id": 1,
                    "source_sql": "xxx",
                    "correct_sql": "xxx",
                    "generate_type": "user",
                    "correct_configs": [
                        {
                            "correct_config_item_id": 1,
                            "field": "field1",
                            "correct_config_detail": {
                                "rules": [
                                    {
                                        "condition": {
                                            "condition_name": "custom_sql_condition",
                                            "condition_type": "custom",
                                            "condition_value": "field1 IS NOT NULL"
                                        },
                                        "handler": {
                                            "handler_name": "fixed_filling",
                                            "handler_type": "filling",
                                            "handler_value_type": "int",
                                            "handler_value": 100
                                        }
                                    }
                                ],
                                "output": {
                                    "generate_new_field": false,
                                    "new_field": ""
                                }
                            },
                            "correct_config_alias": "",
                            "created_by": "admin",
                            "created_at": "2020-07-27 10:30:00",
                            "updated_by": "admin",
                            "updated_at": "2020-07-27 10:31:00"
                        }
                    ],
                    "created_by": "admin",
                    "created_at": "2020-07-27 10:30:00",
                    "updated_by": "admin",
                    "updated_at": "2020-07-27 10:31:00",
                    "description": ""
                }
            }
        """
        correct_configs = []
        with transaction.atomic(using="bkdata_basic"):
            try:
                correct_config = DataQualityCorrectConfig.objects.get(id=correct_config_id)
            except DataQualityCorrectConfig.DoesNotExist:
                raise dm_pro_errors.CorrectConfigNotExistError()

            correct_config.source_sql = params["source_sql"]
            correct_config.correct_sql = self.generate_correct_sql(
                correct_config.data_set_id,
                params["source_sql"],
                params["correct_configs"],
            )
            correct_config.updated_by = get_request_username()
            correct_config.save()

            for correct_config_params in params["correct_configs"]:
                correct_configs.append(self.update_correct_config(correct_config_params, correct_config))

        return Response(
            {
                "correct_config_id": correct_config.id,
                "data_set_id": correct_config.data_set_id,
                "flow_id": correct_config.flow_id,
                "node_id": correct_config.node_id,
                "source_sql": correct_config.source_sql,
                "correct_sql": correct_config.correct_sql,
                "generate_type": correct_config.generate_type,
                "correct_configs": correct_configs,
                "created_by": correct_config.created_by,
                "created_at": correct_config.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_by": correct_config.updated_by,
                "updated_at": correct_config.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "description": correct_config.description,
            }
        )

    def update_correct_config(self, params, correct_config):
        if "correct_config_item_id" not in params or not params["correct_config_item_id"]:
            return self.create_correct_config_item(params, correct_config)

        try:
            correct_config_item = DataQualityCorrectConfigItem.objects.get(id=params["correct_config_item_id"])
        except DataQualityCorrectConfigItem.DoesNotExist:
            raise dm_pro_errors.CorrectConfigNotExistError()

        correct_config_item.field = params["field"]
        correct_config_item.correct_config_detail = json.dumps(params["correct_config_detail"])
        correct_config_item.correct_config_alias = params["correct_config_alias"]
        correct_config_item.description = params["description"]
        correct_config_item.updated_by = get_request_username()
        correct_config_item.save()

        return {
            "correct_config_item_id": correct_config_item.id,
            "field": correct_config_item.field,
            "correct_config_detail": params["correct_config_detail"],
            "correct_config_alias": params["correct_config_alias"],
            "created_by": correct_config_item.created_by,
            "created_at": correct_config_item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_by": correct_config_item.updated_by,
            "updated_at": correct_config_item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "description": correct_config_item.description,
        }

    @params_valid(serializer=DataQualityDataSetSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dataquality/correct_configs/ 查询修正规则配置

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_list
        @apiDescription 查询修正规则配置

        @apiParam {String} data_set_id 数据集

        @apiSuccess (200) {Number} data.correct_config_id 修正配置ID
        @apiSuccess (200) {Number} data.data_set_id 数据集ID
        @apiSuccess (200) {Number} data.flow_id 数据流ID
        @apiSuccess (200) {Number} data.node_id 数据流节点ID
        @apiSuccess (200) {String} data.source_sql 原始SQL
        @apiSuccess (200) {String} data.correct_sql 修正SQL
        @apiSuccess (200) {String} data.generate_type 生成类型
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间
        @apiSuccess (200) {String} data.description 描述
        @apiSuccess (200) {List} data.correct_configs 修正详细配置项列表
        @apiSuccess (200) {Number} data.correct_configs.correct_config_item_id 修正配置ID
        @apiSuccess (200) {String} data.correct_configs.field 数据集字段
        @apiSuccess (200) {Object} data.correct_configs.correct_config_detail 修正配置
        @apiSuccess (200) {String} data.correct_configs.correct_config_alias 修正配置别名
        @apiSuccess (200) {String} data.correct_configs.created_by 创建人
        @apiSuccess (200) {String} data.correct_configs.created_at 创建时间
        @apiSuccess (200) {String} data.correct_configs.updated_by 更新人
        @apiSuccess (200) {String} data.correct_configs.updated_at 更新时间
        @apiSuccess (200) {String} data.correct_configs.description 规则配置描述

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "correct_config_id": 1,
                        "data_set_id": "591_table",
                        "bk_biz_id": 591,
                        "flow_id": 1,
                        "node_id": 1,
                        "source_sql": "xxx",
                        "correct_sql": "xxx",
                        "generate_type": "user",
                        "correct_configs": [
                            {
                                "correct_config_item_id": 1,
                                "field": "field1",
                                "correct_config_detail": {
                                    "rules": [
                                        {
                                            "condition": {
                                                "condition_name": "custom_sql_condition",
                                                "condition_type": "custom",
                                                "condition_value": "field1 IS NOT NULL"
                                            },
                                            "handler": {
                                                "handler_name": "fixed_filling",
                                                "handler_type": "filling",
                                                "handler_value_type": "int",
                                                "handler_value": 100
                                            }
                                        }
                                    ],
                                    "output": {
                                        "generate_new_field": false,
                                        "new_field": "",
                                    }
                                },
                                "correct_config_alias": "",
                                "created_by": "admin",
                                "created_at": "2020-07-27 10:30:00",
                                "updated_by": "admin",
                                "updated_at": "2020-07-27 10:31:00"
                            }
                        ],
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00",
                        "description": ""
                    }
                ]
            }
        """
        data_set_id = params["data_set_id"]
        correct_configs = DataQualityCorrectConfig.objects.filter(active=True, data_set_id=data_set_id).order_by("id")

        results = []
        for correct_config in correct_configs:
            correct_config_items = []
            for correct_config_item in DataQualityCorrectConfigItem.objects.filter(correct_config=correct_config):
                correct_config_items.append(
                    {
                        "correct_config_item_id": correct_config_item.id,
                        "field": correct_config_item.field,
                        "correct_config_detail": json.loads(correct_config_item.correct_config_detail),
                        "correct_config_alias": correct_config_item.correct_config_alias,
                        "created_by": correct_config_item.created_by,
                        "created_at": correct_config_item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "updated_by": correct_config_item.updated_by,
                        "updated_at": correct_config_item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "description": correct_config_item.description,
                    }
                )

            results.append(
                {
                    "correct_config_id": correct_config.id,
                    "data_set_id": correct_config.data_set_id,
                    "flow_id": correct_config.flow_id,
                    "node_id": correct_config.node_id,
                    "source_sql": correct_config.source_sql,
                    "correct_sql": correct_config.correct_sql,
                    "generate_type": correct_config.generate_type,
                    "correct_configs": correct_config_items,
                    "created_by": correct_config.created_by,
                    "created_at": correct_config.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_by": correct_config.updated_by,
                    "updated_at": correct_config.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "description": correct_config.description,
                }
            )

        return Response(results)

    def retrieve(self, request, correct_config_id):
        """
        @api {get} /datamanage/dataquality/correct_configs/{correct_config_id}/ 查询单个修正规则配置

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_retrieve
        @apiDescription 查询单个修正规则配置

        @apiSuccess (200) {Number} data.correct_config_id 修正配置ID
        @apiSuccess (200) {Number} data.data_set_id 数据集ID
        @apiSuccess (200) {Number} data.flow_id 数据流ID
        @apiSuccess (200) {Number} data.node_id 数据流节点ID
        @apiSuccess (200) {String} data.source_sql 原始SQL
        @apiSuccess (200) {String} data.correct_sql 修正SQL
        @apiSuccess (200) {String} data.generate_type 生成类型
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间
        @apiSuccess (200) {String} data.description 描述
        @apiSuccess (200) {List} data.correct_configs 修正详细配置项列表
        @apiSuccess (200) {Number} data.correct_configs.correct_config_item_id 修正配置ID
        @apiSuccess (200) {String} data.correct_configs.field 数据集字段
        @apiSuccess (200) {Object} data.correct_configs.correct_config_detail 修正配置
        @apiSuccess (200) {String} data.correct_configs.correct_config_alias 修正配置别名
        @apiSuccess (200) {String} data.correct_configs.created_by 创建人
        @apiSuccess (200) {String} data.correct_configs.created_at 创建时间
        @apiSuccess (200) {String} data.correct_configs.updated_by 更新人
        @apiSuccess (200) {String} data.correct_configs.updated_at 更新时间
        @apiSuccess (200) {String} data.correct_configs.description 规则配置描述

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "correct_config_id": 1,
                        "data_set_id": "591_table",
                        "bk_biz_id": 591,
                        "flow_id": 1,
                        "node_id": 1,
                        "source_sql": "xxx",
                        "correct_sql": "xxx",
                        "generate_type": "user",
                        "correct_configs": [
                            {
                                "correct_config_item_id": 1,
                                "field": "field1",
                                "correct_config_detail": {
                                    "rules": [
                                        {
                                            "condition": {
                                                "condition_name": "custom_sql_condition",
                                                "condition_type": "custom",
                                                "condition_value": "field1 IS NOT NULL"
                                            },
                                            "handler": {
                                                "handler_name": "fixed_filling",
                                                "handler_type": "filling",
                                                "handler_value_type": "int",
                                                "handler_value": 100
                                            }
                                        }
                                    ],
                                    "output": {
                                        "generate_new_field": false,
                                        "new_field": "",
                                    }
                                },
                                "correct_config_alias": "",
                                "created_by": "admin",
                                "created_at": "2020-07-27 10:30:00",
                                "updated_by": "admin",
                                "updated_at": "2020-07-27 10:31:00"
                            }
                        ],
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00",
                        "description": ""
                    }
                ]
            }
        """
        try:
            correct_config = DataQualityCorrectConfig.objects.get(id=correct_config_id)
        except DataQualityCorrectConfig.DoesNotExist:
            logger.error("修正配置({})不存在".format(correct_config_id))
            raise dm_pro_errors.CorrectConfigNotExistError()

        correct_config_items = []
        for correct_config_item in DataQualityCorrectConfigItem.objects.filter(correct_config=correct_config):
            correct_config_items.append(
                {
                    "correct_config_item_id": correct_config_item.id,
                    "field": correct_config_item.field,
                    "correct_config_detail": json.loads(correct_config_item.correct_config_detail),
                    "correct_config_alias": correct_config_item.correct_config_alias,
                    "created_by": correct_config_item.created_by,
                    "created_at": correct_config_item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_by": correct_config_item.updated_by,
                    "updated_at": correct_config_item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "description": correct_config_item.description,
                }
            )

        return Response(
            {
                "correct_config_id": correct_config.id,
                "data_set_id": correct_config.data_set_id,
                "flow_id": correct_config.flow_id,
                "node_id": correct_config.node_id,
                "source_sql": correct_config.source_sql,
                "correct_sql": correct_config.correct_sql,
                "generate_type": correct_config.generate_type,
                "correct_configs": correct_config_items,
                "created_by": correct_config.created_by,
                "created_at": correct_config.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_by": correct_config.updated_by,
                "updated_at": correct_config.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "description": correct_config.description,
            }
        )

    def delete(self, request, correct_config_id):
        """
        @api {delete} /datamanage/dataquality/correct_configs/{correct_config_id}/ 删除单个修正规则配置

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_delete
        @apiDescription 删除单个修正规则配置

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": "ok"
            }
        """
        try:
            correct_config = DataQualityCorrectConfig.objects.get(id=correct_config_id)
        except DataQualityCorrectConfig.DoesNotExist:
            logger.error("修正配置({})不存在".format(correct_config_id))
            raise dm_pro_errors.CorrectConfigNotExistError()

        with transaction.atomic(using="bkdata_basic"):
            DataQualityCorrectConfigItem.objects.filter(correct_config=correct_config).delete()
            correct_config.delete()

        return Response("ok")

    @list_route(methods=["post"], url_path="correct_sql")
    @params_valid(serializer=CorrectSqlSerializer)
    def correct_sql(self, request, params):
        """
        @api {post} /datamanage/dataquality/correct_configs/correct_sql/ 生成修正SQL

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_config_correct_sql
        @apiDescription 生成修正SQL

        @apiParam {String} data_set_id 数据集
        @apiParam {String} source_sql 原始SQL
        @apiParam {Object} correct_configs 修正配置

        @apiSuccess (200) {Number} data.correct_sql 修正SQL

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": "SELECT * FROM table"
            }
        """
        return Response(
            self.generate_correct_sql(
                params["data_set_id"],
                params["source_sql"],
                params["correct_configs"],
            )
        )

    @list_route(methods=["post"], url_path="debug/submit")
    @params_valid(serializer=CorrectDebugSubmitSerializer)
    def debug_submit(self, request, params):
        """
        @api {post} /datamanage/dataquality/correct_configs/debug/submit/ 提交修正SQL调试任务

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_debug_submit
        @apiDescription 提交修正SQL调试任务

        @apiParam {String} source_data_set_id 上游数据集ID
        @apiParam {String} data_set_id 当前数据集ID
        @apiParam {String} source_sql 原始SQL
        @apiParam {Object} correct_configs 修正配置

        @apiSuccess (200) {Number} data.debug_request_id 调试请求ID，用于获取调试结果

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "debug_request_id": "xx"
                }
            }
        """
        # 获取调试用的样本数据
        source_data_set_id = params["source_data_set_id"]
        sample_data = self.get_data_set_sample_data(source_data_set_id)
        sample_file_name, sample_file_path = self.generate_parquet_sample_data(sample_data, source_data_set_id)

        # 把调试用样本数据写到HDFS，使调试Session server能通过相关接口访问
        cluster_name, hosts, physical_table_name = self.get_hdfs_info_by_result_table(DEBUG_VIRTUAL_TABLE)
        hdfs_client = get_hdfs_client(cluster_name, hosts)
        dest_path = os.path.join(physical_table_name, self.VIRTUAL_TABLE_TMP_FOLDER, sample_file_name)
        hdfs_client.copy_from_local(localsrc=sample_file_path, dest=dest_path, overwrite=True)

        # 生成调试SQL
        debug_correct_sql = self.generate_correct_sql(
            params["data_set_id"],
            params["source_sql"],
            params["correct_configs"],
            for_debug=True,
        )

        session_server_id = self.get_session_server_id(CORRECTING_DEBUG_SESSION_KEY)
        codes = [
            """import json""",
            """df = spark_session.create_customize_path_dataframe('{}', ['{}'])""".format(
                DEBUG_VIRTUAL_TABLE,
                os.path.join(self.VIRTUAL_TABLE_TMP_FOLDER, sample_file_name),
            ),
            """df.createOrReplaceTempView('{}')""".format(source_data_set_id),
            """debug_df = spark_session.sql('''{}''')""".format(debug_correct_sql),
            """debug_results = debug_df.collect()""",
            """print(json.dumps([item.asDict() for item in debug_results]))""",
        ]

        res = DataflowApi.interactive_codes.create(
            {
                "server_id": session_server_id,
                "code": "\n".join(codes),
                "geog_area_code": SESSION_GEOG_AREA_CODE,
            },
            raise_exception=True,
        )

        return Response(
            {
                "debug_request_id": res.data.get("id"),
            }
        )

    def get_data_set_sample_data(self, data_set_id):
        channel_storage = self.fetch_data_set_storages(data_set_id, self.SAMPLE_DATA_SOURCE_CLUSTER_TYPE)

        (
            consumer,
            topic,
            partition,
            max_offset,
            min_offset,
        ) = self.get_consumer_by_channel(channel_storage)

        sample_data = []
        target_offset = max(max_offset - self.MAX_SAMPLE_DATA_COUNT, min_offset)
        target_count = min(max_offset - min_offset + 1, self.MAX_SAMPLE_DATA_COUNT)

        topic_partition = TopicPartition(topic=topic, partition=partition, offset=target_offset)
        self.consumer_seek_offset(consumer, topic_partition)
        messages = consumer.consume(target_count, timeout=self.SAMPLE_DATA_TIMEOUT)

        for message in messages:
            data = self.parse_avro_message(message.value())
            sample_data.extend(data[0])

            if len(sample_data) > self.MAX_SAMPLE_DATA_COUNT:
                break

        return sample_data[:100]

    def get_consumer_by_channel(self, channel_storage):
        channel_info = channel_storage.get("storage_channel", {})
        topic_name = str(channel_storage.get("physical_table_name"))

        topic_partition = TopicPartition(topic=topic_name, partition=0)

        if not channel_info.get("cluster_domain"):
            return

        consumer = Consumer(
            {
                "bootstrap.servers": "{}:{}".format(
                    channel_info.get("cluster_domain"), channel_info.get("cluster_port")
                ),
                "group.id": "correction_debug_group",
                "auto.offset.reset": "latest",
            }
        )
        consumer.assign([topic_partition])

        min_offset, max_offset = consumer.get_watermark_offsets(topic_partition)
        return consumer, topic_name, 0, max_offset, min_offset

    def consumer_seek_offset(self, consumer, topic_partition):
        start_time = time.time()
        while True:
            if time.time() - start_time > self.SAMPLE_DATA_TIMEOUT:
                raise dm_pro_errors.SetConsumerOffsetTimeoutError()

            try:
                consumer.seek(topic_partition)
                break
            except Exception:
                time.sleep(0.1)
                continue

    def parse_avro_message(self, message):
        records = []
        decode_data = message.decode("utf8")
        encode_data = decode_data.encode("ISO-8859-1")
        row_file = io.BytesIO(encode_data)
        records.extend([x["_value_"] for x in fastavro.reader(row_file)])
        return records

    def generate_parquet_sample_data(self, sample_data, source_data_set_id):
        sample_df = pd.DataFrame(sample_data)
        sample_table = pa.Table.from_pandas(sample_df)
        sample_file_name = "{}.parquet".format(source_data_set_id)
        sample_file_path = os.path.join(PARQUET_FILE_TMP_FOLDER, sample_file_name)
        pq.write_table(sample_table, sample_file_path)

        return sample_file_name, sample_file_path

    def get_hdfs_info_by_result_table(self, result_table_id):
        hdfs_storage = self.fetch_data_set_storages(result_table_id, self.VIRTUAL_TABLE_CLUSTER_TYPE)
        connection_info = json.loads(hdfs_storage.get("storage_cluster", {}).get("connection_info", "{}"))
        hosts = connection_info.get("hosts")
        port = connection_info.get("port")
        hosts = ",".join(["{}:{}".format(host, port) for host in hosts.split(",")])
        cluster_name = connection_info.get("hdfs_cluster_name")
        physical_table_name = hdfs_storage.get("physical_table_name")
        return cluster_name, hosts, physical_table_name

    @list_route(methods=["get"], url_path="debug/result")
    @params_valid(serializer=CorrectDebugResultSerializer)
    def debug_result(self, request, params):
        """
        @api {get} /datamanage/dataquality/correct_configs/debug/result/ 获取修正SQL调试任务

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_debug_result
        @apiDescription 获取修正SQL调试任务

        @apiParam {String} debug_request_id 调试请求ID

        @apiSuccess (200) {Number} data.started 调试开始时间
        @apiSuccess (200) {Number} data.completed 调试结束时间
        @apiSuccess (200) {String} data.state 调试运行状态
        @apiSuccess (200) {Number} data.progress 调试运行进度
        @apiSuccess (200) {Object} data.output 调试输出结果
        @apiSuccess (200) {String} data.output.status 调试输出结果状态
        @apiSuccess (200) {Object) data.output.data 调试输出内容

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": "SELECT * FROM table"
            }
        """
        debug_request_id = params["debug_request_id"]
        session_server_id = self.get_session_server_id(CORRECTING_DEBUG_SESSION_KEY)

        res = DataflowApi.interactive_codes.retrieve(
            {
                "server_id": session_server_id,
                "code_id": debug_request_id,
                "geog_area_code": SESSION_GEOG_AREA_CODE,
            },
            raise_exception=True,
        )

        debug_results = []
        if res.data.get("state") == "available":
            if res.data.get("output", {}).get("status") == "ok":
                debug_results = json.loads(res.data.get("output", {}).get("data", {}).get("text/plain", {}))

        status = (res.data.get("output") or {}).get("status")
        response = {
            "started": res.data.get("started"),
            "completed": res.data.get("completed"),
            "state": res.data.get("state"),
            "progress": res.data.get("progress"),
            "output": {"status": status, "data": debug_results, "error": {}},
        }
        if status == "error":
            response["output"]["error"].update(
                {
                    "message": res.data.get("output", {}).get("evalue"),
                    "exception": res.data.get("output", {}).get("ename"),
                    "traceback": res.data.get("output", {}).get("traceback"),
                }
            )

        return Response(response)


class DataQualityCorrectHandlerViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityCorrectHandlerTemplate
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = CorrectHandlerTemplateSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True)

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/correct_handlers/ 数据修正处理模板列表

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_handler_template_list
        @apiDescription 数据修正处理模板列表

        @apiSuccess (200) {String} data.handler_template_name 修正处理模板名称
        @apiSuccess (200) {String} data.handler_template_alias 修正处理模板别名
        @apiSuccess (200) {String} data.handler_template_type 修正处理模板类型
        @apiSuccess (200) {String} data.handler_template_config 修正处理模板配置
        @apiSuccess (200) {String} data.description 模板描述
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "id": 1,
                        "handler_template_name": "fixed_filling",
                        "handler_template_alias": "固定值填充",
                        "handler_template_type": "fixed",
                        "handler_template_config": {},
                        "description": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityCorrectHandlerViewSet, self).list(request)


class DataQualityCorrectConditionViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityCorrectConditionTemplate
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = CorrectConditionTemplateSerializer
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.filter(active=True)

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/correct_conditions/ 数据修正判断模板列表

        @apiVersion 3.5.0
        @apiGroup DataQualityCorrection
        @apiName dataquality_correct_condition_template_list
        @apiDescription 数据修正判断模板列表

        @apiSuccess (200) {String} data.condition_template_name 判断模板名称
        @apiSuccess (200) {String} data.condition_template_alias 判断模板别名
        @apiSuccess (200) {String} data.condition_template_type 判断模板类型
        @apiSuccess (200) {String} data.condition_template_config 判断模板配置
        @apiSuccess (200) {String} data.description 模板描述
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "id": 1,
                        "condition_template_name": "custom_sql_condition",
                        "condition_template_alias": "自定义SQL条件",
                        "condition_template_type": "custom",
                        "condition_template_config": {},
                        "description": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityCorrectConditionViewSet, self).list(request)
