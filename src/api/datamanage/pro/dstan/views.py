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


import traceback
from datetime import datetime

from datamanage.exceptions import (
    ParamBlankError,
    ParamFormatError,
    QueryResultEmptyError,
)
from datamanage.lite.tag import tagaction
from datamanage.pizza_settings import AUTH_API_ROOT
from datamanage.pro.datamap.dmaction import (
    meta_dgraph_complex_search,
    parse_tag_codes_list,
)
from datamanage.pro.dstan.models import (
    DmConstraintConfig,
    DmDataTypeConfig,
    DmDetaildataFieldConfig,
    DmIndicatorFieldConfig,
    DmStandardConfig,
    DmStandardContentConfig,
    DmStandardVersionConfig,
    DmUnitConfig,
)
from datamanage.pro.dstan.serializers import (
    DmConstraintConfigSerializer,
    DmDataTypeConfigSerializer,
    DmDetaildataFieldConfigSerializer,
    DmIndicatorFieldConfigSerializer,
    DmStandardConfigSerializer,
    DmStandardContentConfigSerializer,
    DmStandardVersionConfigSerializer,
    StandardPublicityInfoSerializer,
    StandardPublicityListSerializer,
)
from datamanage.pro.dstan.utils import SEARCH_COUNT_DEFAULT
from datamanage.utils.api.datamanage import DatamanageApi
from django.db import connections
from django.db.models import Q
from django.forms.models import model_to_dict
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from common.api import AuthApi
from common.api.base import DataAPI
from common.base_utils import request_fetch, safe_int
from common.decorators import list_route, params_valid
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet, APIViewSet

# constraint of field
constraint_dict = {
    "value_range": _("值范围"),
    "value_enum": _("值枚举"),
    "include": _("包含"),
    "not_include": _("不包含"),
    "start_with": _("开头是"),
    "end_with": _("结尾是"),
    "regex": _("正则表达式"),
}

# window_type_dict
window_type_dict = {
    "none": _("无窗口"),
    "scroll": _("滚动窗口"),
    "slide": _("滑动窗口"),
    "accumulate": _("累加窗口"),
}
# 统计频率
count_freq_dict = {30: "30s", 60: "60s", 180: "180s", 300: "300s", 600: "600s"}
# 窗口长度
window_time_dict = {10: "10min", 30: "30min", 45: "45min", 60: "60min", 24: "24h"}
# 等待时间
waiting_time_dict = {0: "无", 10: "10s", 30: "30s", 60: "60s"}


class DataTypeView(APIModelViewSet):
    # 数据类型信息表
    """
    @api {get} /datamanage/dstan/datatype/ 获取所有的数据类型
    @apiVersion 1.0.0
    @apiGroup Dstan/DataType
    @apiName get_data_type

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = DmDataTypeConfig
    serializer_class = DmDataTypeConfigSerializer
    filter_fields = {}
    ordering_fields = ("id", "created_at")
    ordering = ("-id",)

    def get_queryset(self):
        return self.model.objects.all()


class StandardMarketPermView(APIViewSet):
    # 为数据集市项目增加数据权限
    @list_route(methods=["post"], url_path="add_perm")
    def add_perm(self, request):
        # 需要赋权的表通过post参数result_table_ids传递
        project_id = request_fetch(request, "project_id", 0)
        result_table_ids = request_fetch(request, "result_table_ids", "")
        result = {}
        if not result_table_ids or result_table_ids == "" or project_id < 1:
            return Response(result)

        result_tables = result_table_ids.split(",")
        AuthApi.add_perm = DataAPI(
            method="POST",
            url=AUTH_API_ROOT + "/projects/{project_id}/data/add/",
            module="AUTH",
            url_keys=["project_id"],
            description="为项目增加数据权限",
        )
        for rt_id in result_tables:
            try:
                biz_id = safe_int(rt_id.split("_")[0], 0)
                resp = AuthApi.add_perm(
                    {
                        "project_id": project_id,
                        "bk_biz_id": biz_id,
                        "result_table_id": rt_id,
                    },
                    raise_exception=True,
                )
                result[rt_id] = {
                    "code": resp.code,
                    "data": resp.data,
                    "message": resp.message,
                }

            except Exception as e:
                result[rt_id] = {"result": False, "error": str(e)}

        return Response(result)


class StandardView(APIViewSet):
    # 创建标准
    @list_route(methods=["post"], url_path="create")
    @auto_meta_sync(using="bkdata_basic")
    def standard_create(self, request):
        """
        @api {post} /datamanage/dstan/standard/create/ 创建标准
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName standard_create
        @apiParam {version_info} Dict 版本信息
        @apiParam {standard_content} List 标准内容
        @apiParam {basic_info} Dict 标准基本信息
        @apiParamExample {json} 参数样例:
            {
                "version_info":{
                    "created_by":"admin",
                    "standard_version_status":"developing",
                    "description":"初始",
                    "standard_version":"v1.0"
                },
                "standard_content":[
                    {
                        "standard_fields":[
                            {
                                "field_type":"string",
                                "field_alias":"a",
                                "description":"",
                                "constraint":{

                                },
                                "created_by":"admin",
                                "active":1,
                                "source_record_id":0,
                                "field_name":"a",
                                "field_index":0
                            }
                        ],
                        "standard_info":{
                            "source_record_id":0,
                            "description":"明细",
                            "window_period":{

                            },
                            "created_by":"admin",
                            "standard_content_type":"detaildata",
                            "parent_id":"[]",
                            "standard_content_name":"明细",
                            "category_id":12
                        }
                    },
                    {
                        "standard_fields":[
                            {
                                "field_type":"string",
                                "field_alias":"b",
                                "description":"",
                                "constraint":{

                                },
                                "is_dimension":"true",
                                "created_by":"admin",
                                "active":1,
                                "source_record_id":0,
                                "field_name":"b",
                                "field_index":0
                            }
                        ],
                        "standard_info":{
                            "standard_content_sql":"select * from ${明细}",
                            "parent_id_type":"detaildata",
                            "source_record_id":0,
                            "description":"",
                            "filter_cond":"where",
                            "standard_content_type":"indicator",
                            "created_by":"admin",
                            "window_period":{
                                "window_type":"none",
                                "window_time":null,
                                "count_freq":null,
                                "waiting_time":null
                            },
                            "standard_content_name":"原子",
                            "category_id":12
                        }
                    }
                ],
                "basic_info":{
                    "standard_name":"文档接口标准",
                    "category_id":12,
                    "description":"文档接口标准",
                    "created_by":"admin"
                }
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "standard_id":231,
                    "version_id":473
                }
            }
        """
        # 将传递参数的明细数据放在原子指标前面
        # request.data['standard_content'].sort(cmp_standard_content)
        # 数据标准总表
        basic_info = self.params_valid(serializer=DmStandardConfigSerializer, params=request.data["basic_info"])
        dm_standard_config = DmStandardConfig(**basic_info)
        dm_standard_config.save()

        param_dict = request.data
        param_dict["dm_standard_config_id"] = dm_standard_config.id
        version_id = self.update_version_standard(param_dict)

        result = {}
        result["version_id"] = version_id
        result["standard_id"] = dm_standard_config.id

        return Response(result)

    # 获取单位信息
    @list_route(methods=["get"], url_path="get_unit")
    def unit_get(self, request):
        try:
            unit_config = DmUnitConfig.objects.all()
        except DmUnitConfig.DoesNotExist:
            logger.error("DmUnitConfig get all columns fail")
            return Response({})
        category_list = []
        unit_list = []
        for each_unit in unit_config:
            if each_unit.category_name not in category_list:
                category_list.append(each_unit.category_name)
                unit_list.append(
                    {
                        "value": each_unit.category_name,
                        "label": each_unit.category_alias,
                        "children": [
                            {
                                "value": each_unit.name,
                                "label": each_unit.alias,
                            }
                        ],
                    }
                )
            else:
                for each_category in unit_list:
                    if each_unit.category_name == each_category["value"]:
                        each_category["children"].append(
                            {
                                "value": each_unit.name,
                                "label": each_unit.alias,
                            }
                        )
                        break
        return Response(unit_list)

    # 获取单位中文名
    @list_route(methods=["get"], url_path="get_unit_list")
    def unit_list(self, request):
        try:
            unit_config = DmUnitConfig.objects.all()
        except DmUnitConfig.DoesNotExist:
            logger.error("DmUnitConfig get all columns fail")
            return Response({})
        res = {}
        for each_unit in unit_config:
            res[each_unit.name] = each_unit.alias
        return Response(res)

    @list_route(methods=["get"], url_path="get_standard")
    def standard_get(self, request):
        """
        @api {get} /datamanage/dstan/standard/get_standard/ 获取标准详情
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName standard_get
        @apiParam {Int} dm_standard_config_id 标准id
        @apiParam {Int} dm_standard_version_config_id 标准版本id
        @apiParam {Int} dm_standard_content_config_id 标准内容id
        @apiParam {Boolean} online 标准是否上线
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "version_info":{
                    "description":"标准化单位",
                    "standard_id":5,
                    "created_by":"admin",
                    "standard_version":"v1.3",
                    "standard_version_status":"online",
                    "id":239,
                    "updated_by":null
                },
                "standard_content":[],
                "basic_info":{
                    "description":"xx",
                    "created_at":"2019-03-12 00:00:00",
                    "updated_at":"2019-03-12 00:00:00",
                    "created_by":"admin",
                    "standard_name":"yy",
                    "active":true,
                    "category_id":40,
                    "id":5,
                    "updated_by":null
                }
            },
            "result":true
        }
        """
        result = {}
        # 获取传入参数
        dm_standard_config_id = request.query_params.get("dm_standard_config_id", "")
        dm_standard_version_config_id = request.query_params.get("dm_standard_version_config_id", "")
        dm_standard_content_config_id = request.query_params.get("dm_standard_content_config_id", "")
        online = request.query_params.get("online", "false")
        if dm_standard_content_config_id.isdigit():
            try:
                dm_standard_content_config = DmStandardContentConfig.objects.get(
                    id=dm_standard_content_config_id, active=True
                )
            except DmStandardContentConfig.DoesNotExist:
                logger.error("get DmStandardContentConfig content error failed for dm_standard_content_config_id error")
                return Response(result)
            # 根据标准内容id，获取版本id，并判断所获得的版本id和传入的版本id是否一致
            standard_version_id_tmp = str(dm_standard_content_config.standard_version_id)
            if not dm_standard_version_config_id:
                dm_standard_version_config_id = standard_version_id_tmp
            if dm_standard_version_config_id != standard_version_id_tmp:
                logger.error("standard_version_id and standard_content_id do not match")
                return Response({})
            # 根据标准id，获得标准id，并判断所获得的标准id和传入的标准id是否一致
            try:
                dm_standard_version_config = DmStandardVersionConfig.objects.get(id=dm_standard_version_config_id)
            except DmStandardVersionConfig.DoesNotExist:
                logger.error("get DmStandardVersionConfig content failed for dm_standard_version_config_id error")
                return Response(result)
            standard_id_tmp = str(dm_standard_version_config.standard_id)
            if not dm_standard_config_id:
                dm_standard_config_id = standard_id_tmp
            if dm_standard_config_id != standard_id_tmp:
                logger.error("standard_version_id and standard_id do not match")
                return Response({})

        if (
            not dm_standard_config_id.isdigit()
            and not dm_standard_version_config_id.isdigit()
            and not dm_standard_content_config_id.isdigit()
        ):
            return Response({})

        if dm_standard_config_id.isdigit():
            # 获取数据标准总表
            try:
                dm_standard_config = DmStandardConfig.objects.get(id=dm_standard_config_id, active=True)
            except DmStandardConfig.DoesNotExist:
                logger.error("get DmStandardConfig content failed for dm_standard_config empty")
                return Response(result)
            dm_standard_config_dict = model_to_dict(dm_standard_config)
            dm_standard_config_dict["created_at"] = (
                dm_standard_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if dm_standard_config.created_at else None
            )
            dm_standard_config_dict["updated_at"] = (
                dm_standard_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if dm_standard_config.updated_at else None
            )
            result["basic_info"] = dm_standard_config_dict
        # else:
        #     logger.error('dm_standard_config_id非数字类型，请核实后再传入参数')
        #     return Response({})

        # 获取数据标准版本表
        if dm_standard_version_config_id.isdigit():
            dm_standard_version_config_id = int(dm_standard_version_config_id)
        else:
            if dm_standard_config:
                if online == "true":
                    query_dict = {
                        "standard_id": dm_standard_config.id,
                        "standard_version_status": "online",
                    }
                else:
                    query_dict = {
                        "standard_id": dm_standard_config.id,
                    }

                if len(DmStandardVersionConfig.objects.filter(**query_dict)) > 0:

                    dm_standard_version_config_newest = DmStandardVersionConfig.objects.filter(**query_dict).order_by(
                        "-created_at"
                    )[0]
                    if dm_standard_version_config_newest:
                        dm_standard_version_config_id = dm_standard_version_config_newest.id
                    else:
                        # 这里参数需要进行错误返回，先不暂时处理，等待后续完成
                        dm_standard_version_config_id = 0
                else:
                    raise QueryResultEmptyError
        try:
            dm_standard_version_config = DmStandardVersionConfig.objects.get(id=dm_standard_version_config_id)
        except DmStandardVersionConfig.DoesNotExist:
            logger.error("get DmStandardVersionConfig content failed for dm_standard_version_config empty")
            return Response({})

        # 校验version_id和标准id相对应
        if dm_standard_config_id.isdigit():
            if dm_standard_version_config.standard_id != int(dm_standard_config_id):
                logger.error("dm_standard_config_id and dm_standard_version_config_id do not match")
                return Response({})

        dm_standard_version_config_dict = model_to_dict(dm_standard_version_config)
        result["version_info"] = dm_standard_version_config_dict

        # 获取标准内容:
        # 1.明细数据标准：明细数据标准字段详情表（值约束）
        # 2.原子指标标准：原子指标字段详情表（值约束）
        result["standard_content"] = []
        standard_content_list = DmStandardContentConfig.objects.filter(
            active=True, standard_version_id=dm_standard_version_config.id
        ).order_by("id")
        standard_content_maps = {}
        for standard_content in standard_content_list:
            standard_content_dict = {}

            standard_info_dict = model_to_dict(standard_content)
            standard_content_maps[standard_content.id] = standard_content
            # 处理parent_type 和 parent_name
            if standard_content.standard_content_type == "indicator":
                if len(standard_content.parent_id) > 0 and standard_content_maps.get(standard_content.parent_id[0]):
                    standard_info_dict["parent_type"] = standard_content_maps.get(
                        standard_content.parent_id[0]
                    ).standard_content_type
                    standard_info_dict["parent_name"] = standard_content_maps.get(
                        standard_content.parent_id[0]
                    ).standard_content_name

            standard_content_dict["standard_info"] = standard_info_dict
            standard_content_dict["standard_fields"] = []

            if standard_content.standard_content_type == "detaildata":
                standard_fields = DmDetaildataFieldConfig.objects.filter(
                    active=True, standard_content_id=standard_content.id
                )
            else:
                standard_fields = DmIndicatorFieldConfig.objects.filter(
                    active=True, standard_content_id=standard_content.id
                )
            for standard_field in standard_fields:
                standard_field_dict = model_to_dict(standard_field)

                # 获取约束值
                try:
                    dm_constraint_config = DmConstraintConfig.objects.get(active=True, id=standard_field.constraint_id)
                    standard_field_dict["constraint"] = model_to_dict(dm_constraint_config)
                    standard_content_dict["standard_fields"].append(standard_field_dict)
                except DmConstraintConfig.DoesNotExist:
                    standard_content_dict["standard_fields"].append(standard_field_dict)
                    logger.info("DmConstraintConfig.DoesNotExist")

            result["standard_content"].append(standard_content_dict)

        return Response(result)

    def update_model_list(self, model_list, params_dict):
        if model_list:
            for model_obj in model_list:
                model_obj.__dict__.update(**params_dict)
                model_obj.save()

    def update_active_value(self, model_list, val):
        if model_list:
            for model_obj in model_list:
                model_obj.active = val
                model_obj.save()

    # 更新标准
    @list_route(methods=["post"], url_path="update")
    # @transaction.atomic(using='bkdata_basic')
    @auto_meta_sync(using="bkdata_basic")
    def standard_update(self, request):
        """
        @api {post} /datamanage/dstan/standard/update/ 更新标准
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName standard_update
        @apiParam {version_info} Dict 版本信息
        @apiParam {standard_content} List 标准内容
        @apiParam {basic_info} Dict 标准基本信息
        @apiParam {dm_standard_config_id} Int 标准id
        @apiParamExample {json} 参数样例:
            {
                "dm_standard_config_id": 231,
                "version_info": {
                    "version_id": "473",
                    "created_by": "admin",
                    "standard_version_status": "developing",
                    "description": "文档接口标准编辑",
                    "standard_version": "v1.0"
                },
                "standard_content": [
                    {
                        "standard_fields": [
                            {
                                "field_type": "string",
                                "field_alias": "a",
                                "description": "",
                                "constraint": {

                                },
                                "constraint_id": null,
                                "created_by": "admin",
                                "active": 1,
                                "source_record_id": 0,
                                "field_name": "a",
                                "id": 3378,
                                "field_index": 0
                            }
                        ],
                        "standard_info": {
                            "source_record_id": 0,
                            "description": "明细",
                            "standard_content_type": "detaildata",
                            "created_by": "admin",
                            "window_period": {

                            },
                            "parent_id": "[]",
                            "standard_content_name": "明细标准",
                            "category_id": 12,
                            "id": 1168
                        }
                    },
                    {
                        "standard_fields": [
                            {
                                "field_type": "string",
                                "field_alias": "b",
                                "description": "",
                                "constraint": {

                                },
                                "constraint_id": null,
                                "is_dimension": "true",
                                "created_by": "admin",
                                "active": 1,
                                "source_record_id": 0,
                                "field_name": "b",
                                "id": 4232,
                                "field_index": 0
                            }
                        ],
                        "standard_info": {
                            "standard_content_sql": "select * from ${明细}",
                            "parent_id_type": "detaildata",
                            "source_record_id": 0,
                            "description": "",
                            "filter_cond": "where",
                            "standard_content_type": "indicator",
                            "created_by": "admin",
                            "window_period": {
                                "window_type": "none",
                                "window_time": null,
                                "count_freq": null,
                                "waiting_time": null
                            },
                            "standard_content_name": "原子标准",
                            "category_id": 12,
                            "id": 1169
                        }
                    }
                ],
                "basic_info": {
                    "standard_name": "文档接口标准",
                    "category_id": 12,
                    "description": "文档接口标准",
                    "created_by": "admin"
                }
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "version_id":"473"
                }
            }
        """
        # 获取版本参数
        dm_standard_config_id = request.data.get("dm_standard_config_id", -1)
        # 参数校验，确保dm_standard_config_id传入
        if dm_standard_config_id == -1:
            # 失败处理
            raise ParamBlankError

        # request.data['standard_content'].sort(cmp_standard_content)

        # 获取数据标准总表
        basic_info = self.params_valid(serializer=DmStandardConfigSerializer, params=request.data["basic_info"])
        basic_info["updated_at"] = datetime.now()
        # DmStandardConfig.objects.filter(id=dm_standard_config_id).update(**basic_info)
        self.update_model_list(DmStandardConfig.objects.filter(id=dm_standard_config_id), basic_info)
        param_dict = request.data
        param_dict["dm_standard_config_id"] = dm_standard_config_id

        result = {}

        # 如果当前的版本对应状态是上线，就不进行下面标准内容的删除
        if param_dict.get("version_info").get("standard_version_status") == "developing":
            # 标准需要更新的内容
            try:
                stan_update_dict = {
                    "standard_name": param_dict["basic_info"].get("standard_name"),
                    "category_id": param_dict["basic_info"].get("category_id"),
                    "description": param_dict["basic_info"].get("description"),
                    "updated_by": param_dict["basic_info"].get("created_by"),
                }
                # DmStandardConfig.objects.filter(id=param_dict["dm_standard_config_id"]).update(**stan_update_dict)
                self.update_model_list(
                    DmStandardConfig.objects.filter(id=param_dict["dm_standard_config_id"]),
                    stan_update_dict,
                )
            except DmStandardConfig.DoesNotExist:
                logger.info("DmStandardConfig.DoesNotExist")
                raise QueryResultEmptyError
            # 版本需要更新的内容
            try:
                version_update_dict = {
                    "description": param_dict["version_info"].get("description"),
                    "updated_by": param_dict["version_info"].get("created_by"),
                }
                # DmStandardVersionConfig.objects.filter(id=param_dict["version_info"].get('version_id'))
                # .update(**version_update_dict)
                self.update_model_list(
                    DmStandardVersionConfig.objects.filter(id=param_dict["version_info"].get("version_id")),
                    version_update_dict,
                )
                result["version_id"] = param_dict["version_info"].get("version_id")
            except DmStandardVersionConfig.DoesNotExist:
                logger.info("DmStandardVersionConfig.DoesNotExist")
                raise QueryResultEmptyError
            # 更新明细数据标准和原子指标标准的标准内容
            # 首先拿到version_id对应的content_id列表
            content_list = DmStandardContentConfig.objects.filter(
                standard_version_id=param_dict["version_info"].get("version_id"),
                active=True,
            )
            # 标准内容对应的id列表
            content_id_list = []
            for each_content in content_list:
                content_id_list.append(each_content.id)
            indicator_ids = []
            detaildata_id = None
            for each_content in param_dict["standard_content"]:
                if each_content["standard_info"]["id"] != -1 and each_content["standard_info"]["id"] in content_id_list:
                    if each_content["standard_info"]["standard_content_type"] == "indicator":
                        indicator_ids.append(each_content["standard_info"]["id"])
                    else:
                        detaildata_id = each_content["standard_info"]["id"]
                    content_update_dict = each_content["standard_info"]
                    content_update_dict["updated_by"] = each_content["standard_info"]["created_by"]
                    content_update_dict.pop("created_by")
                    if "parent_id_type" in list(content_update_dict.keys()):
                        content_update_dict.pop("parent_id_type")
                    # DmStandardContentConfig.objects.filter(id=each_content['standard_info']['id'])
                    # .update(**content_update_dict)
                    self.update_model_list(
                        DmStandardContentConfig.objects.filter(id=each_content["standard_info"]["id"]),
                        content_update_dict,
                    )
                    # 将之前就有的content_id从列表中删除
                    content_id_list.remove(each_content["standard_info"]["id"])
                elif each_content["standard_info"]["id"] == -1:
                    if each_content["standard_info"]["standard_content_type"] == "indicator":
                        if each_content["standard_info"]["parent_id_type"] == "detaildata":
                            each_content["standard_info"]["parent_id"] = [detaildata_id]
                        else:
                            if each_content["standard_info"]["parent_id_index"] >= len(indicator_ids):
                                raise ParamFormatError
                            else:
                                each_content["standard_info"]["parent_id"] = [
                                    indicator_ids[each_content["standard_info"]["parent_id_index"]]
                                ]
                    else:
                        each_content["standard_info"]["parent_id"] = []
                    each_content["standard_info"]["standard_version_id"] = param_dict["version_info"].get("version_id")
                    dm_standard_content_dict = self.params_valid(
                        serializer=DmStandardContentConfigSerializer,
                        params=each_content["standard_info"],
                    )
                    dm_standard_content_config = DmStandardContentConfig(**dm_standard_content_dict)
                    dm_standard_content_config.save()
                    each_content["standard_info"]["id"] = dm_standard_content_config.id

                # 字段对应的id列表
                field_id_list = []
                if each_content["standard_info"]["standard_content_type"] == "detaildata":
                    field_list = DmDetaildataFieldConfig.objects.filter(
                        standard_content_id=each_content["standard_info"]["id"]
                    )
                else:
                    field_list = DmIndicatorFieldConfig.objects.filter(
                        standard_content_id=each_content["standard_info"]["id"]
                    )
                for each_field in field_list:
                    field_id_list.append(each_field.id)
                for each_field in each_content["standard_fields"]:
                    # 约束更新，如果有约束的话
                    if each_field.get("constraint_id"):
                        if len(list(each_field["constraint"].keys())) != 0:
                            constraint_update_dict = each_field["constraint"]
                            constraint_update_dict["updated_by"] = each_field["constraint"]["created_by"]
                            constraint_update_dict.pop("created_by")
                            # DmConstraintConfig.objects.filter(id=each_field['constraint_id'])
                            # .update(**constraint_update_dict)
                            self.update_model_list(
                                DmConstraintConfig.objects.filter(id=each_field["constraint_id"]),
                                constraint_update_dict,
                            )
                        else:
                            dcsc2_list = DmConstraintConfig.objects.filter(id=each_field["constraint_id"])
                            # .update(active=False)
                            for dcsc2_obj in dcsc2_list:
                                dcsc2_obj.active = False
                                dcsc2_obj.save()
                    # 如果没有约束的话
                    else:
                        if len(list(each_field["constraint"].keys())) != 0:
                            constraint = self.params_valid(
                                serializer=DmConstraintConfigSerializer,
                                params=each_field["constraint"],
                            )
                            dm_constraint_config = DmConstraintConfig(**constraint)
                            dm_constraint_config.save()
                            each_field["constraint_id"] = dm_constraint_config.id

                    # 更新field内容，如果字段之前已有
                    if each_field["id"] != -1 and each_field["id"] != 0 and each_field["id"] in field_id_list:
                        field_update_dict = each_field
                        field_update_dict["updated_by"] = each_field["created_by"]
                        field_update_dict.pop("created_by")
                        field_update_dict.pop("constraint")
                        if "is_dimension" in list(field_update_dict.keys()):
                            field_update_dict.pop("is_dimension")
                        if each_content["standard_info"]["standard_content_type"] == "detaildata":
                            # DmDetaildataFieldConfig.objects.filter(id=each_field['id']).update(**field_update_dict)
                            self.update_model_list(
                                DmDetaildataFieldConfig.objects.filter(id=each_field["id"]),
                                field_update_dict,
                            )
                        else:
                            field_update_dict["compute_model_id"] = 0
                            # DmIndicatorFieldConfig.objects.filter(id=each_field['id']).update(**field_update_dict)
                            self.update_model_list(
                                DmIndicatorFieldConfig.objects.filter(id=each_field["id"]),
                                field_update_dict,
                            )

                        field_id_list.remove(each_field["id"])
                    # create field内容，字段之前没有
                    elif each_field["id"] == -1 or each_field["id"] == 0:
                        each_field["standard_content_id"] = each_content["standard_info"]["id"]
                        if each_content["standard_info"]["standard_content_type"] == "detaildata":
                            dm_detaildata_field_config_dict = self.params_valid(
                                serializer=DmDetaildataFieldConfigSerializer,
                                params=each_field,
                            )
                            dm_detaildata_field_config = DmDetaildataFieldConfig(**dm_detaildata_field_config_dict)
                            dm_detaildata_field_config.save()
                        else:
                            if "is_dimension" in list(each_field.keys()):
                                each_field["is_dimension"] = True if each_field["is_dimension"] == "true" else False
                            each_field["compute_model_id"] = 0
                            dm_indicator_field_config_dict = self.params_valid(
                                serializer=DmIndicatorFieldConfigSerializer,
                                params=each_field,
                            )
                            dm_indicator_field_config = DmIndicatorFieldConfig(**dm_indicator_field_config_dict)
                            dm_indicator_field_config.save()

                # 软删除掉不用的字段
                if each_content["standard_info"]["standard_content_type"] == "detaildata":
                    for each_field_id in field_id_list:
                        # DmDetaildataFieldConfig.objects.filter(id=each_field_id).update(active=False)
                        self.update_active_value(
                            DmDetaildataFieldConfig.objects.filter(id=each_field_id),
                            False,
                        )

                else:
                    for each_field_id in field_id_list:
                        # DmIndicatorFieldConfig.objects.filter(id=each_field_id).update(active=False)
                        self.update_active_value(
                            DmIndicatorFieldConfig.objects.filter(id=each_field_id),
                            False,
                        )

            # 软删除掉不用的content
            for each_content_id in content_id_list:
                # DmStandardContentConfig.objects.filter(id=each_content_id).update(active=False)
                self.update_active_value(DmStandardContentConfig.objects.filter(id=each_content_id), False)

        if (
            param_dict.get("version_info").get("standard_version_status") == "online"
            or param_dict.get("version_info").get("standard_version_status") == "offline"
        ):
            # 更新版本
            version_id = self.update_version_standard(param_dict)
            result["version_id"] = version_id

        return Response(result)

    # 更新版本号，标准内容，明细数据标准&原子指标标准及约束条件
    def update_version_standard(self, param_dict):
        # 数据标准版本表
        version_info_tmp = param_dict["version_info"]
        version_info_tmp["standard_id"] = param_dict["dm_standard_config_id"]
        version_info = self.params_valid(serializer=DmStandardVersionConfigSerializer, params=version_info_tmp)
        if version_info["standard_version_status"] == "online" or version_info["standard_version_status"] == "offline":
            version_info["standard_version_status"] = "developing"
        dm_standard_version_config = DmStandardVersionConfig(**version_info)
        dm_standard_version_config.save()

        # 标准内容:
        # 1.明细数据标准：明细数据标准字段详情表（值约束）
        # 2.原子指标标准：原子指标字段详情表（值约束）
        standard_content_list = param_dict["standard_content"]

        # 明细数据content_id,默认为-1表示不可用，待插入明细数据到数据库后detaildata_id便可用
        # 所以需要保证前端传入的参数明细数据指标在原子指标之前
        detaildata_id = -1
        indicator_ids = []

        for content_item in standard_content_list:
            # 明细数据标准&原子指标标准 基本信息
            standard_info = content_item["standard_info"]

            # 处理parent_id，如果是detaildata则保持不变并保持不处理，
            # 如果是indicator则寻找合适的处理parent_id
            if standard_info["standard_content_type"] == "indicator":
                if standard_info["parent_id_type"] == "detaildata":
                    standard_info["parent_id"] = [detaildata_id]
                else:
                    if standard_info["parent_id_index"] >= len(indicator_ids):
                        raise ParamFormatError
                    else:
                        standard_info["parent_id"] = [indicator_ids[standard_info["parent_id_index"]]]
            else:
                standard_info["parent_id"] = []

            standard_info["standard_version_id"] = dm_standard_version_config.id
            version_info_dict = self.params_valid(serializer=DmStandardContentConfigSerializer, params=standard_info)
            dm_standard_content_config = DmStandardContentConfig(**version_info_dict)
            dm_standard_content_config.save()

            # 保存detaildata_id和indicator_ids，确保后续原子指标插入时可以找到他们对应的parent_id
            if standard_info["standard_content_type"] == "indicator":
                indicator_ids.append(dm_standard_content_config.id)
            else:
                detaildata_id = dm_standard_content_config.id

            # 明细数据标准&原子指标标准 字段信息
            standard_fields = content_item["standard_fields"]

            for standard_field in standard_fields:
                # 字段约束
                try:
                    if len(list(standard_field["constraint"].keys())) != 0:
                        constraint = self.params_valid(
                            serializer=DmConstraintConfigSerializer,
                            params=standard_field["constraint"],
                        )
                        dm_constraint_config = DmConstraintConfig(**constraint)
                        dm_constraint_config.save()
                        standard_field["constraint_id"] = dm_constraint_config.id
                except Exception as e:
                    logger.error("dm_constraint_config params_valid or save failed for:{}".format(str(e)))
                    traceback.print_exc()
                standard_field["standard_content_id"] = dm_standard_content_config.id

                # 明细数据标准字段详情表&原子指标字段详情表
                if dm_standard_content_config.standard_content_type == "detaildata":
                    # 明细数据标准
                    dm_detaildata_field_config_dict = self.params_valid(
                        serializer=DmDetaildataFieldConfigSerializer,
                        params=standard_field,
                    )
                    dm_detaildata_field_config = DmDetaildataFieldConfig(**dm_detaildata_field_config_dict)
                    dm_detaildata_field_config.save()
                else:
                    # 原子指标标准
                    # 目前并不是每一个字段都有一个加工方式(原子指标用的是统一的sql)，因此令compute_model_id为0
                    standard_field["compute_model_id"] = 0
                    dm_indicator_field_config_dict = self.params_valid(
                        serializer=DmIndicatorFieldConfigSerializer,
                        params=standard_field,
                    )
                    dm_indicator_field_config = DmIndicatorFieldConfig(**dm_indicator_field_config_dict)
                    dm_indicator_field_config.save()
        return dm_standard_version_config.id

    # 获取某标准下所有版本信息
    @list_route(methods=["get"], url_path="get_versions")
    def standard_versions_get(self, request):
        result = []
        # 获取传入参数
        dm_standard_config_id = request.query_params.get("dm_standard_config_id", "")

        if not dm_standard_config_id.isdigit():
            return Response({})
        else:
            dm_standard_config_id = int(dm_standard_config_id)

        versions = DmStandardVersionConfig.objects.filter(standard_id=dm_standard_config_id).order_by("-created_at")
        if len(versions) > 0:
            for version in versions:
                version_dict = model_to_dict(version)
                result.append(version_dict)

        return Response(result)

    # 全文搜索标准
    @list_route(methods=["get"], url_path="search")
    def standard_search(self, request):
        """
        @api {get} /datamanage/dstan/standard/search/ 获取标准
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName standard_search
        @apiParam {Int} [count] 标准个数
        @apiParam {Int} [is_online] 是否在线
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":[],
            "result":true
        }
        """
        # 获取传入参数
        # 前端要取多少条数据
        count = request.query_params.get("count", SEARCH_COUNT_DEFAULT)
        is_online = request.query_params.get("is_online", "-1")

        try:
            count = int(count)
            is_online = int(is_online)
            if count < 0 or count > SEARCH_COUNT_DEFAULT:
                count = SEARCH_COUNT_DEFAULT
        except ValueError:
            logger.info("convert string to int error")
            count = SEARCH_COUNT_DEFAULT
        content = request.query_params.get("content", "")
        category_id = request.query_params.get("category_id", "")

        standard_search = DmStandardConfig.objects.filter(active=True).order_by("-updated_at")
        if category_id:
            standard_search = standard_search.filter(category_id=category_id)
        if content:
            standard_search = standard_search.filter(
                Q(standard_name__icontains=content)
                | Q(description__icontains=content)
                | Q(created_by__icontains=content)
            )

        standard_search = standard_search[:count]

        result = []
        for standard in standard_search:
            standard_dict, success = get_standard_other_info(standard, is_online)
            if success:
                result.append(standard_dict)
                if len(result) >= count:
                    break

        return Response(result)

    # 删除标准
    @list_route(methods=["post"], url_path="delete")
    # @transaction.atomic(using='bkdata_basic')
    @auto_meta_sync(using="bkdata_basic")
    def standard_delete(self, request):
        """
        @api {post} /datamanage/dstan/standard/delete/ 删除标准
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName standard_delete
        @apiParam {dm_standard_config_id} Int 标准id
        @apiParamExample {json} 参数样例:
            {"dm_standard_config_id": 231}
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": "success",
                "result": true
            }
        """
        # 获取版本参数
        dm_standard_config_id = request.data.get("dm_standard_config_id", -1)
        # 参数校验，确保dm_standard_config_id传入
        if dm_standard_config_id == -1:
            # 失败处理
            raise ParamBlankError

        # 获取数据标准总表
        # DmStandardConfig.objects.filter(id=dm_standard_config_id).update(active=False)
        self.update_active_value(DmStandardConfig.objects.filter(id=dm_standard_config_id), False)
        # deleted version list
        version_delete_list = DmStandardVersionConfig.objects.filter(standard_id=dm_standard_config_id)
        for each_version in version_delete_list:
            # deleted content list
            content_delete_list = DmStandardContentConfig.objects.filter(standard_version_id=each_version.id)
            # content_delete_list.update(active=False)
            self.update_active_value(content_delete_list, False)
            for each_content in content_delete_list:
                # deleted detail and indicator
                detail_delete_list = DmDetaildataFieldConfig.objects.filter(standard_content_id=each_content.id)
                # detail_delete_list.update(active=False)
                self.update_active_value(detail_delete_list, False)
                for each_detail in detail_delete_list:
                    if each_detail.constraint_id:
                        # DmConstraintConfig.objects.filter(id=each_detail.constraint_id).update(active=False)
                        self.update_active_value(
                            DmConstraintConfig.objects.filter(id=each_detail.constraint_id),
                            False,
                        )
                # deleted detail and indicator
                indicator_delete_list = DmIndicatorFieldConfig.objects.filter(standard_content_id=each_content.id)
                # indicator_delete_list.update(active=False)
                self.update_active_value(indicator_delete_list, False)
                for each_indicator in indicator_delete_list:
                    if each_indicator.constraint_id:
                        # DmConstraintConfig.objects.filter(id=each_indicator.constraint_id).update(active=False)
                        self.update_active_value(
                            DmConstraintConfig.objects.filter(id=each_indicator.constraint_id),
                            False,
                        )

        return Response("success")

    @list_route(methods=["post"], url_path="set_online")
    # @transaction.atomic(using='bkdata_basic')
    @auto_meta_sync(using="bkdata_basic")
    def set_online(self, request):
        """
        @api {post} /datamanage/dstan/standard/set_online/ 标准上线
        @apiVersion 0.1.0
        @apiGroup Standard
        @apiName set_online
        @apiParam {dm_standard_config_id} Int 版本id
        @apiParamExample {json} 参数样例:
            {"dm_standard_version_config_id": 476}
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": "success",
                "result": true
            }
        """
        version_id = request.data["version_id"]
        # if not version_id.isdigit():
        #     raise ParamFormatError

        # 把同一标准下其它上线版本下线
        dsvc_list = DmStandardVersionConfig.objects.filter(id=version_id)
        if len(dsvc_list) > 0:
            dm_standard_version_config = dsvc_list[0]
            dsvc1_list = DmStandardVersionConfig.objects.filter(
                standard_id=dm_standard_version_config.standard_id,
                standard_version_status="online",
            )
            for dsvc1_obj in dsvc1_list:
                dsvc1_obj.standard_version_status = "offline"
                dsvc1_obj.save()

            dm_standard_version_config.standard_version_status = "online"
            dm_standard_version_config.save()

        return Response("success")

    # 删除版本及对应的指标，约束(是原来没做好developing标准的update时的备用方案)
    def delete_version(self, version_id):
        if len(DmStandardVersionConfig.objects.filter(id=version_id)) <= 0:
            return True
        version_config = DmStandardVersionConfig.objects.filter(id=version_id)[0]

        standard_content_list = (
            DmStandardContentConfig.objects.filter().filter(standard_version_id=version_config.id).order_by("id")
        )
        for standard_content in standard_content_list:
            if standard_content.standard_content_type == "detaildata":
                standard_fields = DmDetaildataFieldConfig.objects.filter(
                    active=True, standard_content_id=standard_content.id
                )
            if standard_content.standard_content_type == "indicator":
                standard_fields = DmIndicatorFieldConfig.objects.filter(
                    active=True, standard_content_id=standard_content.id
                )

            for standard_field in standard_fields:
                # 删除约束条件
                DmConstraintConfig.objects.filter(active=True, id=standard_field.constraint_id).delete()
                # 删除详细字段
                standard_field.delete()

            # 删除标准内容
            standard_content.delete()

        # 删除version
        version_config.delete()

        return True


# 获取标准的其它信息
def get_standard_other_info(dm_standard_config, is_online=False):
    indicator = []
    detaildata = []

    dm_standard_config_dict = model_to_dict(dm_standard_config)
    # 获取最新版本号
    if len(DmStandardVersionConfig.objects.filter(standard_id=dm_standard_config.id)) > 0:
        if is_online == 1:

            dm_standard_version_config_newest_tmp = DmStandardVersionConfig.objects.filter(
                standard_id=dm_standard_config.id, standard_version_status="online"
            ).order_by("-created_at")
            if len(dm_standard_version_config_newest_tmp) > 0:
                dm_standard_version_config_newest = dm_standard_version_config_newest_tmp[0]
            else:
                return None, False
        else:
            dm_standard_version_config_newest = DmStandardVersionConfig.objects.filter(
                standard_id=dm_standard_config.id
            ).order_by("-created_at")[0]
        standard_version_status = dm_standard_version_config_newest.standard_version_status
        dm_standard_config_dict["standard_version_status"] = standard_version_status
        dm_standard_config_dict["standard_version_id"] = dm_standard_version_config_newest.id
        dm_standard_config_dict["standard_version"] = dm_standard_version_config_newest.standard_version
        # 获取detaildata
        standard_detaildata_content_list = (
            DmStandardContentConfig.objects.filter()
            .filter(standard_version_id=dm_standard_version_config_newest.id)
            .filter(standard_content_type="detaildata")
            .order_by("id")
        )
        for standard_detaildata_content in standard_detaildata_content_list:
            standard_detaildata_content_dict = {
                "id": standard_detaildata_content.id,
                "standard_content_name": standard_detaildata_content.standard_content_name,
                "description": standard_detaildata_content.description,
            }
            detaildata.append(standard_detaildata_content_dict)

        # 获取indicator
        standard_indicator_content_list = (
            DmStandardContentConfig.objects.filter()
            .filter(standard_version_id=dm_standard_version_config_newest.id)
            .filter(standard_content_type="indicator")
            .order_by("id")
        )
        for standard_indicator_content in standard_indicator_content_list:
            standard_indicator_content_dict = {
                "id": standard_indicator_content.id,
                "standard_content_name": standard_indicator_content.standard_content_name,
                "description": standard_indicator_content.description,
            }
            indicator.append(standard_indicator_content_dict)
        # dm_standard_config_dict = model_to_dict(dm_standard_config)
        dm_standard_config_dict["indicator"] = indicator
        dm_standard_config_dict["detaildata"] = detaildata
        dm_standard_config_dict["created_at"] = (
            dm_standard_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if dm_standard_config.created_at else None
        )
        dm_standard_config_dict["updated_at"] = (
            dm_standard_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if dm_standard_config.updated_at else None
        )
        return dm_standard_config_dict, True
    else:
        return None, False


class StandardPublicityView(APIViewSet):
    @list_route(methods=["post"], url_path="standards")
    @params_valid(serializer=StandardPublicityListSerializer)
    def standards(self, request, params):
        """数据公示列表"""
        keyword = params.get("keyword")
        keyword = keyword.strip()
        # 从standard_search接口中拿到所有online的
        search_params = {"is_online": 1}
        if keyword:
            search_params["content"] = keyword
        standard_online_key_word_list = DatamanageApi.standard_search(search_params).data
        if not standard_online_key_word_list:
            return Response({"standard_list": [], "standard_count": 0})

        standard_list = []

        tag_codes_list = params.get("tag_ids")
        # 是否按照关联数据量进行排序
        order_linked_data_count = params.get("order_linked_data_count")
        # 如果按标签进行了筛选
        if tag_codes_list:
            tag_codes_list = parse_tag_codes_list(tag_codes_list)

            if len(tag_codes_list) != 1:
                tag_codes_list = [str(each_tag) for each_tag in tag_codes_list]
                data_codes_tuple = tuple(tag_codes_list)
                sql = """select target_id from tag_target where target_type = 'standard'
                          and tag_code in {};""".format(
                    data_codes_tuple
                )
            else:
                sql = """select target_id from tag_target where target_type = 'standard'
                          and tag_code in ('{}');""".format(
                    tag_codes_list[0]
                )
            standard_list_tmp = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
            standard_id_list = [int(each["target_id"]) for each in standard_list_tmp]

            for each_standard in standard_online_key_word_list:
                if each_standard["id"] in standard_id_list:
                    standard_list.append(each_standard)
        else:
            standard_list = standard_online_key_word_list
        if not standard_list:
            return Response({"standard_list": [], "standard_count": 0})

        standard_count = len(standard_list)
        standard_dict = {"standard_count": standard_count}

        # 后台分页
        page_size = params.get("page_size", 10)
        offset = (params.get("page", 1) - 1) * page_size
        standard_dict["standard_list"] = standard_list
        standard_id_list = [each["id"] for each in standard_dict["standard_list"]]
        standard_version_id_list = [each["standard_version_id"] for each in standard_dict["standard_list"]]

        standard_count_dict = {}
        ind_count_dict = {}
        # 统计每个标准对应的数据个数
        if standard_dict["standard_list"]:
            if len(standard_version_id_list) != 1:
                standard_version_id_tuple = tuple(standard_version_id_list)
                sql = """select count(*) as count, sum(task_type='detaildata') as detail_count, standard_version_id
                          from dm_task_detail where standard_version_id in {}
                          and data_set_id in (select result_table_id from result_table where bk_biz_id < 200000)
                          group by standard_version_id;""".format(
                    standard_version_id_tuple
                )
                # 统计每一个指标标准对应的sql
                ind_sql = """select count(*) as count, standard_version_id, standard_content_id
                              from dm_task_detail where standard_version_id in {}
                              and data_set_id in (select result_table_id from result_table where bk_biz_id < 200000)
                              group by standard_version_id, standard_content_id;""".format(
                    standard_version_id_tuple
                )
            else:
                sql = """select count(*) as count, sum(task_type='detaildata') as detail_count, standard_version_id
                          from dm_task_detail where standard_version_id in ({}) and data_set_id in
                          (select result_table_id from result_table where bk_biz_id < 200000)
                          group by standard_version_id;""".format(
                    standard_version_id_list[0]
                )
                # 统计每一个指标标准对应的sql
                ind_sql = """select count(*) as count, standard_version_id, standard_content_id
                              from dm_task_detail where standard_version_id in ({}) and data_set_id in
                              (select result_table_id from result_table where bk_biz_id < 200000)
                              group by standard_version_id, standard_content_id;""".format(
                    standard_version_id_list[0]
                )

            standard_count_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
            ind_count_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], ind_sql)
            for each in standard_count_list:
                standard_count_dict[each["standard_version_id"]] = {
                    "count": each["count"],
                    "detail_count": each["detail_count"],
                }
            for each in ind_count_list:
                ind_count_dict["{}_{}".format(each["standard_version_id"], each["standard_content_id"])] = {
                    "count": each["count"]
                }

        # 查询标准对应的标签
        query_statement = """
            {
              standard_tag(func:eq(DmStandardConfig.id,$standard_id_list)){
                DmStandardConfig.id
                ~Tag.targets{
                  Tag.code,
                  Tag.alias
                }
              }
            }
        """
        query_statement = query_statement.replace("$standard_id_list", str(standard_id_list))

        # 用complex_search查询
        standard_tag_dgraph_list = meta_dgraph_complex_search(query_statement, return_original=True).get(
            "standard_tag", []
        )
        standard_tag_dgraph_dict = {}
        for each in standard_tag_dgraph_list:
            standard_tag_dgraph_dict[each["DmStandardConfig.id"]] = each.get("~Tag.targets", [])
        for each in standard_dict["standard_list"]:
            if not each.get("updated_by"):
                each["updated_by"] = each.get("created_by")
            tag_list = standard_tag_dgraph_dict.get(each["id"], [])
            each["tag_list"] = [
                {"code": each_tag.get("Tag.code"), "alias": each_tag.get("Tag.alias")} for each_tag in tag_list
            ]
            each["linked_data_count"] = standard_count_dict.get(each["standard_version_id"], {}).get("count", 0)
            for each_detail in each["detaildata"]:
                each_detail["linked_data_count"] = standard_count_dict.get(each["standard_version_id"], {}).get(
                    "detail_count", 0
                )
            for each_ind in each["indicator"]:
                key = "{}_{}".format(each["standard_version_id"], each_ind["id"])
                each_ind["linked_data_count"] = ind_count_dict.get(key, {}).get("count", 0)
        if order_linked_data_count == "asc":
            standard_dict["standard_list"].sort(key=lambda each_stan: (each_stan.get("linked_data_count", 0)))
        elif order_linked_data_count == "desc":
            standard_dict["standard_list"].sort(
                key=lambda each_stan: (each_stan.get("linked_data_count", 0)),
                reverse=True,
            )
        standard_dict["standard_list"] = standard_dict["standard_list"][offset : offset + page_size]
        return Response(standard_dict)

    @list_route(methods=["get"], url_path="get_standard_info")
    @params_valid(serializer=StandardPublicityInfoSerializer)
    def get_standard_info(self, request, params):
        # 获取标准对应的详情
        dm_standard_config_id = params.get("dm_standard_config_id")
        online = params.get("online")
        # 从standard_search接口中拿到所有online的
        search_params = {
            "online": online,
            "dm_standard_config_id": dm_standard_config_id,
        }
        standard_info_dict = DatamanageApi.get_standard(search_params).data
        if not standard_info_dict:
            return Response({})

        # 查询标准对应的标签
        query_statement = """
            {
              standard_tag(func:eq(DmStandardConfig.id,$standard_id)){
                DmStandardConfig.id
                ~Tag.targets{
                  Tag.code,
                  Tag.alias
                }
              }
            }
        """
        query_statement = query_statement.replace("$standard_id", str(dm_standard_config_id))
        # 用complex_search查询
        standard_tag_dgraph_list = meta_dgraph_complex_search(query_statement, return_original=True).get(
            "standard_tag", []
        )
        if standard_tag_dgraph_list:
            standard_info_dict["basic_info"]["tag_list"] = [
                {"code": each_tag.get("Tag.code"), "alias": each_tag.get("Tag.alias")}
                for each_tag in standard_tag_dgraph_list[0].get("~Tag.targets", [])
            ]

        # standard data count of this standard (标准和明细对应的标准数据count)
        sql = """select count(data_set_id) as count, sum(task_type='detaildata') as detail_count, standard_version_id
                  from dm_task_detail where standard_version_id = {}
                  and data_set_id in (select result_table_id from result_table where bk_biz_id < 200000);""".format(
            standard_info_dict["version_info"]["id"]
        )
        # 指标对应的标准数据count
        ind_sql = """select count(data_set_id) as count, sum(task_type='detaildata') as detail_count,
                  standard_version_id, standard_content_id from dm_task_detail where standard_version_id = {}
                  and data_set_id in (select result_table_id from result_table where bk_biz_id < 200000)
                  group by standard_content_id;""".format(
            standard_info_dict["version_info"]["id"]
        )
        standard_count_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        standard_info_dict["basic_info"]["linked_data_count"] = (
            standard_count_list[0].get("count", 0) if standard_count_list else 0
        )
        ind_count_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], ind_sql)
        ind_count_dict = {}
        for each in ind_count_list:
            ind_count_dict[each["standard_content_id"]] = each["count"]

        # detaildata_count & indicator_count
        detaildata_count = 0
        indicator_count = 0
        for each in standard_info_dict.get("standard_content", []):
            if each["standard_info"]["standard_content_type"] == "detaildata":
                detaildata_count += 1
                each["standard_info"]["linked_data_count"] = standard_count_list[0].get("detail_count", 0)
            elif each["standard_info"]["standard_content_type"] == "indicator":
                indicator_count += 1
                each["standard_info"]["linked_data_count"] = ind_count_dict.get(each["standard_info"]["id"], 0)
        standard_info_dict["basic_info"]["detaildata_count"] = detaildata_count
        standard_info_dict["basic_info"]["indicator_count"] = indicator_count

        # get the unit of each field
        unit_dict = DatamanageApi.get_unit_list().data

        for each_standard in standard_info_dict["standard_content"]:
            if each_standard["standard_info"].get("window_period"):
                each_standard["standard_info"]["window_period"]["window_type_alias"] = window_type_dict.get(
                    each_standard["standard_info"]["window_period"]["window_type"]
                )
                if each_standard["standard_info"]["window_period"].get("count_freq"):
                    each_standard["standard_info"]["window_period"]["count_freq_alias"] = count_freq_dict.get(
                        each_standard["standard_info"]["window_period"].get("count_freq", -1)
                    )
                if each_standard["standard_info"]["window_period"].get("window_time"):
                    each_standard["standard_info"]["window_period"]["window_time_alias"] = window_time_dict.get(
                        each_standard["standard_info"]["window_period"].get("window_time", -1)
                    )
                if (
                    each_standard["standard_info"]["window_period"].get("waiting_time")
                    or each_standard["standard_info"]["window_period"].get("waiting_time") == 0
                ):
                    each_standard["standard_info"]["window_period"]["waiting_time_alias"] = waiting_time_dict.get(
                        each_standard["standard_info"]["window_period"].get("waiting_time", -1)
                    )

            for each_field in each_standard["standard_fields"]:
                if each_field["unit"]:
                    each_field["unit_alias"] = unit_dict.get(each_field["unit"])
                else:
                    each_field["unit_alias"] = None

                if each_field.get("constraint", {}):
                    for each_rule in each_field["constraint"].get("rule", []):
                        each_rule["type_alias"] = constraint_dict.get(each_rule.get("type", ""))

        # if updated_by == null, then updated_by = created_by
        if not standard_info_dict["basic_info"]["updated_by"]:
            standard_info_dict["basic_info"]["updated_by"] = standard_info_dict["basic_info"]["created_by"]

        return Response(standard_info_dict)

    @list_route(methods=["get"], url_path="tag_dict")
    def tag_dict(self, request):
        """
        @api {get} /datamanage/dstan/standard_publicity/tag_dict/ 获取所有tag基本信息
        @apiVersion 0.1.0
        @apiGroup StandardPublicityView
        @apiName tag_dict
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {},
                "result": true
            }
        """
        # 用于历史搜索时给添加对应的tag_alias等内容
        sql = """select code, alias, description from tag"""
        tag_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        tag_dict = {}
        for each_tag in tag_list:
            tag_dict[each_tag["code"]] = each_tag["alias"]
        return Response(tag_dict)

    @list_route(methods=["get"], url_path="standards/simple")
    def standards_simple(self, request):
        """
        @api {get} /datamanage/dstan/standard_publicity/standards/simple/ 数据标准级联选框接口
        @apiVersion 0.1.0
        @apiGroup StandardPublicityView
        @apiName standards_simple
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    'standard_list': [
                        {
                            "children":[
                                {
                                    "id":1170,
                                    "name":"明细"
                                },
                                {
                                    "id":1171,
                                    "name":"原子"
                                }
                            ],
                            "id":234,
                            "name":"标准"
                        }
                },
                "result": true
            }
        """
        # 从standard_search接口中拿到所有online的
        search_params = {"is_online": 1}
        standard_online_list = DatamanageApi.standard_search(search_params).data
        standard_list = []
        if not standard_online_list:
            return Response({"standard_list": standard_list})
        for each_standard in standard_online_list:
            each_standard["detaildata"].extend(each_standard["indicator"])
            children = [
                {"id": each_detail["id"], "name": each_detail["standard_content_name"]}
                for each_detail in each_standard["detaildata"]
            ]
            standard_list.append(
                {
                    "id": each_standard["standard_version_id"],
                    "name": each_standard["standard_name"],
                    "children": children,
                }
            )
        return Response({"standard_list": standard_list})

    @list_route(methods=["get"], url_path="is_standard")
    def is_standard(self, request):
        dataset_id = request.query_params.get("dataset_id", None)
        if not dataset_id:
            return Response(
                {
                    "is_standard": False,
                }
            )
        sql = """select a.data_set_id data_set_id, b.standard_id standard_id from dm_task_detail a,
                  dm_standard_version_config b where data_set_id = '{}' and a.standard_version_id=b.id
                  and b.standard_version_status = 'online';""".format(
            dataset_id
        )
        stan_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        if not stan_list:
            return Response(
                {
                    "is_standard": False,
                }
            )
        standard_id = stan_list[0]["standard_id"]
        return Response({"is_standard": True, "standard_id": standard_id})
