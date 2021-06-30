# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import datetime
import json

from django import forms
from django.conf import settings
from django.utils import translation
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from apps import exceptions
from apps import forms as data_forms
from apps.api import AccessApi, DataManageApi, MetaApi, StorekitApi
from apps.common.log import logger
from apps.common.views import list_route
from apps.dataflow.datamart.datadict import (
    add_data_manager_and_viewer,
    add_dataset_list_ticket_status,
    add_meta_status,
    add_result_table_attr,
    add_standard_info,
    clean_project_id,
    clean_project_name_dev,
    clean_project_name_product,
    get_dataset_list_info,
    get_format_tag_list,
    judge_permission,
    list_dataset_in_apply,
    meta_status_dict,
    utc_to_local,
)
from apps.dataflow.datamart.datadict_history_search import history_search_attr
from apps.dataflow.datamart.lineage import (
    add_dp_attr,
    add_rt_attr,
    async_add_rawdata_attr,
    filter_dp,
)
from apps.dataflow.handlers.result_table import ResultTable
from apps.dataflow.models import DataMartSearchRecord
from apps.dataflow.utils import thread_pool_func
from apps.dataflow.views.data_map_views import datamap_cache
from apps.generic import APIViewSet

cluster_dict = {"tl": _("同乐"), "cft": _("财付通")}


class ResultTabelInfoViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="dataset_info")
    def dataset_info(self, request):
        """
        @api {get} /datamart/datadict/dataset_info/ 获取一个rt表对应的info
        @apiName dataset_info
        @apiParam {String} result_table_id rt_id
        @apiParam {Int} data_id data_id
        @apiParam {String} data_set_type 数据集类型 raw_data/result_table
        @apiSuccessExample {json} 成功返回
        {
            "message":"",
            "code":"00",
            "data":{
                "result_table_name_alias":"创建表测试",
                "sensitivity":"public",
                "updated_at":"2018-12-04 22:00:39",
                "generate_type":"user",
                "processing_type":"clean",
                "project_created_by":"xx",
                "updated_by":null,
                "storage_types":{
                    "tsdb":"7"
                },
                "can_search":true,
                "bk_biz_name":"灯塔业务",
                "created_by":"admin",
                "count_freq_unit":"s",
                "platform":"bkdata",
                "project_id":4,
                "result_table_id":"100120_system_disk",
                "project_name":"测试项目",
                "count_freq":1,
                "description":"tsdb测试",
                "tags":{
                    "manage":{
                        "geog_area":[
                            {
                                "alias":"中国内地",
                                "code":"inland"
                            }
                        ]
                    }
                },
                "expires":"7",
                "has_permission":true,
                "ticket_status":"",
                "bk_biz_id":100120,
                "data_category_alias":null,
                "created_at":"2018-11-20 22:02:10",
                "result_table_type":"1",
                "result_table_name":"system_disk",
                "data_category":null,
                "is_managed":1,
                "tag_list":[
                    {
                        "tag_alias":"磁盘",
                        "tag_code":"disk",
                        "tag_type":"business"
                    },
                    {
                        "tag_alias":"中国内地",
                        "tag_code":"inland",
                        "tag_type":"manage"
                    }
                ]
            },
            "result":true
        }
        """

        class ResultTabelInfoForm(data_forms.BaseForm):
            result_table_id = forms.CharField(label="结果表ID", required=False)
            tdw_table_id = forms.CharField(label="TDW表ID", required=False)
            data_id = forms.IntegerField(label="数据源ID", required=False)
            data_set_type = forms.CharField(label="数据集类型", required=True)
            standard_id = forms.IntegerField(label="标准id", required=False)

        _params = self.valid(ResultTabelInfoForm)
        data_set_type = _params.get("data_set_type")
        standard_id = _params.get("standard_id", None)
        username = request.user.username
        # 获取当前用户正在申请的权限列表
        tickets = list_dataset_in_apply(username)
        if data_set_type == "result_table":
            result_table_id = _params.get("result_table_id")
            o_rt = ResultTable(result_table_id=result_table_id)
            res = o_rt.data
            if not res:
                res = {"meta_status": "error", "meta_status_message": meta_status_dict["error"]}
                return Response(res)
            res["meta_status"] = "success"
            res["meta_status_message"] = meta_status_dict["success"]
            # 结果表 添加 项目管理员
            manager, viewer = o_rt.data_manager_and_viewer()
            res["manager"] = manager
            res["viewer"] = viewer
            # 给标准结果表添加标准相关属性
            if not standard_id:
                stan_dict = DataManageApi.is_standard({"dataset_id": result_table_id})
                if stan_dict and stan_dict.get("is_standard"):
                    standard_id = stan_dict.get("standard_id")
            if standard_id:
                add_standard_info(res, standard_id, result_table_id)

            # 结果表 添加 业务名称 & 分类名称 & 标签 & 权限
            add_result_table_attr(res, o_rt, request)
            res["storage_types"] = o_rt.expires()
            # 查询rt是否有可以查询的存储
            res["can_search"] = o_rt.can_search()
            # 权限正在申请中：如果结果表在当前用户正在申请的权限列表中，给结果表添加ticket_status=processing的属性
            res["ticket_status"] = "processing" if result_table_id in tickets else ""
            # 热度广度在详情页面左侧弃用
            # get_heat_and_range_score(result_table_id, res)
        elif data_set_type == "raw_data":
            data_id = _params.get("data_id")
            try:
                res = AccessApi.rawdata.retrieve({"raw_data_id": data_id, "show_display": 1})
                res["meta_status"] = "success"
                res["meta_status_message"] = meta_status_dict["success"]
            except Exception as e:
                logger.warning("get access detail info error: [%s]" % e.message)
                res = {"meta_status": "error", "meta_status_message": meta_status_dict["error"]}
            add_data_manager_and_viewer(res, data_id, "raw_data")
            # 查询raw_data对应的标签
            res["tag_list"] = get_format_tag_list(res.get("tags", {}))

            # raw_data 添加 项目名称
            if settings.RUN_MODE == "PRODUCT":
                res["project_name"] = clean_project_name_product
            else:
                res["project_name"] = clean_project_name_dev
            res["project_id"] = clean_project_id

            # 过期时间，raw_data过期时间无法从接口获得，先设置默认过期时间
            res["expires"] = "3"
            # 数据源读权限
            judge_permission(res, data_id, "raw_data", username)
            # 数据源写权限
            judge_permission(res, data_id, "raw_data", username, "update")
            # 权限正在申请中：如果raw_data在当前用户正在申请的权限列表中，给raw_data添加ticket_status=processing的属性
            res["ticket_status"] = "processing" if data_id in tickets else ""
            # 热度广度在详情页面左侧弃用
            # get_heat_and_range_score(data_id, res)
        else:
            tdw_table_id = _params.get("tdw_table_id")
            res = MetaApi.tdw_tables({"table_id": tdw_table_id, "related": "cols_info", "associated_with_rt": False})
            if res:
                res = res[0]
            else:
                return Response({})
            res["sensitivity"] = "private"
            res["bk_biz_name"] = _("腾讯数据仓库TDW")
            res["platform"] = "TDW"
            res["processing_type"] = "TDW"
            res["processing_type_alias"] = "TDW"
            res["description"] = res.get("table_comment", "")
            res["updated_at"] = utc_to_local(res.get("updated_at", ""))
            res["synced_at"] = utc_to_local(res.get("synced_at", ""))
            res["created_at"] = utc_to_local(res.get("created_at", ""))
            res["cluster_alias"] = (
                cluster_dict.get(res.get("cluster_id"))
                if cluster_dict.get(res.get("cluster_id"))
                else res.get("cluster_id")
            )
            fields = [
                {
                    "description": each_col.get("col_comment"),
                    "col_index": each_col.get("col_index"),
                    "field_alias": each_col.get("col_comment"),
                    "field_name": each_col.get("col_name"),
                    "field_type": each_col.get("col_type"),
                }
                for each_col in res.get("cols_info")
            ]
            res["fields"] = fields
            # tdw读权限
            judge_permission(res, tdw_table_id, "tdw_table", username, "select")
            # 元数据状态
            add_meta_status(res)
            # tdw写权限
            judge_permission(res, tdw_table_id, "tdw_table", username, "alter")
            # if res.get('has_permission', False) and res.get('meta_status', '') == 'success':
            #     judge_permission(res, tdw_table_id, 'tdw_table', username, 'alter')
            # else:
            #     res['has_write_permission'] = False
        return Response(res)


class ResultTabelFieldViewSet(APIViewSet):
    def create(self, request):
        request_params = json.loads(request.body)
        username = request.user.username
        result_table_id = request_params.get("result_table_id")
        # 判断有无编辑权限
        o_rt = ResultTable(result_table_id=result_table_id)
        has_write_permission, no_write_pers_reason = o_rt.judge_permission(username, "update")
        if not has_write_permission:
            raise exceptions.PermissionError()

        field_dict = request_params.get("field")
        o_rt = ResultTable(result_table_id=result_table_id)
        field_list = o_rt.data.get("fields", [])
        for each_field in field_list:
            if field_dict.get("field_name", "") == each_field.get("field_name"):
                each_field["field_alias"] = field_dict.get("field_alias", "")
                each_field["description"] = field_dict.get("description", "")
                break
        edit_res_dict = MetaApi.rt_field_edit(
            {"fields": field_list, "bk_username": username, "result_table_id": result_table_id}, raw=True
        )
        data = "fail"
        if edit_res_dict.get("result") and edit_res_dict.get("data"):
            data = "ok"
        return Response(data)


class DataSetListViewSet(APIViewSet):
    @datamap_cache
    def _data_set_list(self, request):
        request_params = json.loads(request.body)
        username = request.user.username
        # 按照"我的"进行过滤
        if request_params.get("is_filterd_by_created_by", False):
            request_params["created_by"] = username

        erp_dict = {
            "ResultTable": {
                "expression": {
                    "result_table_id": True,
                    "result_table_name": True,
                    "processing_type": True,
                    "result_table_name_alias": True,
                    "description": True,
                    "created_by": True,
                    "updated_at": True,
                    "sensitivity": True,
                    "bk_biz_id": True,
                    "project_id": True,
                    "project": {"project_name": True},
                    "~Tag.targets": {"code": True, "alias": True, "tag_type": True},
                    "processing_type_obj": {"processing_type_alias": True},
                    "~DmTaskDetail.data_set": {"standard_version": {"standard_id": True}},
                    "~LifeCycle.target": {
                        "assetvalue_to_cost": True,
                        "range": {
                            "node_count": True,
                            "project_count": True,
                            "biz_count": True,
                            "normalized_range_score": True,
                            "app_code_count": True,
                        },
                        "heat": {"heat_score": True, "query_count": True},
                        "importance": {
                            "importance_score": True,
                        },
                        "cost": {"capacity_score": True, "capacity": {"total_capacity": True}},
                        "asset_value": {"asset_value_score": True},
                    },
                }
            },
            "AccessRawData": {
                "expression": {
                    "bk_biz_id": True,
                    "description": True,
                    "created_by": True,
                    "updated_at": True,
                    "sensitivity": True,
                    "raw_data_alias": True,
                    "raw_data_name": True,
                    "~Tag.targets": {"code": True, "alias": True, "tag_type": True},
                    "~LifeCycle.target": {
                        "assetvalue_to_cost": True,
                        "range": {
                            "node_count": True,
                            "project_count": True,
                            "biz_count": True,
                            "normalized_range_score": True,
                            "app_code_count": True,
                        },
                        "heat": {"heat_score": True, "query_count": True},
                        "importance": {
                            "importance_score": True,
                        },
                        "cost": {"capacity_score": True, "capacity": {"total_capacity": True}},
                        "asset_value": {"asset_value_score": True},
                    },
                }
            },
            "TdwTable": {
                "expression": {
                    "table_id": True,
                    "table_name": True,
                    "table_comment": True,
                    "bk_biz_id": True,
                    "updated_at": True,
                    "created_by": True,
                }
            },
        }
        request_params["extra_retrieve"] = erp_dict

        # 1 数据集列表
        search_res = DataManageApi.get_data_dict_list(request_params)
        dataset_list = search_res

        # 2 给数据集列表属性
        raw_data_list = []
        rt_list = []
        tdw_list = []

        for i, each_dataset in enumerate(dataset_list):
            each_dataset["index"] = i
            if each_dataset.get("data_set_type", "") == "raw_data":
                raw_data_list.append(each_dataset)
            elif each_dataset.get("data_set_type", "") == "result_table":
                # 判断是数据平台的结果表还是tdw的
                rt_list.append(each_dataset)
            elif each_dataset.get("data_set_type", "") == "tdw_table":
                tdw_list.append(each_dataset)

        # 3 给数据集列表添加权限等属性
        raw_data_list = get_dataset_list_info("raw_data", request, raw_data_list)
        result_table_list = get_dataset_list_info("result_table", request, rt_list)
        tdw_rt_list = get_dataset_list_info("tdw", request, tdw_list)
        result_table_list.extend(tdw_rt_list)

        # 4 拼接raw_data_list & result_table_list，并按照index排序
        raw_data_list.extend(result_table_list)
        # 判断数据集列表是否有权限正在申请中
        add_dataset_list_ticket_status(username, raw_data_list)
        # 根据index排序
        raw_data_list.sort(key=lambda x: x["index"])
        return {"results": raw_data_list}

    def create(self, request):
        """
        @api {post} /datamart/datadict/dataset_list/ 获取dataset列表
        @apiName dataset_list
        @apiGroup ResultTabelInfoViewSet
        @apiParam {Int} bk_biz_id 业务id
        @apiParam {Int} project_id 项目id
        @apiParam {String} tag_code 分类
        @apiParam {String} tag_ids 选中的标签
        @apiParam {String} me_type 是否是从标准节点跳转
        @apiParam {Int} page 分页码数
        @apiParam {Int} page_size 分页大小
        @apiParam {String} keyword 查询关键字 "tag"/"standard"
        @apiParam {List} cal_type 是否显示标准数据&是否仅显示标准数据 []/["only_standard"]
        @apiParam {String} data_set_type 按照数据集类型过滤 "all"/"raw_data"/"result_table"
        @apiParam {Int} has_standard 下面有标准的节点跳转 1/0
        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "cal_type":[
                "standard"
            ],
            "has_standard":1,
            "keyword":"",
            "tag_ids":[

            ],
            "tag_code":"virtual_data_mart",
            "page_size":10,
            "me_type":"tag",
            "data_set_type":"all",
            "project_id":null,
            "page":1
        }
        @apiSuccessExample {json} 成功返回
        {
            "message": "",
            "code": "00",
            "data": {
                "count": 4507,
                "project_count": 128,
                "standard_dataset_count": 35,
                "bk_biz_count": 99,
                "data_source_count": 1159,
                "results": [
                    {
                        "result_table_name_alias": "创建表测试",
                        "sensitivity": "public",
                        "updated_at": "2018-12-04 22:00:39",
                        "generate_type": "user",
                        "processing_type": "clean",
                        "bk_biz_name": "灯塔业务",
                        "description": "tsdb测试",
                        "index": 0,
                        "created_by": "test",
                        "count_freq_unit": "s",
                        "platform": "bkdata",
                        "data_set_id": "100120_system_disk",
                        "project_id": 4,
                        "result_table_id": "100120_system_disk",
                        "project_name": "测试项目",
                        "count_freq": 1,
                        "updated_by": null,
                        "tags": {
                            "manage": {
                                "geog_area": [
                                    {
                                        "alias": "中国内地",
                                        "code": "inland"
                                    }
                                ]
                            }
                        },
                        "has_permission": true,
                        "is_standard": 0,
                        "ticket_status": "",
                        "bk_biz_id": 100120,
                        "data_category_alias": null,
                        "created_at": "2018-11-20 22:02:10",
                        "result_table_type": "1",
                        "result_table_name": "system_disk",
                        "data_category": null,
                        "is_managed": 1,
                        "data_set_type": "result_table",
                        "tag_list": [
                            {
                                "tag_alias": "磁盘",
                                "tag_code": "disk",
                                "tag_type": "business"
                            },
                            {
                                "tag_alias": "中国内地",
                                "tag_code": "inland",
                                "tag_type": "manage"
                            }
                        ]
                    }
                ],
                "dataset_count": 3348
            },
            "result": true
        }
        """
        return Response(self._data_set_list(request))

    @list_route(methods=["get"], url_path="storage_type")
    def storage_type(self, request):
        storage_type_dict = StorekitApi.common.list().get("formal_name", {})
        # storage_type_dict.pop('ignite')
        # storage_type_dict.pop('mysql')
        storage_type_list = []
        for key, value in storage_type_dict.items():
            storage_type_list.append({"name": key, "alias": value})
        return Response(storage_type_list)

    @list_route(methods=["post"], url_path="set_search_history")
    def set_search_history(self, request):
        """
        @api {post} /datamart/datadict/dataset_list/set_search_history/数据字典历史搜索
        """

        class DataMartSearchHistoryForm(data_forms.BaseForm):
            tag_ids = data_forms.ListField(label="标签/标准版本id", required=False)
            cal_type = data_forms.ListField(label="是否标准化", required=False)
            me_type = forms.CharField(label="数据分类/标准分类", required=True)
            tag_code = forms.CharField(label="数据地图分类", required=True)
            created_by = forms.CharField(label="创建者", required=False)
            storage_type = forms.CharField(label="存储类型", required=False)
            created_at_start = forms.DateTimeField(label="起始创建时间", required=False)
            created_at_end = forms.DateTimeField(label="终止创建时间", required=False)
            heat_operate = forms.CharField(label="热度比较运算符", required=False)
            heat_score = forms.FloatField(label="热度评分", required=False)
            range_operate = forms.CharField(label="广度比较运算符", required=False)
            range_score = forms.FloatField(label="广度评分", required=False)
            importance_operate = forms.CharField(label="重要度比较运算符", required=False)
            importance_score = forms.FloatField(label="重要度评分", required=False)
            asset_value_operate = forms.CharField(label="价值比较运算符", required=False)
            asset_value_score = forms.FloatField(label="价值评分", required=False)
            assetvalue_to_cost_operate = forms.CharField(label="收益比比较运算符", required=False)
            assetvalue_to_cost = forms.FloatField(label="收益比", required=False)
            storage_capacity_operate = forms.CharField(label="存储成本比较运算符", required=False)
            storage_capacity = forms.FloatField(label="存储成本", required=False)
            bk_biz_id = forms.IntegerField(label="业务id", required=False)
            project_id = forms.IntegerField(label="项目id", required=False)
            data_set_type = forms.CharField(label="数据集类型", required=True)
            platform = forms.CharField(label="数据所在系统", required=True)
            keyword = forms.CharField(label="搜索关键字", required=False, max_length=128)
            order_time = forms.CharField(label="按时间排序", required=False)
            order_heat = forms.CharField(label="按热度评分排序", required=False)
            order_range = forms.CharField(label="按广度评分排序", required=False)
            order_asset_value = forms.CharField(label="按价值评分排序", required=False)
            order_importance = forms.CharField(label="按重要度评分排序", required=False)
            order_storage_capacity = forms.CharField(label="按存储大小排序", required=False)
            order_assetvalue_to_cost = forms.CharField(label="按收益比排序", required=False)
            standard_content_id = forms.IntegerField(label="标准内容id", required=False)
            is_standard = forms.BooleanField(label="是否标准化数据", required=False)

        # 默认参数，用于进行参数校验，如果输入和默认参数一致，则不需要把记录存入表中
        format_param = {
            "bk_biz_id": None,
            "project_id": None,
            "tag_ids": [],
            "keyword": "",
            "tag_code": "virtual_data_mart",
            "me_type": "tag",
            "has_standard": 1,
            "cal_type": ["standard"],
            "data_set_type": "all",
            "platform": "bk_data",
            "order_time": "",
            "order_heat": "",
            "order_range": "",
            "order_asset_value": "",
            "order_importance": "",
            "order_storage_capacity": "",
            "order_assetvalue_to_cost": "",
            "created_at_start": None,
            "created_at_end": None,
            "storage_type": "",
            "range_operate": "",
            "range_score": None,
            "heat_operate": "",
            "heat_score": None,
            "importance_operate": "",
            "importance_score": None,
            "asset_value_operate": "",
            "asset_value_score": None,
            "assetvalue_to_cost_operate": "",
            "assetvalue_to_cost": None,
            "storage_capacity_operate": "",
            "storage_capacity": None,
            "created_by": "",
            "is_standard": False,
            "standard_content_id": None,
        }

        _params = self.valid(DataMartSearchHistoryForm)
        if all((k in format_param and format_param[k] == v) for k, v in _params.items()):
            return Response("fail")

        _params["time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _params["operator"] = request.user.username
        DataMartSearchRecord.objects.create(**_params)
        return Response("success")

    @list_route(methods=["get"], url_path="get_search_history")
    def get_search_history(self, request):
        """
        @api {post} /datamart/datadict/dataset_list/get_search_history/数据字典历史搜索
        """
        # 获取个人在项目中最近十条选择结果表历史
        operator = request.user.username
        search_list = list(DataMartSearchRecord.objects.filter(operator=operator).order_by("-time")[:10].values())
        history_search_attr(search_list)
        return Response(search_list)


class LineageListViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="lineage")
    def lineage(self, request):
        """
        @api {get} /datamart/datadict/lineage/ 获取一个数据源/rt表对应的所有血缘信息，用于血缘分析展示
        @apiName lineage
        @apiGroup LineageListViewSet
        @apiParam {String} type 数据集类型
        @apiParam {String} qualified_name 数据集名称
        @apiParam {Int} is_filter_dp 是否过滤数据处理
        @apiSuccessExample {json} 成功返回
        {
            "message":"",
            "code":"00",
            "data":{
                "lines":[
                    {
                        "source":{
                            "id":"data_processing_591_f1281_split_01",
                            "arrow":"Left"
                        },
                        "target":{
                            "id":"result_table_105_f1281_split_01",
                            "arrow":"Right"
                        }
                    },
                    {
                        "source":{
                            "id":"result_table_591_xxxx1115",
                            "arrow":"Left"
                        },
                        "target":{
                            "id":"data_processing_591_f1281_split_01",
                            "arrow":"Right"
                        }
                    }
                ],
                "locations":[
                    {
                        "bk_biz_id":591,
                        "project_name":"测试项目",
                        "count_freq":0,
                        "description":"直接清洗",
                        "type":"result_table",
                        "sensitivity":"public",
                        "updated_at":"2019-09-04 16:54:28",
                        "updated_by":"admin",
                        "count_freq_unit":"s",
                        "alias":"直接清洗",
                        "project_id":4,
                        "bk_biz_name":"蓝鲸数据平台对内版",
                        "id":"result_table_591_xxxx1115",
                        "name":"591_xxxx1115"
                    },
                    {
                        "bk_biz_id":105,
                        "direction":"current",
                        "project_name":"xxx_proj",
                        "count_freq":0,
                        "name":"105_f1281_split_01",
                        "type":"result_table",
                        "sensitivity":"public",
                        "updated_at":"2019-05-17 19:39:04",
                        "updated_by":"",
                        "count_freq_unit":"s",
                        "alias":"f1281_split_01",
                        "project_id":249,
                        "bk_biz_name":"DNF地下城与勇士",
                        "id":"result_table_105_f1281_split_01",
                        "description":"f1281_split_01"
                    },
                    {
                        "inputs":[
                            {
                                "data_set_id":"591_xxxx1115",
                                "data_set_type":"result_table"
                            },
                            {
                                "data_set_id":"591_xxxx1115",
                                "data_set_type":"result_table"
                            }
                        ],
                        "project_id":249,
                        "updated_by":"admin",
                        "qualified_name":"591_f1281_split_01",
                        "processing_type_alias":"固化节点",
                        "updated_at":"2019-05-17 20:12:58",
                        "generate_type_alias":"用户",
                        "name":"591_f1281_split_01",
                        "processing_alias":"固化节点-split任务591_f1281_split_01",
                        "platform":"bkdata",
                        "project_name":"xxx_proj",
                        "generate_type":"user",
                        "outputs":[
                            {
                                "data_set_id":"105_f1281_split_01",
                                "data_set_type":"result_table"
                            },
                            {
                                "data_set_id":"1105_f1281_split_01",
                                "data_set_type":"result_table"
                            },
                            {
                                "data_set_id":"591_f1281_split_01",
                                "data_set_type":"result_table"
                            }
                        ],
                        "processing_type":"transform",
                        "type":"data_processing",
                        "id":"data_processing_591_f1281_split_01",
                        "processing_id":"591_f1281_split_01",
                        "description":"固化节点-split任务"
                    }
                ]
            },
            "result":true
        }
        """

        class LineageForm(data_forms.BaseForm):
            type = forms.CharField(label="数据集类型", required=True)
            qualified_name = forms.CharField(label="数据集名称", required=True)
            is_filter_dp = forms.IntegerField(label="是否过滤数据处理", required=False)

        _params = self.valid(LineageForm)
        dataset_type = _params.get("type")
        if dataset_type == "tdw_table":
            dataset_type = "result_table"
        qualified_name = _params.get("qualified_name")
        is_filter_dp = _params.get("is_filter_dp")
        # 1 血缘接口 & 交互式关联信息查询协议
        extra_rt_dict = {
            "ResultTable": {
                "bk_biz_id": True,
                "count_freq_unit": True,
                "result_table_name_alias": True,
                "project_id": True,
                "count_freq": True,
                "updated_by": True,
                "sensitivity": True,
                "updated_at": True,
                "processing_type": True,
                "result_table_id": True,
                "description": True,
                "project": {"project_name": True},
            },
            "DataProcessing": {
                "processing_type": True,
                "description": True,
                "processing_alias": True,
                "processing_id": True,
                "project": {"project_name": True, "project_id": True},
            },
        }
        # extra_rt_dict = {
        #     "ResultTable": {
        #         "*": True,
        #         "project": {"project_name": True}
        #     },
        #     "DataProcessing": {
        #         "*": True,
        #         "project": {"project_name": True, "project_id": True}
        #     }
        # }
        extra_param = json.dumps(extra_rt_dict)
        search_dict = MetaApi.lineage(
            {
                "type": dataset_type,
                "qualified_name": qualified_name,
                "depth": -1,
                "backend_type": "dgraph",
                "extra_retrieve": extra_param,
                "only_user_entity": True,
            }
        )

        # 2 过滤数据处理节点
        if is_filter_dp == 1:
            filter_dp(search_dict)

        # 3 血缘对应节点locations:
        locations = []
        rt_list = []
        dp_list = []
        rawdata_list = []

        for key, value in search_dict.get("nodes", {}).items():
            if key.startswith("result_table"):
                each_rt = add_rt_attr(value.get("extra"))
                rt_list.append(each_rt)
            elif key.startswith("raw_data"):
                rawdata_list.append(value)
            else:
                each_dp = add_dp_attr(value.get("extra"))
                dp_list.append(each_dp)

        locations.extend(rt_list)
        locations.extend(dp_list)

        # 并发查询数据源和数据处理节点tooltip信息
        thread_pool_func(async_add_rawdata_attr, request, translation.get_language(), rawdata_list)
        locations.extend(rawdata_list)

        # 当前节点的direction标记为current
        for each_dataset in locations:
            if each_dataset["type"] == dataset_type and each_dataset["name"] == qualified_name:
                each_dataset["direction"] = "current"
                break

        # 4 血缘对应边lines,因为测试环境边有重复录入的情况，所以去重
        lines = []
        for each_relation in search_dict.get("relations", []):
            each_line = {
                "source": {"arrow": "Left", "id": each_relation.get("from")},
                "target": {"arrow": "Right", "id": each_relation.get("to")},
            }
            if each_line not in lines:
                lines.append(each_line)
        result_data = {
            "locations": locations,
            "lines": lines,
        }
        return Response(result_data)
