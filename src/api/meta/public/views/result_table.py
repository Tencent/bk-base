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

import json
from collections import defaultdict

from common.auth import UserPerm
from common.auth.objects import is_sys_scopes
from common.decorators import params_valid
from common.local import get_request_username
from common.transaction import auto_meta_sync
from django.conf import settings
from django.utils.translation import ugettext_lazy as _
from rest_framework.decorators import action
from rest_framework.response import Response

from meta import exceptions as meta_errors
from meta.basic.common import RPCViewSet
from meta.public.common import translate_project_name
from meta.public.models.data_processing import DataProcessingRelation
from meta.public.models.result_table import ResultTable
from meta.public.models.result_table_action import ResultTableActions
from meta.public.serializers.common import (
    DestroySerializer,
    ResultTableOverrideSerializer,
)
from meta.public.serializers.result_table import (
    ResultTableSerializer,
    ResultTableUpdateSerializer,
)
from meta.utils.basicapi import parseresult
from meta.utils.common import paged_sql


class ALL(object):
    pass


class ResultTableViewSet(ResultTableActions, RPCViewSet):
    lookup_field = "result_table_id"

    @staticmethod
    def is_in_scope(item, scope):
        if len(list(scope.keys())) == 0:
            return False

        for key in scope:
            if not (key in item and item[key] == scope[key]):
                return False

        return True

    @action(detail=False, methods=["get"], url_path="mine")
    def mine(self, request):
        """
        @api {get} /meta/result_tables/mine/ 获取我有权限的结果表列表
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName list_result_table_mine
        @apiDescription  参数同 /meta/result_tables/ 接口

        @apiParam {String} action_id 操作方式，比如 result_table.query_data
        """
        # 根据需要进行的操作，返回相应的对象列表
        action_id = request.query_params.get("action_id", "result_table.query_data")
        # bk_biz_id = request.query_params.get("bk_biz_id", 0)
        page = request.query_params.get("page", None)
        pagination = True if page is not None else False

        scopes = UserPerm(get_request_username()).list_scopes(action_id)
        # 全局范围，若是，则返回全部对象
        if is_sys_scopes(scopes):
            response = self.list(request, False)
        # 否则加上权限，限制返回
        else:
            response = self.list(request, False)
            data = response.data

            scopes_dict = defaultdict(set)
            for scope in scopes:
                for key, value in list(scope.items()):
                    scopes_dict[key].add(value)

            cleaned_data = []
            items = list(scopes_dict.items())
            for d in data:
                for key, value_list in items:
                    if d.get(key) in value_list:
                        cleaned_data.append(d)
                        break
            response.data = cleaned_data
        if pagination:
            response.data = dict(data=response.data, has_next=False, acc_tdw=False)
        return response

    def list(self, request, pagination_enable=True):
        """
        @api {get} /meta/result_tables/ 获取结果表列表
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName list_result_table

        @apiParam {Boolean} pagination_enable 是否开启分页功能
        @apiParam {Number[]} bk_biz_id 业务ID列表
        @apiParam {Number} project_id 项目ID
        @apiParam {String} processing_type processing_type
        @apiParam {String[]} result_table_ids 结果表ID列表
        @apiParam {String[]} related 需要返回的关联信息,取值[data_processing,fields,storages]
        @apiParam {Number} [page] 页码
        @apiParam {Number} [page_size] 分页大小
        @apiParam {Object} related_filter 根据关联对象信息进行RT过滤。传入关联对象条件信息，详见例子。
        @apiParam {Number} [need_storage_detail] 是否需要返回storages详情,取值[0:不需要;默认需要]
        @apiParam {Boolean} [extra=False] 是否需要返回表特有的信息，如TDW属性。
        @apiParam {String[]} [tdw_related] 需要返回的tdw关联信息,取值[cols_info,parts_info]
        @apiParam {String[]} [tags] 标签code列表

        @apiParamExample {Object} related_filter-Example:
        { "type": "storages" ,
          "attr_name": "common_cluster.cluster_type",
          "attr_value": "es"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
        {
            "errors":{

            },
            "message":"ok",
            "code":"1500200",
            "result":true,
            "data":[
                {
                    "bk_biz_id":2,
                    "project_id":1,
                    "project_name":"测试项目",
                    "result_table_id":"2_output",
                    "result_table_name":"output",
                    "result_table_name_alias":"output_alias",
                    "result_table_type":null,
                    "processing_type":"clean",
                    "generate_type":"user",
                    "sensitivity":"output_alias",
                    "count_freq":60,
                    "created_by":"admin",
                    "created_at":"2018-09-19 17:03:50",
                    "updated_by":null,
                    "updated_at":"2018-09-19 17:03:50",
                    "description":"输出",
                    "concurrency":0,
                    "is_managed":1,
                    "platform":"tdw",
                    "processing_type":"batch",
                    "updated_at":"2019-05-09 17:21:56",
                    "created_by":"bingo",
                    "result_table_type":null,
                    "platform":"tdw",
                    "generate_type":"user",
                    "sensitivity":"public",
                    "is_managed":1,
                    "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
                    "project_id":1
                            }
                        }
                    },
                    "data_processing":{
                        "project_id":1,
                        "processing_id":"xxx",
                        "processing_alias":"xxx",
                        "processing_type":"clean",
                        "created_by ":"xxx",
                        "created_at":"xxx",
                        "updated_by":"xxx",
                        "updated_at":"xxx",
                        "description":"xxx"
                    },
                    "fields":[
                        {
                            "id":13730,
                            "field_index":1,
                            "field_name":"timestamp",
                            "field_alias":"时间",
                            "description":"",
                            "field_type":"timestamp",
                            "is_dimension":0,
                            "origins":"",
                            "created_by":"admin",
                            "created_at":null,
                            "updated_by":null,
                            "updated_at":null
                            "roles": {
                            "event_time": false
                          }
                        }
                    ],
                    "storages":{
                        "tspider":{
                            "id":1,
                            "storage_cluster":{
                                "storage_cluster_config_id":1,
                                "cluster_name":"xxx",
                                "cluster_type":"tspider",
                                "cluster_domain":"xxx",
                                "cluster_group":"xxx",
                                "connection_info":"{}",
                                "priority":234,
                                "version":"23423",
                                "belongs_to":"bkdata"
                            },
                            "storage_channel":{

                            },
                            "physical_table_name":"xxx",
                            "expires":"xxx",
                            "storage_config":"xxx",
                            "priority":1,
                            "generate_type":"user",
                            "description":"xxx",
                            "created_by":"admin",
                            "created_at":null,
                            "updated_by":null,
                            "updated_at":null
                        },
                        "kafka":{
                            "id":2,
                            "storage_cluster":{

                            },
                            "storage_channel":{
                                "channel_cluster_config_id":1,
                                "cluster_name":"xxx",
                                "cluster_type":"kafka",
                                "cluster_role":"inner",
                                "cluster_domain":"xxx",
                                "cluster_backup_ips":"xxx",
                                "cluster_port":2432,
                                "zk_domain":"127.0.0.1",
                                "zk_port":3481,
                                "zk_root_path":"/abc/defg",
                                "priority":234,
                                "attribute":"bkdata",
                                "description":"sdfdsf"
                            },
                            "physical_table_name":"xxx",
                            "expires":"xxx",
                            "storage_config":"xxx",
                            "priority":1,
                            "generate_type":"user",
                            "description":"xxx",
                            "created_by":"admin",
                            "created_at":null,
                            "updated_by":null,
                            "updated_at":null
                        }
                    }
                }
            ]
        }
        """
        bk_biz_ids = request.query_params.getlist("bk_biz_id")
        project_id = request.query_params.get("project_id")
        result_table_ids = request.query_params.getlist("result_table_ids")
        related = request.query_params.getlist("related")
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")
        related_filter = request.query_params.get("related_filter")
        processing_type = request.query_params.get("processing_type")
        need_storage_detail_param = request.query_params.get("need_storage_detail")
        get_extra = request.query_params.get("extra", False)
        tdw_related = request.query_params.getlist("tdw_related")
        tags = request.query_params.getlist("tags")
        is_query = request.query_params.get("is_query", 0)
        with_detail = request.query_params.get("with_detail", 0)

        # 如果不开启分页功能，page参数置空
        if not pagination_enable:
            page = None
            page_size = None

        # 是否需要storage详情
        need_storage_detail = True
        if need_storage_detail_param == "0":
            need_storage_detail = False

        # 过滤processing_type
        p_type_filter_name, p_type_filter_val = parseresult.get_filter_attrs(related_filter, "processing_type")
        # -- 展示不可见结果表
        # {"type":"processing_type", "attr_name": "show","attr_value": "queryset"}
        need_show_queryset = False
        if p_type_filter_name == "show" and p_type_filter_val == "queryset":
            need_show_queryset = True

        # 过滤storages
        storage_filter_name, storage_filter_val = parseresult.get_filter_attrs(related_filter, "storages")
        # -- 指定需要返回的公共cluster_type
        # {"type":"storages", "attr_name": "common_cluster.cluster_type","attr_value": ["tredis","ipredis"]}
        is_common_cluster = False
        if storage_filter_name is not None and storage_filter_name.startswith("common_cluster."):
            is_common_cluster = True
        # -- 指定只返回支持查询的存储对应的rt
        # {"type":"storages", "attr_name": "show","attr_value": "queryable"}
        only_show_queryable = False
        if storage_filter_name == "show" and storage_filter_val == "queryable":
            only_show_queryable = True
            if "storages" not in related:
                related.append("storages")

        # 兼容老参数，is_query场景
        if int(is_query) == 1:
            need_storage_detail = False
            only_show_queryable = True
            if "storages" not in related:
                related.append("storages")

        # 指定需要返回的公共cluster_type
        if is_common_cluster:
            # 找出符合条件的result_table_id
            # TODO:没用上索引。。数据量少应该还没事,后续最好处理一下
            if isinstance(storage_filter_val, list):
                filter_attr_exp = "b.cluster_type in ('{}')".format("', '".join(storage_filter_val))
            else:
                filter_attr_exp = "b.cluster_type = '{}'".format(storage_filter_val)
            rt_id_list_sql = (
                "select a.result_table_id from "
                "bkdata_basic.storage_result_table a,"
                "bkdata_basic.storage_cluster_config b "
                "where a.storage_cluster_config_id=b.id and {0} "
                "union "
                "select a.result_table_id from "
                "bkdata_basic.storage_result_table a, "
                "bkdata_basic.databus_channel_cluster_config b "
                "where a.storage_channel_id=b.id and {0}".format(filter_attr_exp)
            )

            rpc_response = self.entity_complex_search(paged_sql(rt_id_list_sql))
            result_table_id_list = rpc_response.result
            if result_table_id_list:
                if related is not None:
                    related.append("storages")
                else:
                    related = ["storages"]
                result_table_ids = []
                for rt_id_dict in result_table_id_list:
                    result_table_ids.append(rt_id_dict["result_table_id"])
            else:
                return Response([])

        # 不支持拉取全量字段
        if not (result_table_ids or bk_biz_ids or project_id or processing_type):
            if "fields" in related:
                raise meta_errors.NotSupportQuery(message_kv={"error_detail": _("不支持拉取全量字段")})

        query_result_all = parseresult.get_result_table_infos_v3(
            bk_biz_ids=bk_biz_ids,
            project_id=project_id,
            result_table_ids=result_table_ids,
            related=related,
            processing_type=processing_type,
            page=page,
            page_size=page_size,
            need_storage_detail=need_storage_detail,
            tags=tags,
            only_queryable=False if need_show_queryset and not related else True,
        )
        response = []
        for idx, item in enumerate(query_result_all):
            discard = False
            translate_project_name(item)
            if only_show_queryable and "storages" in item and isinstance(item["storages"], dict):
                item["query_storages_arr"] = [
                    n for n in list(item["storages"].keys()) if n in settings.ENABLE_QUERY_STORAGE_LIST
                ]
                if not item["query_storages_arr"]:
                    discard = True
            if item["processing_type"] == "queryset" and not need_show_queryset:
                discard = True
            if not discard:
                response.append(item)

        if settings.ENABLED_TDW and get_extra == "True":
            from meta.extend.tencent.tdw.views.utils_result_table_mixin import (
                mixin_tdw_extra_info,
            )

            mixin_tdw_extra_info(request, response, tdw_related)

        parseresult.add_manage_tag_to_result_table(response)
        if with_detail:
            response = dict(data=response)
        return Response(response)

    @params_valid(serializer=ResultTableSerializer)
    def create(self, request, params):
        """
        @api {post} /meta/result_tables/ 新建结果表
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName create_result_table

        @apiParam {String} bk_username 用户名
        @apiParam {Number} bk_biz_id 业务ID
        @apiParam {Number} project_id 项目ID
        @apiParam {String} result_table_id 结果表ID
        @apiParam {String} result_table_name 结果表名称
        @apiParam {String} result_table_name_alias 结果表中文名
        @apiParam {String} [result_table_type] 结果表类型
        @apiParam {Boolean} [is_managed=1] 是否被托管
        @apiParam {String='bkdata','tdw'} [platform='bkdata'] RT平台归属。
        @apiParam {String} processing_type 数据处理类型
        @apiParam {String="user", "system"} generate_type 生成类型
        @apiParam {String="public","private","sensitive"} sensitivity 敏感度
        @apiParam {Number} count_freq 统计频率
        @apiParam {string} description 结果表描述信息
        @apiParam {Object[]} fields 结果表字段信息
        @apiParam {Object[]} extra 其他平台额外信息更新。
        @apiParam {String[]} tags 标签code列表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "bk_biz_id": 123,
                "project_id": 123,
                "result_table_id": "xxx",
                "result_table_name": "xxx",
                "result_table_name_alias ": "xxx",
                "result_table_type": null,
                "processing_type": "clean",
                "generate_type": "user",
                "sensitivity": "public",
                "count_freq": 60,
                "description": "xxx",
                "is_managed": 1,
                "platform": "tdw",
                "tags": ["NA"],
                "fields": [
                    {
                        "field_index": 1,
                        "field_name": "timestamp",
                        "field_alias": "时间",
                        "description": "",
                        "field_type": "timestamp",
                        "is_dimension": 0,
                        "origins": "",
                        "roles": {
                            "event_time": true
                          }
                    }
                ]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "591_test_table",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1500001 参数校验失败
        @apiError 1521021 结果表字段冲突
        """
        extra = params.get("extra", {})
        platform = params["platform"]
        if platform == "tdw" and not extra.get("tdw", None):
            raise meta_errors.ParamsError(message_kv={"error_detail": _("Tdw RT表必填Tdw信息。")})

        with auto_meta_sync(using="bkdata_basic"):
            result_table = self.run_create(request, params)

        return Response(result_table.result_table_id)

    def retrieve(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/ 获取单个结果表实例的信息
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName retrieve_result_table

        @apiParam {String[]} [related] 需要返回的关联信息,取值[data_processing,fields,storages]
        @apiParam {Boolean} [extra=False] 是否需要返回表特有的信息
        @apiParam {String[]} [tdw_related] 需要返回的tdw关联信息,取值[cols_info,parts_info]
        @apiParam {Object} [erp] 使用erp协议进行查询
        @apiParam {String='classic'} [result_format] 配合erp协议使用，可返回不同的格式

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
              "bk_biz_id": 2,
              "project_id": 2331,
              "project_name": "测试项目",
              "result_table_id": "2_output",
              "result_table_name": "output",
              "result_table_name_alias": "output_alias",
              "result_table_type": null,
              "processing_type": "clean",
              "generate_type": "user",
              "sensitivity": "output_alias",
              "count_freq": 60,
              "created_by": "admin",
              "created_at": "2018-09-19 17:03:50",
              "updated_by": null,
              "updated_at": "2018-09-19 17:03:50",
              "description": "输出",
              "concurrency": 0,
              "is_managed": 1,
              "platform": "tdw",
              "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}},
              "data_processing": {
                "project_id": 2331,
                "processing_id": "2_output",
                "processing_alias": "xxx",
                "processing_type": "clean",
                "created_by ": "xxx",
                "created_at": "xxx",
                "updated_by": "xxx",
                "updated_at": "xxx",
                "description": "xxx"
              },
              "fields": [
                {
                  "id": 13730,
                  "field_index": 1,
                  "field_name": "timestamp",
                  "field_alias": "时间",
                  "description": "",
                  "field_type": "timestamp",
                  "is_dimension": 0,
                  "origins": "",
                  "created_by": "admin",
                  "created_at": null,
                  "updated_by": null,
                  "updated_at": null
                }
              ],
              "storages": {
                "tspider": {
                  "id": 2,
                  "storage_cluster": {
                    "storage_cluster_config_id": 1,
                    "cluster_name": "xxx",
                    "cluster_type": "tspider",
                    "cluster_domain": "xxx",
                    "cluster_group": "xxx",
                    "connection_info": "{}",
                    "priority": 234,
                    "version": "23423",
                    "belongs_to": "bkdata"
                  },
                  "physical_table_name": "xxx",
                  "expires": "xxx",
                  "storage_config": "xxx",
                  "priority": 1,
                  "generate_type": "user",
                  "description": "xxx",
                  "created_by": "admin",
                  "created_at": null,
                  "updated_by": null,
                  "updated_at": null
                },
                "kafka": {
                  "id": 1,
                  "storage_cluster": {},
                  "storage_channel": {
                    "channel_cluster_config_id": 1,
                    "cluster_name": "xxx",
                    "cluster_type": "kafka",
                    "cluster_role": "inner",
                    "cluster_domain": "xxx",
                    "cluster_backup_ips": "xxx",
                    "cluster_port": 2432,
                    "zk_domain": "127.0.0.1",
                    "zk_port": 3481,
                    "zk_root_path": "/abc/defg",
                    "priority": 234,
                    "attribute": "bkdata",
                    "description": "sdfdsf"
                  },
                  "physical_table_name": "xxx",
                  "expires": "xxx",
                  "storage_config": "xxx",
                  "priority": 1,
                  "generate_type": "user",
                  "description": "xxx",
                  "created_by": "admin",
                  "created_at": null,
                  "updated_by": null,
                  "updated_at": null
                }
              }
            }

        @apiError 1521020 结果表不存在
        """
        erp = request.query_params.get("erp", None)
        result_format = request.query_params.get("result_format", False)
        p_related = request.query_params.getlist("related")
        get_extra = request.query_params.get("extra", False)
        tdw_related = request.query_params.getlist("tdw_related")
        check_usability = request.query_params.get("check_usability", False)
        tdw_std_access = True if result_table_id.startswith("{}_".format(settings.TDW_STD_BIZ_ID)) else False
        related = []
        if p_related:
            related.extend(p_related)
        if not result_table_id:
            return Response({})

        result_table_ids = [result_table_id]
        # 优先用erp语句查询
        if erp:
            erp_expression = json.loads(erp)
            if erp_expression:
                if tdw_std_access:
                    erp_expression["result_table_name"] = "true"
                    erp_expression["bk_biz_id"] = "true"
                backend_type = "dgraph_cold" if tdw_std_access else "dgraph"
                erp_args = {"ResultTable": {"expression": erp_expression, "starts": result_table_ids}}
                rpc_response = self.entity_query_via_erp(erp_args, backend_type=backend_type)
                result = rpc_response.result
                if result["ResultTable"]:
                    return_result = result["ResultTable"][0]
                    if result_format == "classic":
                        self.format_classic(return_result)
                    return Response(return_result)
                else:
                    return Response({})

        query_result = parseresult.get_result_table_infos_v3(
            result_table_ids=result_table_ids, related=related, only_queryable=False
        )
        return_result = query_result[0] if query_result else {}
        if not return_result:
            return Response({})

        if settings.ENABLED_TDW and get_extra in ("True", "true"):
            return_result["extra"] = {}
            from meta.extend.tencent.tdw.views.utils_result_table_mixin import (
                check_and_mixin_extra_info,
            )

            check_and_mixin_extra_info(return_result, tdw_related, check_usability, request)

        parseresult.add_manage_tag_to_result_table(return_result)
        translate_project_name(return_result)
        return Response(return_result)

    @staticmethod
    def format_classic(return_result):
        optimized_result = return_result
        if "~ResultTableField.result_table" in optimized_result:
            optimized_result["fields"] = []
            for item in optimized_result["~ResultTableField.result_table"]:
                optimized_result["fields"].append(item)
            optimized_result.pop("~ResultTableField.result_table")
        if "~TdwTable.result_table" in optimized_result:
            optimized_result["extra"] = {}
            optimized_result["extra"]["tdw"] = optimized_result["~TdwTable.result_table"][0]
            optimized_result.pop("~TdwTable.result_table")
        if "~StorageResultTable.result_table" in optimized_result:
            optimized_result["storages"] = {}
            for item in optimized_result["~StorageResultTable.result_table"]:
                if "storage_cluster" in item:
                    if not item.get("active", True):
                        continue
                    storage_instance = (
                        item["storage_cluster"][0]
                        if isinstance(item["storage_cluster"], list)
                        else item["storage_cluster"]
                    )
                    optimized_result["storages"][storage_instance["cluster_type"]] = item
                    item["storage_cluster"] = storage_instance
                elif "storage_channel" in item:
                    if not item.get("active", True):
                        continue
                    storage_instance = (
                        item["storage_channel"][0]
                        if isinstance(item["storage_channel"], list)
                        else item["storage_channel"]
                    )
                    optimized_result["storages"][storage_instance["cluster_type"]] = item
                    item["storage_channel"] = storage_instance
            # if not optimized_result['storages']:
            #     raise ERPCriterionError(_('erp表达式未到cluster信息。'))
            optimized_result.pop("~StorageResultTable.result_table")

    @params_valid(serializer=ResultTableUpdateSerializer)
    def update(self, request, result_table_id, params):
        """
        @api {put} /meta/result_tables/:result_table_id/ 修改结果表信息
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName update_result_table

        @apiParam {String} bk_username 用户名
        @apiParam {String} [result_table_name_alias] 结果表中文名
        @apiParam {String="user", "system"} [generate_type] 生成类型
        @apiParam {String="public","private","sensitive"} [sensitivity] 敏感度
        @apiParam {Number} [count_freq] 统计频率
        @apiParam {String} [description] 结果表描述信息
        @apiParam {Object[]} [fields] 结果表字段信息
        @apiParam {String='bkdata','tdw'} [platform='bkdata'] RT平台归属。
        @apiParam {String} [is_managed==1] 是否被托管
        @apiParam {Object[]} extra 其他平台额外信息更新。

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "result_table_name_alias ": "xxx",
                "generate_type": "system",
                "sensitivity": "public",
                "count_freq": 60,
                "description": "xxx",
                "fields": [
                    {
                        "id": 11,
                        "field_index": 1,
                        "field_name": "timestamp",
                        "field_alias": "时间",
                        "description": "",
                        "field_type": "timestamp",
                        "is_dimension": 0,
                        "origins": ""
                    }
                ],
                "extra": {
                    "tdw": {
                      "pri_part_key": "tdbank_imp_date",
                      "pri_part_type": "range",
                      "table_comment": "test1",
                      "usability":"Grab",
                      "associated_lz_id": {
                        "import": 211,
                        "check": 403
                      },
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "591_test_table",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1500001 参数校验失败
        @apiError 1521020 结果表不存在
        @apiError 1521021 结果表字段冲突
        """

        try:
            result_table = ResultTable.objects.get(result_table_id=result_table_id)
        except ResultTable.DoesNotExist:
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})
        with auto_meta_sync(using="bkdata_basic"):
            self.run_update(request, result_table, params)
        return Response(result_table_id)

    @params_valid(serializer=DestroySerializer)
    def destroy(self, request, result_table_id, params):
        """
        @api {delete} /meta/result_tables/:result_table_id/ 删除结果表
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName delete_result_table

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "591_test_table",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521020 结果表不存在
        @apiError 1521024 该结果表的数据处理尚未删除
        """
        try:
            result_table = ResultTable.objects.get(result_table_id=result_table_id)
        except ResultTable.DoesNotExist:
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})

        # 删除结果表前必须先删除关联的数据处理
        relations = DataProcessingRelation.objects.filter(data_set_type="result_table", data_set_id=result_table_id)
        if relations.count() > 0:
            raise meta_errors.ResultTableHasRelationError()

        # 把待删除的结果表信息组装后写入result_table_del表中，并从result_table和result_table_field中删除记录
        with auto_meta_sync(using="bkdata_basic"):
            self.run_destroy(request, result_table)
        return Response(result_table_id)

    @action(detail=True, methods=["get"], url_path="geog_area")
    def get_geog_area(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/geog_area/ 获取rt所属地域

        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName get_geog_area_result_table

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "code": "inland"
                    }
                ]
            }

        @apiError 1521020 结果表不存在
        """

        if not str(result_table_id).strip():
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})
        return Response(parseresult.get_result_table_geog_area("'" + parseresult.escape_string(result_table_id) + "'"))

    @action(detail=True, methods=["get"], url_path="fields")
    def get_fields(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/fields/ 获取结果表字段信息

        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName get_fields_result_table

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "id": 13730,
                        "field_index": 1,
                        "field_name": "timestamp",
                        "field_alias": "时间",
                        "description": "",
                        "field_type": "timestamp",
                        "is_dimension": 0,
                        "origins": "",
                        "created_by":"admin",
                        "created_at": null,
                        "updated_by": null,
                        "updated_at": null
                    }
                ]
            }

        @apiError 1521020 结果表不存在
        """
        if not str(result_table_id).strip():
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})
        return Response(parseresult.get_result_table_fields_v3("'" + parseresult.escape_string(result_table_id) + "'"))

    @action(detail=True, methods=["get"], url_path="storages")
    def get_storages(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/storages/ 获取结果表的存储信息

        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName get_storages_result_table

        @apiParam {String} [cluster_type] 存储类型<如Druid/TSDB>[可选]

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "tspider": {
                        "id": 2,
                        "storage_cluster": {
                            "storage_cluster_config_id": 1,
                            "cluster_name": "xxx",
                            "cluster_type": "tspider",
                            "cluster_domain": "xxx",
                            "cluster_group": "xxx",
                            "connection_info": "{}",
                            "priority": 234,
                            "version": "23423",
                            "belongs_to": "bkdata"
                        },
                        "physical_table_name": "xxx",
                        "expires": "xxx",
                        "storage_config": "xxx",
                        "priority": 1,
                        "generate_type": "user",
                        "description": "xxx",
                        "created_by":"admin",
                        "created_at": null,
                        "updated_by": null,
                        "updated_at": null
                    },
                    "kafka": {
                        "id": 1,
                        "storage_cluster": {},
                        "storage_channel": {
                            "channel_cluster_config_id": 1,
                            "cluster_name": "xxx",
                            "cluster_type": "kafka",
                            "cluster_role": "inner",
                            "cluster_domain": "xxx",
                            "cluster_backup_ips": "xxx",
                            "cluster_port": 2432,
                            "zk_domain": "127.0.0.1",
                            "zk_port": 3481,
                            "zk_root_path": "/abc/defg",
                            "priority": 234,
                            "attribute": "bkdata",
                            "description": "sdfdsf"
                        },
                        "physical_table_name": "xxx",
                        "expires": "xxx",
                        "storage_config": "xxx",
                        "priority": 1,
                        "generate_type": "user",
                        "description": "xxx",
                        "created_by":"admin",
                        "created_at": null,
                        "updated_by": null,
                        "updated_at": null
                    }
                }
            }

        @apiError 1521020 结果表不存在
        """
        cluster_type = request.query_params.get("cluster_type")
        query_result_dict = parseresult.get_result_table_storages_v3(
            "'" + parseresult.escape_string(result_table_id) + "'", cluster_type=cluster_type
        )
        storages_result_dict = query_result_dict.get(result_table_id, {})
        return Response(storages_result_dict)

    @action(detail=True, methods=["get"], url_path="extra")
    def get_extra(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/extra 获取单个结果表实例的特有信息
        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName retrieve_result_table_extra

        @apiParam {String[]} [tdw_related] 需要返回的tdw关联信息,取值[cols_info,parts_info]
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            "tdw": {
                  "cluster_id": "lz",
                  "db_name": "ieg_tdbank",
                  "table_name": "bkdata_test_dsl_test1_fdt0",
                  "pri_part_key": "tdbank_imp_date",
                  "pri_part_type": "range",
                  "sub_part_key": null,
                  "sub_part_type": null,
                  "created_by ": "root",
                  "created_at": "2019-03-21 10:00:03.143735",
                  "updated_by": "user_by_blueking",
                  "updated_at": "2019-03-21 10:00:03.143735",
                  "table_comment": "test1",
                   "usability":"OK",
                  "associated_lz_id": {
                    "import": 211,
                    "check": 403
                  },
                  "parts_info": [
                    {
                      "level": 0,
                      "part_name": "p_2019041104",
                      "part_values": [
                        "2019041105"
                      ]
                    },
                    {
                      "level": 0,
                      "part_name": "p_2019041106",
                      "part_values": [
                        "2019041107"
                      ]
                    }
                  ],
                  "cols_info":[      {
                    "col_name": "tdbank_imp_date",
                    "col_type": "string",
                    "col_coment": "partition fields"
                  },
                  {
                    "col_name": "dteventtimestamp",
                    "col_type": "string",
                    "col_coment": "dtEventTimeStamp"
                  },
                  {
                    "col_name": "idx",
                    "col_type": "string",
                    "col_coment": "idx"
                  }]
                }
        """

        tdw_related = request.query_params.getlist("tdw_related")
        return_result = {}
        if settings.ENABLED_TDW:
            from meta.extend.tencent.tdw.models import TdwTable
            from meta.extend.tencent.tdw.support import TDWRTSupport

            tables = TdwTable.objects.filter(result_table_id=result_table_id).only("table_id").all()
            if tables:
                return_result = {"tdw": TDWRTSupport.tdw_retrieve(request, tables[0], tdw_related)}
        return Response(return_result)

    @action(detail=True, methods=["get"], url_path="lineage")
    def get_lineage(self, request, result_table_id):
        """
        @api {get} /meta/result_tables/:result_table_id/lineage/ 获取结果表的血缘信息

        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName get_lineage_result_table

        @apiParam {Number} [depth] 数字，默认3[可选]
        @apiParam {String} [direction] BOTH or INPUT or OUTPUT, 默认BOTH[可选]

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "direction": "BOTH",
                    "depth": 2,
                    "nodes": {
                        "result_table_1":{},
                        "result_table_2":{},
                        "result_table_3":{},
                        "result_table_4":{},
                        "data_processing_1":{},
                        "data_processing_2":{}
                    },
                    "relations": [
                        {
                        "from": "result_table_1",
                        "to": "data_processing_1"
                        },
                        {
                        "from": "data_processing_1",
                        "to": "result_table_2"
                        },
                        {
                        "from": "result_table_2",
                        "to": "data_processing_2"
                        },
                        {
                        "from": "result_table_4",
                        "to": "data_processing_2"
                        },
                        {
                        "from": "data_processing_2",
                        "to": "result_table_3"
                        }
                    ]
                }
            }

        @apiError 1521020 结果表不存在
        """
        depth = request.query_params.get("depth")
        direction = request.query_params.get("direction")
        backend_type = request.query_params.get("backend_type")
        params = {"type_name": "ResultTable", "qualified_name": result_table_id}
        if depth:
            params["depth"] = depth
        if direction:
            params["direction"] = direction
        if backend_type:
            params["backend_type"] = backend_type
        res_lineage = parseresult.get_result_table_lineage_info(params)
        return Response(res_lineage)

    @action(detail=True, methods=["post", "put", "patch"], url_path="override")
    @params_valid(serializer=ResultTableOverrideSerializer)
    def override_result_table(self, request, result_table_id, params):
        """
        @api {post/put/patch} /meta/result_tables/:result_table_id/override/ 用新结果表替代旧结果表（用于修改结果表ID的场景）

        @apiVersion 0.2.0
        @apiGroup ResultTable
        @apiName override_result_table

        @apiParam {String} bk_username 用户名
        @apiParam {String} result_table_id 结果表ID
        @apiParam {Number} bk_biz_id 业务ID
        @apiParam {String} result_table_name 结果表名称
        @apiParam {String} [result_table_name_alias] 结果表中文名
        @apiParam {String} [result_table_type] 结果表类型
        @apiParam {String="user", "system"} [generate_type] 生成类型
        @apiParam {String="public","private","sensitive"} [sensitivity] 敏感度
        @apiParam {Number} [count_freq] 统计频率
        @apiParam {string} [description] 结果表描述信息
        @apiParam {Object[]} [fields] 结果表字段信息
        @apiParam {String[]} tags 标签code列表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "zhangshan",
                "bk_biz_id": 100,
                "result_table_id": "100_new_table",
                "result_table_name": "new_table",
                "result_table_name_alias ": "xxx",
                "result_table_type": null,
                "generate_type": "user",
                "sensitivity": "public",
                "count_freq": 60,
                "description": "xxx",
                "tags": ["NA"],
                "fields": [
                    {
                        "field_index": 1,
                        "field_name": "timestamp",
                        "field_alias": "时间",
                        "description": "",
                        "field_type": "timestamp",
                        "is_dimension": 0,
                        "origins": ""
                    }
                ]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "{new_result_table_id}",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521020 结果表不存在
        """
        try:
            result_table = ResultTable.objects.get(result_table_id=result_table_id)
        except ResultTable.DoesNotExist:
            raise meta_errors.ResultTableNotExistError(message_kv={"result_table_id": result_table_id})

        if result_table.platform == "tdw":
            raise meta_errors.ParamsError(message_kv={"error_detail": _("Tdw RT表不支持。")})
        with auto_meta_sync(using="bkdata_basic"):
            new_result_table_id = params["result_table_id"]
            # 更新结果表信息
            result_table.result_table_id = new_result_table_id
            ResultTable.objects.update_table(result_table, params)

            # 更新数据处理关系
            relations = DataProcessingRelation.objects.filter(data_set_type="result_table", data_set_id=result_table_id)
            for relation in relations:
                relation.data_set_id = new_result_table_id
                relation.id = None
                relation.save()
            DataProcessingRelation.objects.filter(data_set_type="result_table", data_set_id=result_table_id).delete()

            result_table = ResultTable.objects.get(result_table_id=result_table_id)
            ResultTable.objects.delete_table(result_table)

        return Response(new_result_table_id)
