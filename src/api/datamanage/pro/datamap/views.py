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


from datamanage.lite.tag import tagaction
from datamanage.pro.datamap import dmaction
from datamanage.pro.datamap.serializers import (
    BasicInfoSerializer,
    BasicListSerializer,
    SearchSerializer,
)
from datamanage.utils.api.datamanage import DatamanageApi
from django.db import connections
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.log import logger
from common.views import APIViewSet


class DataMapViewSet(APIViewSet):
    @list_route(methods=["post"], url_path="search")
    @params_valid(serializer=SearchSerializer)
    def search(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/search/ 条件查询接口,获得数据地图树形结构
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName search

        @apiParam {Integer} bk_biz_id 业务
        @apiParam {Integer} project_id 项目
        @apiParam {List} tag_ids 标签code列表
        @apiParam {String} keyword 搜索关键词

        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":591,
            "project_id":1,
            "tag_ids":["system","login"],
            "keyword":"login"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 3
            }
        """

        index_name_list = ["dataset_count"]
        # tree_list, virtual_data_mart_node, other_node = dmaction.datamap_search(params,
        #                                                                         connections['bkdata_basic_slave'],
        #                                                                         index_name_list)
        # 以下为dgraph版本
        tree_list, virtual_data_mart_node, other_node = dmaction.datamap_search_dgraph(
            params, connections["bkdata_basic_slave"], index_name_list
        )
        data_mart_root = tree_list[0]
        root_sub_list = data_mart_root["sub_list"]  # 技术运营数据,业务用户数据
        show_list = []
        if tree_list:
            for tree_root in root_sub_list:
                sub_list = tree_root["sub_list"]
                for sub_dict in sub_list:
                    sub_dict[dmaction.VIRTUAL_DM_POS_FIELD] = tree_root["seq_index"]
                show_list.extend(sub_list)

        # 删除kpath=0的节点
        dmaction.delete_not_kpath_node(show_list)

        dmaction.add_virtual_other_node(show_list)  # 添加虚拟的其他节点

        # 添加dm_pos字段
        for show_dict in show_list:
            dmaction.add_dm_pos_field(show_dict, show_dict[dmaction.VIRTUAL_DM_POS_FIELD])

        # 赋值loc字段
        for show_dict in show_list:
            dmaction.add_loc_field(show_dict, show_dict.get(dmaction.DATAMAP_LOC_FIELD))

        show_list.sort(key=lambda l: (l["loc"], l["datamap_seq_index"]), reverse=False)

        other_node[dmaction.VIRTUAL_DM_POS_FIELD] = 2
        other_node[dmaction.DATAMAP_LOC_FIELD] = 0
        show_list.append(other_node)
        virtual_data_mart_node["sub_list"] = show_list
        virtual_data_mart_node[dmaction.VIRTUAL_DM_POS_FIELD] = 0
        virtual_data_mart_node[dmaction.DATAMAP_LOC_FIELD] = -1
        return Response([virtual_data_mart_node])

    @list_route(methods=["post"], url_path="get_basic_info")
    @params_valid(serializer=BasicInfoSerializer)
    def get_basic_info(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/get_basic_info/ 获取基础信息
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName get_basic_info

        @apiParam {String} tag_code 标签名称

         @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "tag_code":"online",
            "me_type":"tag",
            "has_standard":1,
            "cal_type":["standard","only_standard"]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "project_count": 1,
                "standard_dataset_count": 274,
                "bk_biz_count": 119,
                "data_source_count": 0,
                "dataset_count": 274,
                "project_list": [
                    4172
                ],
                "bk_biz_list": [
                    100160,
                    730,
                    1123
                ]
            },
            "result": true
        }
        """

        # result_dict = dmaction.floating_window_query(params, connections['bkdata_basic_slave'],
        #                                              dmaction.NEED_FLOATING_DETAIL)
        # 以下为dgraph版本
        result_dict = dmaction.floating_window_query_dgraph(
            params, connections["bkdata_basic_slave"], dmaction.NEED_FLOATING_DETAIL
        )
        return Response(result_dict)

    @list_route(methods=["post"], url_path="search_summary")
    @params_valid(serializer=SearchSerializer)
    def search_summary(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/search_summary/ 查询汇总信息接口
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName search_summary

        @apiParam {Integer} bk_biz_id 业务
        @apiParam {Integer} project_id 项目
        @apiParam {List} tag_ids 标签code列表
        @apiParam {String} keyword 搜索关键词

        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "cal_type":["standard","only_standard"]
        }


        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "project_count": 1,
                "recent_dataset_count_details": [
                    {
                        "dataset_count": 0,
                        "day": "20190719"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190720"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190721"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190722"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190723"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190724"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190725"
                    }
                ],
                "recent_dataset_count_sum": 0,
                "standard_dataset_count": 646,
                "bk_biz_count": 196,
                "recent_standard_dataset_count_details": [
                    {
                        "standard_dataset_count": 0,
                        "day": "20190719"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190720"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190721"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190722"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190723"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190724"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190725"
                    }
                ],
                "data_source_count": 0,
                "recent_data_source_count_details": [
                    {
                        "day": "20190719",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190720",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190721",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190722",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190723",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190724",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190725",
                        "data_source_count": 0
                    }
                ],
                "dataset_count": 646,
                "recent_standard_dataset_count_sum": 0,
                "recent_data_source_count_sum": 0
            },
            "result": true
        }
        """
        result_dict = dmaction.search_summary_dgraph(params, connections["bkdata_basic_slave"])
        dmaction.clear_bk_biz_id_list(result_dict)
        return Response(result_dict)

    @list_route(methods=["post"], url_path="datamap_summary")
    @params_valid(serializer=BasicListSerializer)
    def datamap_summary(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/datamap_summary/ 查询汇总信息接口
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName search_summary

        @apiParam {Integer} bk_biz_id 业务
        @apiParam {Integer} project_id 项目
        @apiParam {List} tag_ids 标签code列表
        @apiParam {String} keyword 搜索关键词

        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "cal_type":["standard","only_standard"]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "project_count": 1,
                "recent_dataset_count_sum": 0,
                "standard_dataset_count": 646,
                "bk_biz_count": 196,
                "data_source_count": 0,
                "dataset_count": 646,
                "recent_standard_dataset_count_sum": 0,
                "recent_data_source_count_sum": 0
            },
            "result": true
        }
        """
        # 以下为dgraph版本
        platform = params.get("platform", "all")
        if platform == "tdw":
            result_dict = DatamanageApi.get_data_dict_count(params).data
            # 标准化结果表是0
            result_dict["standard_dataset_count"] = 0
            return Response(result_dict)
        result_dict = dmaction.datamap_summary(params, connections["bkdata_basic_slave"])
        dmaction.clear_bk_biz_id_list(result_dict)
        return Response(result_dict)

    @list_route(methods=["post"], url_path="datamap_recent_summary")
    @params_valid(serializer=SearchSerializer)
    def datamap_recent_summary(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/datamap_recent_summary/ 查询汇总最近7天新增数据量接口
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName search_summary

        @apiParam {Integer} bk_biz_id 业务
        @apiParam {Integer} project_id 项目
        @apiParam {List} tag_ids 标签code列表
        @apiParam {String} keyword 搜索关键词

        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "cal_type":["standard","only_standard"]
        }


        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "project_count": 1,
                "recent_dataset_count_details": [
                    {
                        "dataset_count": 0,
                        "day": "20190719"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190720"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190721"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190722"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190723"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190724"
                    },
                    {
                        "dataset_count": 0,
                        "day": "20190725"
                    }
                ],
                "recent_dataset_count_sum": 0,
                "standard_dataset_count": 646,
                "bk_biz_count": 196,
                "recent_standard_dataset_count_details": [
                    {
                        "standard_dataset_count": 0,
                        "day": "20190719"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190720"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190721"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190722"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190723"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190724"
                    },
                    {
                        "standard_dataset_count": 0,
                        "day": "20190725"
                    }
                ],
                "data_source_count": 0,
                "recent_data_source_count_details": [
                    {
                        "day": "20190719",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190720",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190721",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190722",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190723",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190724",
                        "data_source_count": 0
                    },
                    {
                        "day": "20190725",
                        "data_source_count": 0
                    }
                ],
                "dataset_count": 646,
                "recent_standard_dataset_count_sum": 0,
                "recent_data_source_count_sum": 0
            },
            "result": true
        }
        """
        # 以下为dgraph版本
        result_dict = dmaction.datamap_recent_summary(params, connections["bkdata_basic_slave"])
        dmaction.clear_bk_biz_id_list(result_dict)
        return Response(result_dict)

    @list_route(methods=["post"], url_path="get_basic_list")
    @params_valid(serializer=BasicListSerializer)
    def get_basic_list(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/get_basic_list/ 获取基础列表信息
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName get_basic_list

         @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "tag_code":"online",
            "me_type":"tag",
            "cal_type":["standard","only_standard"],
            "page":1,
            "page_size":10,
            "data_set_type":"all",//result_table、raw_data
            "created_by":"xiaoming"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "project_count": 1,
                "bk_biz_count": 119,
                "dataset_count": 2,
                "data_source_count": 0,
                "count":2,
                "results": [
                    {"data_set_type":"result_table",
                    "data_set_id":"591_durant1115",
                    "is_standard":0},
                    {"data_set_type":"raw_data",
                    "data_set_id":"123",
                    "is_standard":0}
                ],

            },
            "result": true
        }
        """
        # 比悬浮框接口加上了返回rt_id列表和分页的功能
        extra_retrieve_dict = params.get("extra_retrieve")
        # 以下为dgraph版本
        params.pop("has_standard", None)
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            need_only_uids=False,
            need_all_data_dict_list=True,
        )
        dmaction.parse_basic_list_dgraph_result(result_dict)
        if extra_retrieve_dict:
            dmaction.get_detail_via_erp(result_dict, extra_retrieve_dict)
        dmaction.clear_bk_biz_id_list(result_dict)
        return Response(result_dict)

    @list_route(methods=["post"], url_path="get_data_dict_list")
    @params_valid(serializer=BasicListSerializer)
    def get_data_dict_list(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/get_data_dict_list/ 获取数据字典列表详情信息
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName get_basic_list

         @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "tag_code":"online",
            "me_type":"tag",
            "cal_type":["standard","only_standard"],
            "page":1,
            "page_size":10,
            "data_set_type":"all",//result_table、raw_data
            "created_by":"xiaoming"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "results": [
                    {"data_set_type":"result_table",
                    "data_set_id":"591_durant1115",
                    "is_standard":0},
                    {"data_set_type":"raw_data",
                    "data_set_id":"123",
                    "is_standard":0}
                ]

            },
            "result": true
        }
        """
        # 比悬浮框接口加上了返回rt_id列表和分页的功能
        extra_retrieve_dict = params.get("extra_retrieve")
        # 以下为dgraph版本
        params.pop("has_standard", None)
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            need_only_uids=True,
        )
        # 判断查出来的数据是不是标准化的
        if result_dict.get("data_set_list", []):
            if len(result_dict.get("data_set_list", [])) != 1:
                data_set_list = [
                    each_dataset.get("data_set_id").encode("utf-8")
                    if each_dataset.get("data_set_type") == "result_table"
                    else str(each_dataset.get("data_set_id"))
                    for each_dataset in result_dict.get("data_set_list", [])
                ]
                data_set_tuple = tuple(data_set_list)
                sql = """select data_set_id from dm_task_detail where active=1 and data_set_id in {}
                    and standard_version_id in(select b.id standard_version_id  from dm_standard_config a,
                    dm_standard_version_config b,tag_target c where b.standard_version_status='online'
                    and a.id=b.standard_id and a.id=c.target_id and a.active=1  and c.active=1
                    and c.tag_code=c.source_tag_code and c.target_type='standard')""".format(
                    data_set_tuple
                )
            else:
                sql = """select data_set_id from dm_task_detail where active=1 and data_set_id in ('{}')
                    and standard_version_id in(select b.id standard_version_id  from dm_standard_config a,
                    dm_standard_version_config b,tag_target c where b.standard_version_status='online'
                    and a.id=b.standard_id and a.id=c.target_id and a.active=1  and c.active=1
                    and c.tag_code=c.source_tag_code and c.target_type='standard')""".format(
                    result_dict.get("data_set_list")[0].get("data_set_id")
                )
            logger.info("standard sql:%s" % sql)
            stan_data_set_list_tmp = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
            stan_data_set_list = [each_dataset["data_set_id"] for each_dataset in stan_data_set_list_tmp]

            for each_dataset in result_dict.get("data_set_list", []):
                if each_dataset["data_set_id"] in stan_data_set_list:
                    each_dataset["is_standard"] = 1

        dmaction.parse_basic_list_dgraph_result(result_dict)
        if extra_retrieve_dict:
            dmaction.get_detail_via_erp(result_dict, extra_retrieve_dict)
        dmaction.clear_bk_biz_id_list(result_dict)
        return Response(result_dict["results"])

    @list_route(methods=["post"], url_path="get_data_dict_count")
    @params_valid(serializer=BasicListSerializer)
    def get_data_dict_count(self, request, params):
        """
        @api {post} /datamanage/datamap/retrieve/get_data_dict_count/ 获取数据字典列表统计信息
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName get_basic_list

         @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[],
            "keyword":"",
            "tag_code":"online",
            "me_type":"tag",
            "cal_type":["standard","only_standard"],
            "page":1,
            "page_size":10,
            "data_set_type":"all",//result_table、raw_data
            "created_by":"xiaoming"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "results": [
                    {"data_set_type":"result_table",
                    "data_set_id":"591_durant1115",
                    "is_standard":0},
                    {"data_set_type":"raw_data",
                    "data_set_id":"123",
                    "is_standard":0}
                ]

            },
            "result": true
        }
        """
        # 比悬浮框接口加上了返回rt_id列表和分页的功能
        # 以下为dgraph版本
        params.pop("has_standard", None)
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            need_only_uids=False,
        )
        dmaction.parse_basic_list_dgraph_result(result_dict)
        dmaction.clear_bk_biz_id_list(result_dict)
        result_dict.pop("results", None)
        return Response(result_dict)
