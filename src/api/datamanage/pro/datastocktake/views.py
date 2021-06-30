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

import numpy as np
import pandas as pd
from datamanage.lite.tag import tagaction
from datamanage.pro.datamap import dmaction
from datamanage.pro.datamap.serializers import BasicListSerializer, DataValueSerializer
from datamanage.pro.datastocktake.dataset_process import dataset_filter
from datamanage.pro.datastocktake.metrics.score_level import level_distribution
from datamanage.pro.datastocktake.metrics.storage import storage_capacity_trend
from datamanage.pro.datastocktake.metrics.trend import score_trend_pandas_groupby
from datamanage.pro.datastocktake.sankey_diagram import (
    fetch_value_between_source_target,
    format_sankey_diagram,
    minimal_value,
)
from datamanage.pro.datastocktake.settings import SCORE_DICT
from datamanage.pro.lifecycle.metrics.cost import hum_storage_unit
from datamanage.pro.lifecycle.metrics.ranking import score_aggregate
from datamanage.pro.utils.time import get_date
from datamanage.utils.api.dataquery import DataqueryApi
from django.conf import settings
from django.core.cache import cache
from django.db import connections
from django.utils.translation import ugettext as _
from pandas import DataFrame
from rest_framework.response import Response

from common.bklanguage import bktranslates
from common.decorators import list_route, params_valid
from common.views import APIViewSet

RUN_MODE = getattr(settings, "RUN_MODE", "DEVELOP")

METRIC_DICT = {
    "active": "project_id",
    "app_important_level_name": "bk_biz_id",
    "is_bip": "bk_biz_id",
}

ABN_BIP_GRADE_NAME_LIST = ["确认自研信用", "退市", "已下架"]
CORR_BIP_GRADE_NAME = _("其他")

OPER_STATE_NAME_ORDER = [
    "前期接触",
    "接入评估",
    "接入准备",
    "接入中",
    "技术封测",
    "测试阶段",
    "封测",
    "内测",
    "不删档",
    "公测",
    "正式运行",
    "停运",
    "取消",
    "退市",
    "其他",
]

BIP_GRADE_NAME_ORDER = [
    "其他",
    "暂无评级",
    "长尾",
    "三星",
    "预备四星",
    "四星",
    "限制性预备五星",
    "预备五星",
    "五星",
    "预备六星",
    "六星",
]


class QueryView(APIViewSet):
    @list_route(methods=["get"], url_path="popular_query")
    def popular_query(self, request):
        """
        @api {get} /datamanage/datastocktake/query/popular_query/ 获取最近热门查询表
        @apiVersion 0.1.0
        @apiGroup Query
        @apiName popular_query
        @apiParam {top} int 获取最近热门topn查询表
        @apiSuccessExample Success-Response:
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "count":20000,
                    "result_table_name_alias":"xx",
                    "app_code":"xx",
                    "result_table_id":"xx"
                }
            ],
            "result":true
        }
        """
        # 获取前日日期，格式：'20200115'，用昨日日期的话，凌晨1点前若离线任务没有算完会导致热门查询没有数据
        yesterday = get_date()
        top = int(request.query_params.get("top", 10))
        prefer_storage = "tspider"
        sql_latest = """SELECT count, result_table_id, result_table_name_alias, app_code
                        FROM 591_dataquery_processing_type_rt_alias_one_day
                        WHERE thedate={} and result_table_id is not null and app_code is not null
                        order by count desc limit {}""".format(
            yesterday, top
        )
        ret_latest = DataqueryApi.query({"sql": sql_latest, "prefer_storage": prefer_storage}).data
        if ret_latest:
            ret_latest = ret_latest.get("list", [])
            app_code_dict = cache.get("app_code_dict")
            for each in ret_latest:
                if "app_code" in each:
                    each["app_code_alias"] = (
                        app_code_dict[each["app_code"]] if app_code_dict.get(each["app_code"]) else each["app_code"]
                    )
        else:
            return Response([])
        # 此处缺ret_latest里app_code的中文
        return Response(ret_latest)


class DataSourceDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="distribution")
    @params_valid(serializer=BasicListSerializer)
    def data_source_distribution(self, request, params):
        """
        @api {post} /datamanage/datastocktake/data_source/distribution/ 获取数据来源分布情况
        @apiVersion 0.1.0
        @apiGroup DataSourceDistribution
        @apiName data_source_distribution

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
                "tag_count_list": [
                    6,
                    278
                ],
                "tag_code_list": [
                    "xx",
                    "other"
                ],
                "tag_alias_list": [
                    "XX",
                    "其他"
                ]
            },
            "result": true
        }
        """
        params.pop("has_standard", None)
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            data_source_distribute=True,
        ).get("dgraph_result")
        # 数据来源对应的数据
        tag_code_list = ["components", "sys_host", "system"]
        tag_alias_list = [_("组件"), _("设备"), _("系统")]
        tag_count_list = [
            result_dict.get("c_%s" % each_tag_code)[0].get("count", 0)
            for each_tag_code in tag_code_list
            if result_dict.get("c_%s" % each_tag_code)
        ]
        # 满足查询条件的总数
        rt_count = result_dict.get("rt_count")[0].get("count", 0) if result_dict.get("rt_count") else 0
        rd_count = result_dict.get("rd_count")[0].get("count", 0) if result_dict.get("rd_count") else 0
        tdw_count = result_dict.get("tdw_count")[0].get("count", 0) if result_dict.get("tdw_count") else 0
        total_count = rt_count + rd_count + tdw_count
        # 其他对应的数目
        other_count = total_count
        for each_tag_count in tag_count_list:
            other_count -= each_tag_count
        tag_count_list.append(other_count)
        tag_code_list.append("other")
        tag_alias_list.append(_("其他"))
        return Response(
            {
                "tag_count_list": tag_count_list,
                "tag_alias_list": tag_alias_list,
                "tag_code_list": tag_code_list,
            }
        )

    @list_route(methods=["post"], url_path="detail_distribution")
    @params_valid(serializer=BasicListSerializer)
    def data_source_detail_distribution(self, request, params):
        """
        @api {post} /datamanage/datastocktake/data_source/detail_distribution/ 获取数据来源详细分布情况
        @apiVersion 0.1.0
        @apiGroup DataSourceDistribution
        @apiName data_source_detail_distribution
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
            "created_by":"xiaoming"，
            "top":5,
            "parent_tag_code":"components"
        }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
           {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "tag_count_list": [
                    6,
                    278
                ],
                "tag_code_list": [
                    "xx",
                    "other"
                ],
                "tag_alias_list": [
                    "XX",
                    "其他"
                ]
            },
            "result": true
        }
        """
        params.pop("has_standard", None)
        top = params.get("top", 5)
        parent_code = params.get("parent_tag_code", "all")
        # 1 对所有数据来源标签的查询结果
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            data_source_detail_distribute=True,
        )
        dgraph_result = result_dict.get("dgraph_result")
        data_source_tag_list = result_dict.get("data_source_tag_list")

        for each_tag in data_source_tag_list:
            each_tag["count"] = (
                dgraph_result.get("c_%s" % each_tag.get("tag_code"))[0].get("count", 0)
                if dgraph_result.get("c_%s" % each_tag.get("tag_code"))
                else 0
            )
        # 按照count对所有数据来源二级标签进行排序
        data_source_tag_list.sort(key=lambda k: (k.get("count", 0)), reverse=True)
        if parent_code == "all":
            top_tag_list = data_source_tag_list[:top]
            # 满足查询条件的总数
            rt_count = dgraph_result.get("rt_count")[0].get("count", 0) if dgraph_result.get("rt_count") else 0
            rd_count = dgraph_result.get("rd_count")[0].get("count", 0) if dgraph_result.get("rd_count") else 0
            tdw_count = dgraph_result.get("tdw_count")[0].get("count", 0) if dgraph_result.get("tdw_count") else 0
            total_count = rt_count + rd_count + tdw_count

        else:
            total_count = data_source_tag_list[0].get("count")
            top_tag_list = data_source_tag_list[1 : (top + 1)]

        other_count = total_count
        for each_tag_count in top_tag_list:
            other_count -= each_tag_count.get("count")

        # 数据来源对应的数据
        tag_code_list = [each_tag.get("tag_code") for each_tag in top_tag_list]
        tag_alias_list = [each_tag.get("tag_alias") for each_tag in top_tag_list]
        tag_count_list = [each_tag.get("count") for each_tag in top_tag_list]
        if other_count > 0:
            tag_count_list.append(other_count)
            tag_code_list.append("other")
            tag_alias_list.append(_("其他"))
        return Response(
            {
                "tag_count_list": tag_count_list,
                "tag_alias_list": tag_alias_list,
                "tag_code_list": tag_code_list,
            }
        )


class DataTypeDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="distribution")
    @params_valid(serializer=BasicListSerializer)
    def data_type_distribution(self, request, params):
        """
        @api {post} /datamanage/datastocktake/data_type/distribution/ 获取数据类型分布情况
        @apiVersion 0.1.0
        @apiGroup DataTypeDistribution
        @apiName data_type_distribution

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
                "tag_count_list": [
                    6,
                    278
                ],
                "tag_code_list": [
                    "xx",
                    "other"
                ],
                "tag_alias_list": [
                    "XX",
                    "其他"
                ]
            },
            "result": true
        }
        """
        params.pop("has_standard", None)
        # 1 对所有数据来源标签的查询结果
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            data_type_distribute=True,
        )
        dgraph_result = result_dict.get("dgraph_result")
        data_type_tag_list = result_dict.get("data_source_tag_list")

        for each_tag in data_type_tag_list:
            each_tag["count"] = (
                dgraph_result.get("c_%s" % each_tag.get("tag_code"))[0].get("count", 0)
                if dgraph_result.get("c_%s" % each_tag.get("tag_code"))
                else 0
            )

        # 满足查询条件的总数
        rt_count = dgraph_result.get("rt_count")[0].get("count", 0) if dgraph_result.get("rt_count") else 0
        rd_count = dgraph_result.get("rd_count")[0].get("count", 0) if dgraph_result.get("rd_count") else 0
        tdw_count = dgraph_result.get("tdw_count")[0].get("count", 0) if dgraph_result.get("tdw_count") else 0
        total_count = rt_count + rd_count + tdw_count

        other_count = total_count
        for each_tag_count in data_type_tag_list:
            other_count -= each_tag_count.get("count")

        # 数据来源对应的数据
        tag_code_list = [each_tag.get("tag_code") for each_tag in data_type_tag_list]
        tag_alias_list = [each_tag.get("tag_alias") for each_tag in data_type_tag_list]
        tag_count_list = [each_tag.get("count") for each_tag in data_type_tag_list]
        if other_count > 0:
            tag_count_list.append(other_count)
            tag_code_list.append("other")
            tag_alias_list.append(_("其他"))
        return Response(
            {
                "tag_count_list": tag_count_list,
                "tag_alias_list": tag_alias_list,
                "tag_code_list": tag_code_list,
            }
        )


class SankeyDiagramView(APIViewSet):
    @list_route(methods=["post"], url_path="distribution")
    @params_valid(serializer=BasicListSerializer)
    def sankey_diagram_distribution(self, request, params):
        """
        @api {post} /datamanage/datastocktake/sankey_diagram/distribution/ 获取桑基图
        @apiGroup SankeyDiagramView
        @apiName sankey_diagram_distribution
        """
        params.pop("has_standard", None)
        level = params.get("level", 4)
        platform = params.get("platform", "all")
        if platform == "tdw":
            return Response(
                {
                    "label": [],
                    "source": [],
                    "target": [],
                    "value": [],
                    "alias": [],
                    "other_app_code_list": [],
                    "level": 0,
                }
            )
        # 1 对所有数据来源标签的查询结果
        result_dict = dmaction.floating_window_query_dgraph(
            params,
            connections["bkdata_basic_slave"],
            dmaction.NEED_DATA_SET_ID_DETAIL,
            sankey_diagram_distribute=True,
        )
        dgraph_result = result_dict.get("dgraph_result")
        rt_count = dgraph_result.get("rt_count")[0].get("count") if dgraph_result.get("rt_count") else 0
        if rt_count == 0:
            return Response(
                {
                    "label": [],
                    "source": [],
                    "target": [],
                    "value": [],
                    "alias": [],
                    "other_app_code_list": [],
                    "level": 0,
                }
            )
        first_level_tag_list = result_dict.get("first_level_tag_list")
        second_level_tag_list = result_dict.get("second_level_tag_list")
        # 第一层label
        label = []
        alias_list = []
        for each_tag in first_level_tag_list:
            if each_tag.get("tag_code") and each_tag.get("tag_alias"):
                label.append(each_tag.get("tag_code"))
                alias_list.append(each_tag.get("tag_alias"))

        # 其他标签
        label.append("other")
        alias_list.append(_("其他"))
        source = []
        target = []
        value = []
        second_level_have_data_list = []
        for each_tag in second_level_tag_list:
            # 第二层label
            if each_tag.get("tag_code") and each_tag.get("tag_code") not in label:
                label.append(each_tag.get("tag_code"))
                alias_list.append(each_tag.get("tag_alias"))
            # 第一层和第二层之间的value
            if dgraph_result.get("c_%s" % each_tag.get("tag_code")) and dgraph_result.get(
                "c_%s" % each_tag.get("tag_code")
            )[0].get("count"):
                # 记录第二层有哪些节点有数据
                if each_tag.get("tag_code") not in second_level_have_data_list:
                    second_level_have_data_list.append(each_tag.get("tag_code"))

                source.append(label.index(each_tag.get("parent_code")))
                target.append(label.index(each_tag.get("tag_code")))
                value.append(dgraph_result.get("c_%s" % each_tag.get("tag_code"))[0].get("count"))
        processing_type_dict = copy.deepcopy(dmaction.processing_type_dict)
        processing_type_dict["batch_model"] = _("ModelFlow模型(batch)")
        processing_type_dict["stream_model"] = _("ModelFlow模型(stream)")
        fetch_value_between_source_target(
            second_level_tag_list,
            dgraph_result,
            processing_type_dict,
            label,
            alias_list,
            source,
            target,
            value,
        )
        other_app_code_list = []
        if level == 4:
            other_app_code_list, real_level = format_sankey_diagram(
                params,
                request,
                source,
                target,
                value,
                level,
                label,
                alias_list,
                processing_type_dict,
            )
        # 第二层存在的节点到第三层之间没有link，导致第二层的节点放置在第三层
        for each_tag in second_level_have_data_list:
            if each_tag in label and label.index(each_tag) not in source:
                for each_processing_type in dmaction.processing_type_list:
                    if each_processing_type in label:
                        source.append(label.index(each_tag))
                        target.append(label.index(each_processing_type))
                        value.append(minimal_value)
                        break
        # 将分类中"其他"放在第二层，即将"其他"前加输入
        if "other" in label:
            for each_tag in first_level_tag_list:
                if each_tag.get("tag_code") in label and label.index(each_tag.get("tag_code")) in source:
                    source.append(label.index(each_tag.get("tag_code")))
                    target.append(label.index("other"))
                    value.append(minimal_value)
                    break
        # 对应processing_type没有作为分类的输出节点,把"其他"作为该processing_type的输入
        for each_processing_type in dmaction.processing_type_list:
            if (
                each_processing_type in label
                and label.index(each_processing_type) not in target
                and "other" in label
                and (label.index("other") in source or label.index("other") in target)
            ):
                source.append(label.index("other"))
                target.append(label.index(each_processing_type))
                value.append(minimal_value)

        # 判断其他后面有无接processing_type
        # 在某些搜索条件"其他"没有数据的情况下，可能其他和第三层processing_type之间没有连接，导致其他变到最后一层
        if label.index("other") not in source:
            # other在source中的位置
            for each_processing_type in dmaction.processing_type_list:
                if each_processing_type in label and label.index(each_processing_type) in target:
                    source.append(label.index("other"))
                    target.append(label.index(each_processing_type))
                    value.append(minimal_value)
                    break

        return Response(
            {
                "label": label,
                "source": source,
                "target": target,
                "value": value,
                "alias": alias_list,
                "other_app_code_list": other_app_code_list,
                "level": real_level,
            }
        )


class TagTypeView(APIViewSet):
    @list_route(methods=["get"], url_path="info")
    def tag_type_info(self, request):
        """
        @api {get} /datamanage/datastocktake/tag_type/info/ 获取不同分类tag基本信息
        @apiVersion 0.1.0
        @apiGroup TagTypeView
        @apiName tag_type_info
        """
        sql = """select name, alias, description from tag_type_config limit 10"""
        tag_type_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        # 对tag_type_info描述作翻译
        for each_tag_type in tag_type_list:
            each_tag_type["alias"] = bktranslates(each_tag_type["alias"])
            each_tag_type["description"] = bktranslates(each_tag_type["description"])
        return Response(tag_type_list)


class NodeCountDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="distribution_filter")
    @params_valid(serializer=DataValueSerializer)
    def node_count_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/node_count/distribution_filter/ 后继节点分布情况
        @apiVersion 0.1.0
        @apiGroup NodeCountDistributionView
        @apiName node_count_distribution
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")

        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": []})

        num_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                num_list.append(each["node_count"])

        # num_list = [each['node_count'] for each in metric_list]
        x = [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "10-15",
            "15-20",
            "20-25",
            "25-30",
            "30-40",
            "40-50",
            "50-100",
            ">100",
        ]
        bins = [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            15,
            20,
            25,
            30,
            40,
            50,
            100,
            np.inf,
        ]
        score_cat = pd.cut(num_list, bins, right=False)
        bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
        sum_count = len(metric_list)
        z = [round(each / float(sum_count), 7) for each in bin_result_list]
        return Response({"x": x, "y": bin_result_list, "z": z})


class ScoreDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="range_filter")
    @params_valid(serializer=DataValueSerializer)
    def range_score_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/score_distribution/range_filter/ 广度评分分布
        @apiVersion 0.1.0
        @apiGroup ScoreDistributionView
        @apiName range_score_distribution
        @apiSuccess (输出) {String} data.y 评分，代表y轴数据
        @apiSuccess (输出) {String} data.x index
        @apiSuccess (输出) {String} data.perc 当前评分对应的数据占比
        @apiSuccess (输出) {String} data.cum_perc 评分>=当前评分的数据累计占比
        @apiSuccess (输出) {String} data.cnt 气泡大小
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "perc":[],
                    "cum_perc":[],
                    "cnt":[],
                    "y":[],
                    "x":[],
                    "z":[]
                },
                "result":true
            }
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")

        if (not metric_list) or platform == "tdw":
            return Response(
                {
                    "x": [],
                    "y": [],
                    "z": [],
                    "cnt": [],
                    "cum_perc": [],
                    "perc": [],
                    "80": 0,
                    "15": 0,
                }
            )
        # has_filter_cond 是否有过滤条件
        # filter_dataset_dict 过滤后的结果，没有符合条件的过滤结果和没有过滤条件都为{}
        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response(
                {
                    "x": [],
                    "y": [],
                    "z": [],
                    "cnt": [],
                    "cum_perc": [],
                    "perc": [],
                    "80": 0,
                    "15": 0,
                }
            )

        range_score_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                range_score_list.append(
                    {
                        "count": each["dataset_id"],
                        "score": each["normalized_range_score"],
                    }
                )

        res_dict = score_aggregate(range_score_list)

        return Response(res_dict)

    @list_route(methods=["post"], url_path="heat_filter")
    @params_valid(serializer=DataValueSerializer)
    def heat_score_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/score_distribution/heat_filter/ 热度评分分布
        @apiVersion 0.1.0
        @apiGroup ScoreDistributionView
        @apiName heat_score_distribution
        @apiSuccess (输出) {String} data.y 评分，代表y轴数据
        @apiSuccess (输出) {String} data.x index
        @apiSuccess (输出) {String} data.perc 当前评分对应的数据占比
        @apiSuccess (输出) {String} data.cum_perc 评分>=当前评分的数据累计占比
        @apiSuccess (输出) {String} data.cnt 气泡大小
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "perc":[],
                    "cum_perc":[],
                    "cnt":[],
                    "y":[],
                    "x":[],
                    "z":[]
                },
                "result":true
            }
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response(
                {
                    "x": [],
                    "y": [],
                    "z": [],
                    "cnt": [],
                    "cum_perc": [],
                    "perc": [],
                    "80": 0,
                    "15": 0,
                }
            )

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response(
                {
                    "x": [],
                    "y": [],
                    "z": [],
                    "cnt": [],
                    "cum_perc": [],
                    "perc": [],
                    "80": 0,
                    "15": 0,
                }
            )

        heat_score_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                heat_score_list.append({"count": each["dataset_id"], "score": each["heat_score"]})

        res_dict = score_aggregate(heat_score_list)
        return Response(res_dict)

    @list_route(methods=["post"], url_path=r"(?P<score_type>\w+)")
    @params_valid(serializer=DataValueSerializer)
    def score_distribution(self, request, score_type, params):
        """
        @api {post} /datamanage/datastocktake/score_distribution/:score_type/ 价值、收益比、热度、广度、重要度等评分分布气泡图
        @apiVersion 0.1.0
        @apiGroup Datastocktake/ScoreDistribution
        @apiName score_distribution
        @apiParam {String} score_type importance/heat/range/asset_value/assetvalue_to_cost
        @apiSuccess (输出) {String} data.y 评分，代表y轴数据
        @apiSuccess (输出) {String} data.x index
        @apiSuccess (输出) {String} data.z 当前评分对应的数据个数
        @apiSuccess (输出) {String} data.perc 当前评分对应的数据占比
        @apiSuccess (输出) {String} data.cum_perc 评分>=当前评分的数据累计占比
        @apiSuccess (输出) {String} data.cnt 气泡大小
        @apiParamExample {json} 参数样例:
        {
            "bk_biz_id":null,
            "project_id":null,
            "tag_ids":[

            ],
            "keyword":"",
            "tag_code":"virtual_data_mart",
            "me_type":"tag",
            "has_standard":1,
            "cal_type":[
                "standard"
            ],
            "data_set_type":"all",
            "page":1,
            "page_size":10,
            "platform":"bk_data",
        }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "perc":[],
                    "cum_perc":[],
                    "cnt":[],
                    "y":[],
                    "x":[],
                    "z":[]
                },
                "result":true
            }
        """
        # 从redis拿到生命周期相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": [], "cnt": [], "cum_perc": [], "perc": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": [], "cnt": [], "cum_perc": [], "perc": []})

        score_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                if each[SCORE_DICT[score_type]] >= 0:
                    score_list.append(
                        {
                            "count": each["dataset_id"],
                            "score": each[SCORE_DICT[score_type]],
                        }
                    )
        if not score_list:
            return Response({"x": [], "y": [], "z": [], "cnt": [], "cum_perc": [], "perc": []})
        res_dict = score_aggregate(score_list, score_type=score_type)
        return Response(res_dict)

    @list_route(methods=["post"], url_path=r"trend/(?P<score_type>\w+)")
    @params_valid(serializer=DataValueSerializer)
    def score_trend(self, request, score_type, params):
        """
        @api {post} /datamanage/datastocktake/score_distribution/trend/:score_type/ 价值、重要度等评分趋势
        @apiVersion 0.1.0
        @apiGroup Datastocktake/ScoreDistribution
        @apiName score_trend
        @apiParam {String} score_type 生命周期评分指标 importance/heat/range/asset_value/assetvalue_to_cost
        @apiParamExample {json} 参数样例:
            HTTP/1.1 http://{domain}/v3/datamanage/datastocktake/score_distribution/trend/importance/
            {
                "bk_biz_id":null,
                "project_id":null,
                "tag_ids":[

                ],
                "keyword":"",
                "tag_code":"virtual_data_mart",
                "me_type":"tag",
                "has_standard":1,
                "cal_type":[
                    "standard"
                ],
                "data_set_type":"all",
                "page":1,
                "page_size":10,
                "platform":"bk_data",
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "index": 0,
                        "num": [
                            10513,
                            10383,
                            10323,
                            10147,
                            10243,
                            10147,
                            10357,
                            10147
                        ],
                        "score_level": "[0,25]",
                        "time": [
                            "08-06",
                            "08-07",
                            "08-09",
                            "08-12",
                            "08-10",
                            "08-13",
                            "08-08",
                            "08-11"
                        ]
                    },
                    {
                        "index": 1,
                        "num": [
                            37118,
                            37177,
                            37222,
                            37349,
                            37279,
                            37349,
                            37203,
                            37349
                        ],
                        "score_level": "(25,50]",
                        "time": [
                            "08-06",
                            "08-07",
                            "08-09",
                            "08-12",
                            "08-10",
                            "08-13",
                            "08-08",
                            "08-11"
                        ]
                    },
                    {
                        "index": 2,
                        "num": [
                            37119,
                            37179,
                            37181,
                            37205,
                            37189,
                            37205,
                            37179,
                            37205
                        ],
                        "score_level": "(50,80]",
                        "time": [
                            "08-06",
                            "08-07",
                            "08-09",
                            "08-12",
                            "08-10",
                            "08-13",
                            "08-08",
                            "08-11"
                        ]
                    },
                    {
                        "index": 3,
                        "num": [
                            7691,
                            7702,
                            7715,
                            7740,
                            7730,
                            7740,
                            7702,
                            7740
                        ],
                        "score_level": "(80,100]",
                        "time": [
                            "08-06",
                            "08-07",
                            "08-09",
                            "08-12",
                            "08-10",
                            "08-13",
                            "08-08",
                            "08-11"
                        ]
                    }
                ],
                "result": true
            }
        """
        # 1）get data_trend_df from redis
        data_trend_df = cache.get("data_trend_df")
        platform = params.get("platform", "all")
        # data_trend_df
        if data_trend_df.empty or platform == "tdw":
            return Response([])

        # 2）判断是否有搜索条件 & 搜索命中的数据集
        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response([])

        # 3）评分趋势
        ret = score_trend_pandas_groupby(data_trend_df, filter_dataset_dict, score_type)
        return Response(ret)

    @list_route(methods=["post"], url_path=r"level_and_trend/(?P<score_type>\w+)")
    @params_valid(serializer=DataValueSerializer)
    def score_level_and_trend(self, request, score_type, params):
        """
        @api {post} /datamanage/datastocktake/score_distribution/level_and_trend/:score_type/ 价值、重要度等等级分布&评分趋势
        @apiVersion 0.1.0
        @apiGroup Datastocktake/ScoreDistribution
        @apiName score_level_and_trend
        @apiParam {String} score_type 生命周期评分指标 importance/heat/range/asset_value/assetvalue_to_cost
        @apiParamExample {json} 参数样例:
            HTTP/1.1 http://{domain}/v3/datamanage/datastocktake/score_distribution/level_and_trend/importance/
            {
                "bk_biz_id":null,
                "project_id":null,
                "tag_ids":[

                ],
                "keyword":"",
                "tag_code":"virtual_data_mart",
                "me_type":"tag",
                "has_standard":1,
                "cal_type":[
                    "standard"
                ],
                "data_set_type":"all",
                "page":1,
                "page_size":10,
                "platform":"bk_data",
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "score_trend":[
                        {
                            "index":0,
                            "num":[
                                10513,
                                10383,
                                10323,
                                10147,
                                10243,
                                10147,
                                10357
                            ],
                            "score_level":"[0,25]",
                            "time":[
                                "08-06",
                                "08-07",
                                "08-09",
                                "08-12",
                                "08-10",
                                "08-13",
                                "08-08"
                            ]
                        },
                        {
                            "index":1,
                            "num":[
                                37118,
                                37177,
                                37222,
                                37349,
                                37279,
                                37349,
                                37203
                            ],
                            "score_level":"(25,50]",
                            "time":[
                                "08-06",
                                "08-07",
                                "08-09",
                                "08-12",
                                "08-10",
                                "08-13",
                                "08-08"
                            ]
                        },
                        {
                            "index":2,
                            "num":[
                                37119,
                                37179,
                                37181,
                                37205,
                                37189,
                                37205,
                                37179
                            ],
                            "score_level":"(50,80]",
                            "time":[
                                "08-06",
                                "08-07",
                                "08-09",
                                "08-12",
                                "08-10",
                                "08-13",
                                "08-08"
                            ]
                        },
                        {
                            "index":3,
                            "num":[
                                7691,
                                7702,
                                7715,
                                7740,
                                7730,
                                7740,
                                7702
                            ],
                            "score_level":"(80,100]",
                            "time":[
                                "08-06",
                                "08-07",
                                "08-09",
                                "08-12",
                                "08-10",
                                "08-13",
                                "08-08"
                            ]
                        }
                    ],
                    "level_distribution":{
                        "y":[
                            245,
                            8010,
                            1887,
                            1657
                        ],
                        "x":[
                            "[0,10]",
                            "(10,50]",
                            "(50,75]",
                            "(75,100]"
                        ],
                        "sum_count":11799,
                        "z":[
                            0.0207645,
                            0.6788711,
                            0.1599288,
                            0.1404356
                        ]
                    }
                },
                "result":true
            }
        """
        metric_list = cache.get("data_value_stocktake")
        data_trend_df = cache.get("data_trend_df")

        platform = params.get("platform", "all")
        if (not metric_list and data_trend_df.empty) or platform == "tdw":
            return Response(
                {
                    "score_trend": [],
                    "level_distribution": {"x": [], "y": [], "z": [], "sum_count": 0},
                }
            )

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response(
                {
                    "score_trend": [],
                    "level_distribution": {"x": [], "y": [], "z": [], "sum_count": 0},
                }
            )

        # 1）评分等级分布
        if not metric_list:
            x = []
            bin_result_list = []
            z = []
            sum_count = 0
        else:
            x, bin_result_list, z, sum_count = level_distribution(metric_list, filter_dataset_dict, score_type)

        # 2）评分趋势
        if data_trend_df.empty:
            trend_ret_list = []
        else:
            trend_ret_list = score_trend_pandas_groupby(data_trend_df, filter_dataset_dict, score_type)
        return Response(
            {
                "score_trend": trend_ret_list,
                "level_distribution": {
                    "x": x,
                    "y": bin_result_list,
                    "z": z,
                    "sum_count": sum_count,
                },
            }
        )


class LevelDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path=r"(?P<score_type>\w+)")
    @params_valid(serializer=DataValueSerializer)
    def level_distribution(self, request, score_type, params):
        """
        @api {post} /datamanage/datastocktake/level_distribution/:score_type/ 价值、重要度等等级分布
        @apiVersion 0.1.0
        @apiGroup Datastocktake/LevelDistribution
        @apiName level_distribution
        @apiParam {String} score_type 生命周期评分指标 importance/heat/range/asset_value/assetvalue_to_cost
        @apiSuccess (输出) {String} data.x 等级区间
        @apiSuccess (输出) {String} data.y 数据个数
        @apiSuccess (输出) {String} data.z 数据占比
        @apiSuccess (输出) {String} data.sum_count 数据表总数
        @apiParamExample {json} 参数样例:
            HTTP/1.1 http://{domain}/v3/datamanage/datastocktake/level_distribution/importance/
            {
                "bk_biz_id":null,
                "project_id":null,
                "tag_ids":[

                ],
                "keyword":"",
                "tag_code":"virtual_data_mart",
                "me_type":"tag",
                "has_standard":1,
                "cal_type":[
                    "standard"
                ],
                "data_set_type":"all",
                "page":1,
                "page_size":10,
                "platform":"bk_data",
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "y": [
                        245,
                        8010,
                        1887,
                        1657
                    ],
                    "x": [
                        "[0,10]",
                        "(10,50]",
                        "(50,75]",
                        "(75,100]"
                    ],
                    "sum_count": 11799,
                    "z": [
                        0.0207645,
                        0.6788711,
                        0.1599288,
                        0.1404356
                    ]
                },
                "result": true
            }
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": [], "sum_count": 0})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": [], "sum_count": 0})

        x, bin_result_list, z, sum_count = level_distribution(metric_list, filter_dataset_dict, score_type)
        return Response({"x": x, "y": bin_result_list, "z": z, "sum_count": sum_count})


class CostDistribution(APIViewSet):
    @list_route(methods=["post"], url_path="storage_capacity")
    @params_valid(serializer=DataValueSerializer)
    def storage_capacity(self, request, params):
        """
        @api {post} /datamanage/datastocktake/cost_distribution/storage_capacity/ 存储分布
        @apiVersion 0.1.0
        @apiGroup Datastocktake/CostDistribution
        @apiName storage_capacity
        @apiSuccess (输出) {String} data.capacity_list 不同存储的大小
        @apiSuccess (输出) {String} data.label 不同存储的名称
        @apiSuccess (输出) {String} data.sum_capacity 总存储大小
        @apiSuccess (输出) {String} data.unit 存储单位，自适应单位
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "capacity_list": [],
                    "label": [],
                    "sum_capacity": 0,
                    "unit": "TB"
                },
                "result": true
            }
        """
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"capacity_list": [], "label": [], "sum_capacity": 0, "unit": "MB"})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"capacity_list": [], "label": [], "sum_capacity": 0, "unit": "MB"})

        hdfs_list = []
        tspider_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                hdfs_list.append(each["hdfs_capacity"])
                tspider_list.append(each["tspider_capacity"])

        sum_hdfs = sum(hdfs_list)
        sum_tspider = sum(tspider_list)
        sum_capacity = sum_hdfs + sum_tspider
        format_max_capacity, unit, power = hum_storage_unit(sum_capacity, return_unit=True)
        sum_hdfs = round(sum_hdfs / float(1024 ** power), 3)
        sum_tspider = round(sum_tspider / float(1024 ** power), 3)
        sum_capacity = round(sum_capacity / float(1024 ** power), 3)
        capacity_list = [sum_hdfs, sum_tspider]
        label = ["hdfs", "tspider"]
        return Response(
            {
                "capacity_list": capacity_list,
                "label": label,
                "sum_capacity": sum_capacity,
                "unit": unit,
            }
        )

    @list_route(methods=["post"], url_path="storage_trend")
    @params_valid(serializer=DataValueSerializer)
    def storage_trend(self, request, params):
        """
        @api {post} /datamanage/datastocktake/cost_distribution/storage_trend/ 存储成本趋势
        @apiVersion 0.1.0
        @apiGroup Datastocktake/CostDistribution
        @apiName storage_trend
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "tspider_capacity": [
                        23,
                        23,
                        23,
                        23,
                        23,
                        23
                    ],
                    "hdfs_capacity": [
                        1000,
                        1000,
                        1000,
                        1000,
                        1000,
                        1000
                    ],
                    "total_capacity": [
                        1023,
                        1023,
                        1023,
                        1023,
                        1023,
                        1023
                    ],
                    "unit": "TB",
                    "time": [
                        "08-13",
                        "08-14",
                        "08-15",
                        "08-16",
                        "08-17",
                        "08-18"
                    ]
                },
                "result": true
            }
        """
        data_trend_df = cache.get("data_trend_df")
        platform = params.get("platform", "all")
        if data_trend_df.empty or platform == "tdw":
            return Response(
                {
                    "tspider_capacity": [],
                    "hdfs_capacity": [],
                    "total_capacity": [],
                    "time": [],
                    "unit": "B",
                }
            )

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response(
                {
                    "tspider_capacity": [],
                    "hdfs_capacity": [],
                    "total_capacity": [],
                    "time": [],
                    "unit": "B",
                }
            )
        ret_dict = storage_capacity_trend(data_trend_df, filter_dataset_dict)
        return Response(ret_dict)


class ImportanceDistribution(APIViewSet):
    @list_route(methods=["post"], url_path=r"(?P<metric_type>\w+)")
    @params_valid(serializer=DataValueSerializer)
    def importance_metric_distribution(self, request, metric_type, params):
        """
        @api {post} /datamanage/datastocktake/importance_distribution/:metric_type/
        业务重要度、关联BIP、项目运营状态、数据敏感度分布、数据生成类型
        @apiVersion 0.1.0
        @apiGroup Datastocktake/ImportanceDistribution
        @apiName importance_metric_distribution
        @apiParam {String} metric_type 重要度相关指标 app_important_level_name/is_bip/active/sensitivity/generate_type
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":{
                    "dataset_count":[
                        10274,
                        1557
                    ],
                    "metric":[
                        false,
                        true
                    ],
                    "dataset_perct":[
                        0.8683966,
                        0.1316034
                    ],
                    "biz_count":[
                        317,
                        30
                    ],
                    "biz_perct":[
                        0.9135447,
                        0.0864553
                    ]
                },
                "result":true
            }
        """
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"metric": [], "dataset_count": [], "dataset_perct": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"metric": [], "dataset_count": [], "dataset_perct": []})

        importance_metric_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                if metric_type not in list(METRIC_DICT.keys()):
                    importance_metric_list.append({"dataset_id": each["dataset_id"], "metric": each[metric_type]})
                else:
                    importance_metric_list.append(
                        {
                            "dataset_id": each["dataset_id"],
                            "metric": each[metric_type],
                            METRIC_DICT[metric_type]: each[METRIC_DICT[metric_type]],
                        }
                    )

        df1 = DataFrame(importance_metric_list)
        df2 = df1.groupby("metric", as_index=False).count().sort_values(["metric"], axis=0, ascending=True)
        metric_count_agg_list = df2.to_dict(orient="records")
        metric = [each["metric"] for each in metric_count_agg_list]
        dataset_count = [each["dataset_id"] for each in metric_count_agg_list]
        metric_sum = len(metric_list)
        dataset_perct = [
            round(each["dataset_id"] / float(metric_sum), 7) if metric_sum else 0 for each in metric_count_agg_list
        ]

        if metric_type in list(METRIC_DICT.keys()):
            df3 = (
                df1.groupby(["metric", METRIC_DICT[metric_type]], as_index=False)
                .count()
                .sort_values(["metric"], axis=0, ascending=True)
            )
            df4 = df3.groupby("metric", as_index=False).count().sort_values(["metric"], axis=0, ascending=True)
            count_agg_list = df4.to_dict(orient="records")
            count = [each["dataset_id"] for each in count_agg_list]
            count_sum = sum(count)
            perct = [round(each["dataset_id"] / float(count_sum), 7) if count_sum else 0 for each in count_agg_list]
            if METRIC_DICT[metric_type] == "bk_biz_id":
                return Response(
                    {
                        "metric": metric,
                        "dataset_count": dataset_count,
                        "dataset_perct": dataset_perct,
                        "biz_count": count,
                        "biz_perct": perct,
                    }
                )
            else:
                return Response(
                    {
                        "metric": metric,
                        "dataset_count": dataset_count,
                        "dataset_perct": dataset_perct,
                        "project_count": count,
                        "project_perct": perct,
                    }
                )
        return Response(
            {
                "metric": metric,
                "dataset_count": dataset_count,
                "dataset_perct": dataset_perct,
            }
        )

    @list_route(methods=["post"], url_path="biz")
    @params_valid(serializer=DataValueSerializer)
    def bip_grade_and_oper_state_distr(self, request, params):
        """
        @api {post} /datamanage/datastocktake/importance_distribution/biz/ 业务星际&运营状态分布
        @apiVersion 0.1.0
        @apiGroup Datastocktake/ImportanceDistribution
        @apiName bip_grade_and_oper_state_distribution
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "dataset_count": [
                        148
                    ],
                    "oper_state_name": [
                        "不删档"
                    ],
                    "biz_count": [
                        6
                    ],
                    "bip_grade_name": [
                        "三星"
                    ]
                },
                "result": true
            }
        """
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response(
                {
                    "dataset_count": [],
                    "oper_state_name": [],
                    "biz_count": [],
                    "bip_grade_name": [],
                }
            )

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response(
                {
                    "dataset_count": [],
                    "oper_state_name": [],
                    "biz_count": [],
                    "bip_grade_name": [],
                }
            )

        biz_metric_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                bip_grade_name = (
                    each["bip_grade_name"]
                    if each["bip_grade_name"] not in ABN_BIP_GRADE_NAME_LIST
                    else CORR_BIP_GRADE_NAME
                )
                biz_metric_list.append(
                    {
                        "dataset_id": each["dataset_id"],
                        "oper_state_name": each["oper_state_name"],
                        "bip_grade_name": bip_grade_name,
                        "bk_biz_id": each["bk_biz_id"],
                    }
                )
        df1 = DataFrame(biz_metric_list)
        # 按照oper_state_name和bip_grade_name聚合，按照bip_grade_name排序
        df2 = df1.groupby(["oper_state_name", "bip_grade_name"], as_index=False).count()
        # 自定义排序顺序,按照bip_grade_name和oper_state_name排序
        df2["oper_state_name"] = df2["oper_state_name"].astype("category").cat.set_categories(OPER_STATE_NAME_ORDER)
        df2["bip_grade_name"] = df2["bip_grade_name"].astype("category").cat.set_categories(BIP_GRADE_NAME_ORDER)
        df2 = df2.dropna()
        df2 = df2.sort_values(by=["oper_state_name", "bip_grade_name"], ascending=True)

        metric_count_agg_list = df2.to_dict(orient="records")
        oper_state_name = [each["oper_state_name"] for each in metric_count_agg_list]
        bip_grade_name = [each["bip_grade_name"] for each in metric_count_agg_list]
        dataset_count = [each["dataset_id"] for each in metric_count_agg_list]
        df3 = df1.groupby(["oper_state_name", "bip_grade_name", "bk_biz_id"], as_index=False).count()
        df4 = df3.groupby(["oper_state_name", "bip_grade_name"], as_index=False).count()
        # 自定义排序顺序,按照bip_grade_name和oper_state_name排序
        df4["oper_state_name"] = df4["oper_state_name"].astype("category").cat.set_categories(OPER_STATE_NAME_ORDER)
        df4["bip_grade_name"] = df4["bip_grade_name"].astype("category").cat.set_categories(BIP_GRADE_NAME_ORDER)
        df4 = df4.dropna()
        df4 = df4.sort_values(by=["oper_state_name", "bip_grade_name"], ascending=True)
        count_agg_list = df4.to_dict(orient="records")
        biz_count = [each["dataset_id"] for each in count_agg_list]
        return Response(
            {
                "oper_state_name": oper_state_name,
                "bip_grade_name": bip_grade_name,
                "dataset_count": dataset_count,
                "biz_count": biz_count,
            }
        )


class QueryCountDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="distribution_filter")
    @params_valid(serializer=DataValueSerializer)
    def day_query_count_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/day_query_count/distribution_filter/ 每日查询次数分布情况
        @apiVersion 0.1.0
        @apiGroup QueryCountDistributionView
        @apiName day_query_count_distribution
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": []})

        num_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                num_list.append(int(each["query_count"] / 7.0))

        # num_list = [int(each['query_count']/7.0) for each in metric_list]
        x = [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "5-10",
            "10-20",
            "20-30",
            "30-40",
            "40-50",
            "50-100",
            "100-500",
            "500-1000",
            "1000-5000",
            ">5000",
        ]
        bins = [0, 1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 500, 1000, 5000, np.inf]
        score_cat = pd.cut(num_list, bins, right=False)
        bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
        sum_count = len(metric_list)
        z = [round(each / float(sum_count), 7) for each in bin_result_list]
        return Response({"x": x, "y": bin_result_list, "z": z})


class ApplicationDistributionView(APIViewSet):
    @list_route(methods=["post"], url_path="biz_count_distribution_filter")
    @params_valid(serializer=DataValueSerializer)
    def biz_count_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/application/biz_count_distribution_filter/
        @apiVersion 0.1.0
        @apiGroup ApplicationDistributionView
        @apiName biz_count_distribution
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": []})

        biz_count_tmp_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                biz_count_tmp_list.append(
                    {
                        "dataset_id": each["dataset_id"],
                        "biz_count": each["biz_count"] if each["biz_count"] else 1,
                    }
                )

        df1 = DataFrame(biz_count_tmp_list)
        df2 = df1.groupby("biz_count", as_index=False).count().sort_values(["biz_count"], axis=0, ascending=True)
        biz_count_agg_list = df2.to_dict(orient="records")
        x = [each["biz_count"] for each in biz_count_agg_list]
        y = [each["dataset_id"] for each in biz_count_agg_list]
        sum = len(metric_list)
        z = [round(each["dataset_id"] / float(sum), 7) if sum else 0 for each in biz_count_agg_list]

        return Response({"x": x, "y": y, "z": z})

    @list_route(methods=["post"], url_path="project_count_distribution_filter")
    @params_valid(serializer=DataValueSerializer)
    def project_count_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/application/project_count_distribution_filter/
        @apiVersion 0.1.0
        @apiGroup ApplicationDistributionView
        @apiName project_count_distribution
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": []})

        project_count_tmp_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                project_count_tmp_list.append(
                    {
                        "dataset_id": each["dataset_id"],
                        "project_count": each["project_count"] if each["project_count"] else 1,
                    }
                )

        df1 = DataFrame(project_count_tmp_list)
        df2 = (
            df1.groupby("project_count", as_index=False).count().sort_values(["project_count"], axis=0, ascending=True)
        )
        project_count_agg_list = df2.to_dict(orient="records")
        x = [each["project_count"] for each in project_count_agg_list]
        y = [each["dataset_id"] for each in project_count_agg_list]
        sum = len(metric_list)
        z = [round(each["dataset_id"] / float(sum), 7) if sum else 0 for each in project_count_agg_list]

        return Response({"x": x, "y": y, "z": z})

    @list_route(methods=["post"], url_path="app_code_count_distribution_filter")
    @params_valid(serializer=DataValueSerializer)
    def app_code_count_distribution_filter(self, request, params):
        """
        @api {get} /datamanage/datastocktake/application/app_code_count_distribution_filter/
        @apiVersion 0.1.0
        @apiGroup ApplicationDistributionView
        @apiName app_code_count_distribution
        """
        # 从redis拿到热度、广度相关明细数据
        metric_list = cache.get("data_value_stocktake")
        platform = params.get("platform", "all")
        if (not metric_list) or platform == "tdw":
            return Response({"x": [], "y": [], "z": []})

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        if has_filter_cond and not filter_dataset_dict:
            return Response({"x": [], "y": [], "z": []})

        app_code_count_tmp_list = []
        for each in metric_list:
            if not filter_dataset_dict or each["dataset_id"] in filter_dataset_dict:
                app_code_count_tmp_list.append(
                    {
                        "dataset_id": each["dataset_id"],
                        "app_code_count": each["app_code_count"] if each["app_code_count"] else 0,
                    }
                )

        df1 = DataFrame(app_code_count_tmp_list)
        df2 = (
            df1.groupby("app_code_count", as_index=False)
            .count()
            .sort_values(["app_code_count"], axis=0, ascending=True)
        )
        app_code_count_agg_list = df2.to_dict(orient="records")
        x = []
        y = []
        z = []
        sum = len(metric_list)
        for each in app_code_count_agg_list:
            x.append(each["app_code_count"])
            y.append(each["dataset_id"])
            z.append(round(each["dataset_id"] / float(sum), 7) if sum else 0)

        return Response({"x": x, "y": y, "z": z})
