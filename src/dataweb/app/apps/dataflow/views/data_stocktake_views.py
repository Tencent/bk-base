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

import json

from rest_framework.response import Response

from apps.api import DataManageApi
from apps.common.views import list_route
from apps.dataflow.views.data_map_views import datamap_cache
from apps.generic import APIViewSet


class DataStockTakeViewSet(APIViewSet):
    @datamap_cache
    def _overall_statistic(self, request, result_table_id=None):
        request_params_overall = {
            "bk_biz_id": None,
            "project_id": None,
            "tag_ids": [],
            "keyword": "",
            "tag_code": "virtual_data_mart",
            "me_type": "tag",
            "has_standard": 1,
            "cal_type": ["standard"],
            "data_set_type": "all",
            "page": 1,
            "page_size": 10,
            "platform": "bk_data",
            "order_time": None,
            "order_heat": None,
            "order_range": None,
        }
        # 1 整体数据统计
        search_res_overall = DataManageApi.get_data_dict_count(request_params_overall)
        request_params = json.loads(request.body)
        username = request.user.username
        # 按照"我的"进行过滤
        if request_params.get("is_filterd_by_created_by", False):
            request_params["created_by"] = username
        search_res = DataManageApi.get_data_dict_count(request_params)
        # 数据源比例
        search_res["data_source_perct"] = round(
            search_res.get("data_source_count") / float(search_res_overall.get("data_source_count")), 4
        )
        search_res["data_source_perct"] = search_res["data_source_perct"] if search_res["data_source_perct"] <= 1 else 1
        # 结果表比例
        search_res["dataset_perct"] = round(
            search_res.get("dataset_count") / float(search_res_overall.get("dataset_count")), 4
        )
        search_res["dataset_perct"] = search_res["dataset_perct"] if search_res["dataset_perct"] <= 1 else 1
        # 业务比例
        search_res["bk_biz_perct"] = round(
            search_res.get("bk_biz_count") / float(search_res_overall.get("bk_biz_count")), 4
        )
        search_res["bk_biz_perct"] = search_res["bk_biz_perct"] if search_res["bk_biz_perct"] <= 1 else 1
        # 项目比例
        search_res["project_perct"] = round(
            search_res.get("project_count") / float(search_res_overall.get("project_count")), 4
        )
        search_res["project_perct"] = search_res["project_perct"] if search_res["project_perct"] <= 1 else 1
        return search_res

    @list_route(methods=["post"], url_path="overall_statistic")
    def overall_statistic(self, request):
        """
        @api {post} /datamart/datastocktake/overall_statistic/ 获取数据字典页面的整体统计情况
        @apiName overall_statistic
        @apiGroup DataSetListViewSet
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
        """
        return Response(self._overall_statistic(request))
