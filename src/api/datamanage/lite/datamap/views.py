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

from datamanage.lite.datamap import dmaction
from datamanage.lite.tag import tagaction
from django.core.cache import cache
from django.db import connections
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.decorators import list_route
from common.views import APIViewSet


def add_sub_list(
    sub_list,
    tag_category_dict,
    result_obj,
    de_weight_tag_id_list,
    de_weight_result_list,
):
    for sub_obj in sub_list:  # sub_obj如等于登入登出
        tag_category_dict[sub_obj["tag_id"]] = result_obj
        sub_result_list = []
        sub_sub_list = sub_obj["sub_list"]  # 第二层,如业务安全,如果非空,把该节点以下sub_list的全部平铺提到第二层
        sub_result_list.extend(sub_sub_list)
        tile_list = []  # 平铺后的结果
        if sub_sub_list:
            for sub_sub_obj in sub_sub_list:
                dmaction.tile_all_sub_leaf(sub_sub_obj, tile_list)

        tile_tag_alias_dict = {}  # 中文名为key
        need_remove_tile_list = []  # 记录要删掉的标签
        for tile_dict in tile_list:  # 按中文名去重
            tile_tag_alias = tile_dict["tag_alias"]
            tile_tag_code = tile_dict["tag_code"]
            if tile_tag_alias in tile_tag_alias_dict:
                tile_tag_alias_dict[tile_tag_alias]["tag_code"] += "|" + tile_tag_code
                need_remove_tile_list.append(tile_dict)
            else:
                tile_tag_alias_dict[tile_tag_alias] = tile_dict

        if need_remove_tile_list:
            for remove_dict in need_remove_tile_list:
                tile_list.remove(remove_dict)

        sub_result_list.extend(tile_list)

        final_sub_result_list = []
        for tmp_obj in sub_result_list:  # 去重
            tag_category_dict[tmp_obj["tag_id"]] = sub_obj
            if tmp_obj["tag_id"] not in de_weight_tag_id_list:
                final_sub_result_list.append(tmp_obj)

        sub_obj["sub_list"] = final_sub_result_list

    for sub_obj in sub_list:
        if sub_obj["sub_list"] or sub_obj["tag_id"] not in de_weight_tag_id_list:
            de_weight_result_list.append(sub_obj)


def tag_sort_count_list_add_addition(result_list):
    for result_obj in result_list:
        sub_top_list = result_obj["sub_top_list"]
        de_weight_tag_id_list = []
        for tmp_top_obj in sub_top_list:
            de_weight_tag_id_list.append(tmp_top_obj["tag_id"])
        de_weight_result_list = []
        sub_list = result_obj["sub_list"]  # 第一层,如安全
        tag_category_dict = {}  # 标签与分类的对应关系
        if sub_list:
            add_sub_list(
                sub_list,
                tag_category_dict,
                result_obj,
                de_weight_tag_id_list,
                de_weight_result_list,
            )
        sub_top_tag_alias_dict = {}  # 中文名为key,为了对top的标签进行根据中文名称过滤重复
        need_remove_sub_top_list = []  # 记录要删除的标签
        for tmp_top_obj in sub_top_list:  # 把top数据加上具体的分类
            top_tag_code = tmp_top_obj["tag_code"]
            top_tag_alias = tmp_top_obj["tag_alias"]
            if top_tag_alias in sub_top_tag_alias_dict:
                exist_tag_dict = sub_top_tag_alias_dict[top_tag_alias]
                exist_tag_code = exist_tag_dict["tag_code"]
                if top_tag_code + "|" not in exist_tag_code and "|" + top_tag_code not in exist_tag_code:
                    exist_tag_dict["tag_code"] += "|" + top_tag_code
                need_remove_sub_top_list.append(tmp_top_obj)
            else:
                sub_top_tag_alias_dict[top_tag_alias] = tmp_top_obj

            belong_to_category = tag_category_dict.get(tmp_top_obj["tag_id"])
            if belong_to_category:
                tmp_top_obj["be_category_id"] = belong_to_category["tag_id"]
                tmp_top_obj["be_category_code"] = belong_to_category["tag_code"]
                tmp_top_obj["be_category_alias"] = belong_to_category["tag_alias"]

        if need_remove_sub_top_list:
            for tmp_top_obj1 in need_remove_sub_top_list:
                sub_top_list.remove(tmp_top_obj1)

        result_obj["sub_list"] = de_weight_result_list


def add_overall_topn_shown(result2_list, overall_top):
    # 如果要展示所有标签中对应数据集最多的top_n
    # 按照me_dataset_count大小进行排序
    data_type_id = None
    for each_tag in result2_list:
        if each_tag.get("tag_code", "") == "data_type":
            # 正式环境数据类型的tag_id为366
            data_type_id = each_tag.get("tag_id", 366)
            break
    tag_include_business_system_desc = [
        each_tag
        for each_tag in result2_list
        if each_tag.get("tag_type", "") in ["business", "system"]
        or (each_tag.get("tag_type", "") == "desc" and each_tag.get("parent_id", -1) == data_type_id)
    ]
    tag_include_business_system_desc.sort(key=lambda k: (k.get("me_dataset_count", 0)), reverse=True)
    overall_tag_topn = tag_include_business_system_desc[:overall_top]
    return overall_tag_topn


def add_result_list1(result1_list, top, result_list):
    """"""
    for tag1_obj in result1_list[:3]:  # 取前3
        sub_list = tag1_obj["sub_list"]
        sort_result_list = []  # 平铺节点下的所有子节点
        dmaction.handler_sub_leaf(tag1_obj, sort_result_list)
        sort_result_list.sort(key=lambda l: (l["me_dataset_count"]), reverse=True)
        sub_top_list = sort_result_list[:top]
        tag1_obj["sub_top_list"] = sub_top_list
        sub_list.sort(key=lambda l: (l["dataset_count"]), reverse=True)
        result_list.append(tag1_obj)

    sort_result4_list = []  # 平铺节点下的所有子节点
    other_obj = {}
    for tag4_obj in result1_list[3:]:
        dmaction.handler_sub_leaf(tag4_obj, sort_result4_list)
    sort_result4_list.sort(key=lambda l: (l["me_dataset_count"]), reverse=True)
    other_obj["sub_top_list"] = sort_result4_list[:top]
    other_obj["sub_list"] = result1_list[3:]
    other_obj["tag_id"] = -1
    other_obj["tag_alias"] = dmaction.OTHER_STR
    other_obj["dataset_count"] = 0
    other_obj["parent_id"] = 2
    other_obj["tag_type"] = "datamap_other"
    other_obj["seq_index"] = 4
    other_obj["tag_code"] = "datamap_other"
    result_list.append(other_obj)  # 其他标签


def add_result_list3(result3_list, top, result_list):
    sort_result5_list = []
    source_obj = {}
    for tag4_obj in result3_list:
        dmaction.handler_sub_leaf(tag4_obj, sort_result5_list)
    sort_result5_list.sort(key=lambda l: (l["me_dataset_count"]), reverse=True)
    source_obj["sub_top_list"] = sort_result5_list[:top]
    source_obj["sub_list"] = result3_list
    source_obj["tag_id"] = -2
    source_obj["tag_alias"] = _("数据来源")
    source_obj["dataset_count"] = 0
    source_obj["parent_id"] = 2
    source_obj["tag_type"] = "datamap_source"
    source_obj["seq_index"] = 5
    source_obj["tag_code"] = "datamap_source"
    result_list.append(source_obj)


def add_result_list4(result4_list, top, result_list):
    """"""
    sort_result5_list = []
    source_obj = {}
    for tag4_obj in result4_list:
        dmaction.handler_sub_leaf(tag4_obj, sort_result5_list)
    sort_result5_list.sort(key=lambda l: (l["me_dataset_count"]), reverse=True)
    source_obj["sub_top_list"] = sort_result5_list[:top]
    source_obj["sub_list"] = result4_list
    source_obj["tag_id"] = -3
    source_obj["tag_alias"] = _("数据类型")
    source_obj["dataset_count"] = 0
    source_obj["parent_id"] = 2
    source_obj["tag_type"] = "data_type"
    source_obj["seq_index"] = 6
    source_obj["tag_code"] = "data_type"
    result_list.append(source_obj)


def add_dgraph_result(result2_list):
    """"""
    rt_bk_biz_id_cond = dmaction.dgraph_exclude_bk_biz_id_cond("ResultTable")
    rd_bk_biz_id_cond = dmaction.dgraph_exclude_bk_biz_id_cond("AccessRawData")
    tdw_bk_biz_id_cond = dmaction.dgraph_exclude_bk_biz_id_cond("TdwTable")
    d_query_statement = (
        "{"
        + """
                 supported_entitys as var(func:has(~Tag.targets)) @filter({} or {} or {})
                """.format(
            rt_bk_biz_id_cond, rd_bk_biz_id_cond, tdw_bk_biz_id_cond
        )
    )
    for tag_dict in result2_list:
        tag_code = tag_dict["tag_code"]
        d_query_statement += dmaction.get_single_tag_query(tag_code, "supported_entitys", need_me_count=True)
    d_query_statement += "\n}"
    dgraph_result = dmaction.meta_dgraph_complex_search(d_query_statement)
    if dgraph_result:
        cache.set(dmaction.DGRAPH_CACHE_KEY, dgraph_result, dmaction.DGRAPH_TIME_OUT)


def origin_tag(exclude_source):
    sql3 = """select a.id tag_id,a.code tag_code,a.alias tag_alias,a.parent_id,a.tag_type,a.seq_index from tag a
            where a.active=1 and a.code in(
            select code from tag where parent_id=0 and tag_type='system' and active=1)
            and a.tag_type='system' $exclude_source_cond
            union all
            select a.id tag_id,a.code tag_code,a.alias tag_alias,a.parent_id,a.tag_type,a.seq_index from tag a
            where a.active=1 and a.code in(
            select code from tag where code='data_type' and tag_type='desc' and active=1)  and a.tag_type='desc'"""

    if exclude_source == "1":
        sql3 = sql3.replace("$exclude_source_cond", " and a.code not in('sys_host','components')")
    else:
        sql3 = sql3.replace("$exclude_source_cond", "")

    result5_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql3)

    result3_list, result4_list = [], []
    for res5_dict in result5_list:
        res5_tag_type = res5_dict["tag_type"]
        if res5_tag_type == "system":
            result3_list.append(res5_dict)
        elif res5_tag_type == "desc":
            result4_list.append(res5_dict)
    return result3_list, result4_list


def get_biz_tag():
    sql1 = """select a.id tag_id,a.code tag_code,a.alias tag_alias,a.parent_id,a.tag_type,a.seq_index from tag a
            where a.active=1 and a.code in(select code from tag where parent_id in (
            select id from tag where parent_id=(select id from tag where code='metric_domain' and active=1)
            and active=1) and active=1) and a.tag_type='business'
            """
    result1_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql1)

    sql2 = """select a.id tag_id,a.code tag_code,a.alias tag_alias,a.parent_id,a.seq_index,a.tag_type,a.description,
            0 as dataset_count, 0 as me_dataset_count from tag a where a.active=1"""
    result2_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql2)
    return result1_list, result2_list


class DataMapViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="get_tag_sort_count")
    def get_tag_sort_count(self, request):
        """
        @api {get} /datamanage/datamap/retrieve/get_tag_sort_count/ 标签按数据集个数排序的接口
        @apiVersion 0.1.0
        @apiGroup DataMap
        @apiName get_tag_sort_count

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [{
                    "sub_list": [ {
                    "parent_id": 26,
                    "tag_code": "game_user",
                    "tag_alias": "游戏用户",
                    "tag_id": 28,
                    "dataset_count": 0,
                    "sub_list": [
                        {
                            "parent_id": 37,
                            "tag_code": "online",
                            "tag_alias": "在线",
                            "tag_id": 96,
                            "dataset_count": 3709,
                            "tag_type": "business"
                        }

                    ],
                    "tag_type": "business"
                        }]
                    "dataset_count": 0,
                    "tag_code": "game",
                    "tag_alias": "游戏",
                    "tag_id": 26,
                    "sub_top_list": [
                        {
                            "parent_id": 37,
                            "tag_code": "online",
                            "tag_alias": "在线",
                            "tag_id": 96,
                            "dataset_count": 3709,
                            "tag_type": "business"
                        }
                    ],
                    "parent_id": 2,
                    "seq_index": 1,
                    "tag_type": "business"
                }]
            }
        """
        is_overall_topn_shown = request.query_params.get("is_overall_topn_shown")
        if is_overall_topn_shown:
            is_overall_topn_shown = int(is_overall_topn_shown)
        overall_top = request.query_params.get("overall_top")
        if overall_top:
            overall_top = int(overall_top)
        top = request.query_params.get("top")
        top = int(top)
        exclude_source = request.query_params.get("exclude_source")

        # 首先筛选出来需要展示的业务标签:
        result1_list, result2_list = get_biz_tag()
        dgraph_result = cache.get(dmaction.DGRAPH_CACHE_KEY)
        if not dgraph_result:
            add_dgraph_result(result2_list)
        for tag_dict in result2_list:
            tag_code = tag_dict["tag_code"]
            d_dataset_count = dgraph_result.get("c_" + tag_code, 0)
            d_me_dataset_count = dgraph_result.get("me_" + tag_code, 0)
            tag_dict["dataset_count"] = d_dataset_count
            tag_dict["me_dataset_count"] = d_me_dataset_count
        overall_tag_topn = []
        if is_overall_topn_shown:
            overall_tag_topn = add_overall_topn_shown(result2_list, overall_top)
        # 来源标签
        result3_list, result4_list = origin_tag(exclude_source)

        result_list = []  # 返回的最终结果
        tree_list = dmaction.build_tree(result2_list, "tag_id")  # 一棵完整的标签树

        result1_list = dmaction.overall_handler(result1_list, tree_list, True, "tag_id")

        game_dict = None  # 因为我们是游戏部门,所以把游戏分类下的第一级提升层级来直接排
        for idx in range(len(result1_list) - 1, -1, -1):
            tag1_obj = result1_list[idx]
            tag_code = tag1_obj["tag_code"]
            if tag_code == "game":
                game_dict = tag1_obj
                del result1_list[idx]
                break

        if game_dict:
            game_sub_list = game_dict["sub_list"]
            result1_list.extend(game_sub_list)
            # 把游戏分类的第一层直接放在了最后,所以需要重新排序
            result1_list.sort(key=lambda l: (l["dataset_count"]), reverse=True)

        if result1_list:
            add_result_list1(result1_list, top, result_list)
        result3_list = dmaction.overall_handler(result3_list, tree_list, False, "tag_id")
        if result3_list:  # 来源标签
            add_result_list3(result3_list, top, result_list)
        result4_list = dmaction.overall_handler(result4_list, tree_list, False, "tag_id")  # 数据类型
        if result4_list:
            add_result_list4(result4_list, top, result_list)
        # 最后做平铺子节点及去重
        if result_list:
            tag_sort_count_list_add_addition(result_list)
        return Response({"tag_list": result_list, "overall_top_tag_list": overall_tag_topn})
