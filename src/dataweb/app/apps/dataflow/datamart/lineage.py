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

from django.utils.translation import activate
from django.utils.translation import ugettext as _

from apps.api import AccessApi, MetaApi
from apps.dataflow.datamart.datadict import (
    format_tag_list,
    get_sort_tag_list,
    processing_type_dict,
    utc_to_local,
)
from apps.dataflow.handlers.business import Business
from apps.utils.local import activate_request

all_biz_dict = None


def substr(qualified_name):
    """
    去掉字符串前的节点类型
    :param qualified_name: 'result_table_591_durant1115'
    :return:'591_durant1115'
    """
    type_list = ["result_table_", "data_processing_", "raw_data_"]
    for each_type in type_list:
        if each_type in qualified_name:
            qualified_name = qualified_name.replace(each_type, "")
            break
    return qualified_name


def filter_dp(search_dict):
    """
    过滤数据处理节点
    :param search_dict: 血缘接口返回内容
    {
        "depth":1,
        "direction":"BOTH",
        "nodes":{
            "data_processing_125_adsad":{},
            "raw_data_100600":{},
            "result_table_125_adsad":{}
        },
        "relations":[
            {
                "status":"ACTIVE",
                "to":"data_processing_125_adsad",
                "from":"raw_data_100600"
            },
            {
                "status":"ACTIVE",
                "to":"result_table_125_adsad",
                "from":"data_processing_125_adsad"
            }
        ]
    }
    :return:血缘接口过滤掉dp节点和dp节点所在的边
    {
        "depth":1,
        "direction":"BOTH",
        "nodes":{
            "raw_data_100600":{},
            "result_table_125_adsad":{}
        },
        "relations":[
            {
                "status":"ACTIVE",
                "to":"result_table_125_adsad",
                "from":"raw_data_100600"
            }
        ]
    }
    """
    topo_dict = get_lineage_topo_dict(search_dict.get("relations", []))

    keys = list(search_dict.get("nodes", {}).keys())
    for key in keys:
        value = search_dict["nodes"][key]
        if value["type"] == "data_processing":
            del search_dict.get("nodes", {})[key]
            children = topo_dict.get(key, {}).get("children")
            parents = topo_dict.get(key, {}).get("parents")

            from_relation = topo_dict.get(key, {}).get("from_relation", [])
            to_relation = topo_dict.get(key, {}).get("to_relation", [])

            for relation in from_relation:
                if relation in search_dict.get("relations", []):
                    search_dict.get("relations", []).remove(relation)
            for relation in to_relation:
                if relation in search_dict.get("relations", []):
                    search_dict.get("relations", []).remove(relation)

            if children and parents:
                for each_child in children:
                    for each_parent in parents:
                        search_dict.get("relations", []).append(
                            {"status": "ACTIVE", "to": each_child, "from": each_parent}
                        )


def get_lineage_topo_dict(relations_list):
    """
    记录每一个节点的parents、children、指向该节点的线和指出该节点的线
    :param relations_list: 血缘接口返回的边
    :return:
    """
    topo_dict = {}
    for each_relation in relations_list:
        child = each_relation["to"]
        parent = each_relation["from"]
        if child not in topo_dict:
            topo_dict[child] = {"from_relation": [], "to_relation": [], "parents": [], "children": []}
        if parent not in topo_dict:
            topo_dict[parent] = {"from_relation": [], "to_relation": [], "parents": [], "children": []}
        # 记录箭头指向某一节点的线
        topo_dict[child]["to_relation"].append(each_relation)
        topo_dict[child]["parents"].append(parent)
        # 记录箭头指出某一节点的线
        topo_dict[parent]["from_relation"].append(each_relation)
        topo_dict[parent]["children"].append(child)
    return topo_dict


def add_rt_attr(each_rt):
    """
    添加result_table tooltip属性
    :param each_rt: dict
    :return:
    """
    global all_biz_dict
    if not all_biz_dict:
        all_biz_dict = Business.get_name_dict()
    time_unit_map = {
        "s": _("秒"),
        "m": _("分钟"),
        "H": _("小时"),
        "d": _("天"),
        "w": _("周"),
        "M": _("月"),
        "O": "O",
        "R": "R",
        "S": _("秒"),
        "h": _("小时"),
        "D": _("天"),
        "W": _("周"),
        "o": "O",
        "r": "R",
    }
    each_rt["project_name"] = each_rt["project"][0]["project_name"] if each_rt.get("project") else ""
    each_rt["type"] = "result_table"
    each_rt["name"] = each_rt.get("result_table_id")
    each_rt["id"] = "{}_{}".format(each_rt["type"], each_rt.get("result_table_id"))
    each_rt["alias"] = each_rt.get("result_table_name_alias", "")
    each_rt["bk_biz_name"] = all_biz_dict.get(each_rt.get("bk_biz_id"), "")
    each_rt["count_freq_unit_alias"] = (
        time_unit_map.get(each_rt.get("count_freq_unit"))
        if time_unit_map.get(each_rt.get("count_freq_unit"))
        else each_rt.get("count_freq_unit")
    )
    each_rt["processing_type_alias"] = (
        processing_type_dict.get(each_rt.get("processing_type"))
        if each_rt.get("processing_type") not in ["storage", "view"]
        else ""
    )
    # each_rt['updated_at'] = each_rt.get('updated_at', '').replace('+08:00', '').replace('T', ' ').replace('Z', ' ')
    each_rt["updated_at"] = utc_to_local(each_rt.get("updated_at", ""))
    return each_rt


def async_add_dp_attr(item, request):
    """
    并发给数据处理节点添加属性
    :param item: dict
    :return:
    """
    # 激活除了主线程以外的request
    activate_request(request)
    add_dp_attr(item, request)


def add_dp_attr(item):
    """
    给数据处理节点添加属性
    :param item:dict
    :return:
    """
    # 处理类型字典表
    item["processing_type_alias"] = processing_type_dict.get(item.get("processing_type"), "")
    item["project_name"] = item["project"][0].get("project_name", "") if item.get("project", []) else ""
    item["project_id"] = item["project"][0].get("project_id", "") if item.get("project", []) else ""
    item["type"] = "data_processing"
    item["id"] = "{}_{}".format(item["type"], item["processing_id"])
    item["name"] = item["processing_id"]
    return item


def async_add_rawdata_attr(item, request, language):
    """
    并发给数据源节点添加tooltip属性
    :param item:dict
    :return:
    """
    # 激活除了主线程以外的request
    activate_request(request)
    activate(language)
    add_rawdata_attr(item, request)


def add_rawdata_attr(item, request):
    """
    并发给数据源节点添加tooltip属性
    :param item: dict
    :param request:
    :return:
    """
    rawdata_dict = AccessApi.rawdata.retrieve({"raw_data_id": item["id"], "show_display": 1})
    item["id"] = "{}_{}".format(item["type"], item["id"])
    item["name"] = item["qualified_name"]
    raw_data_property_list = [
        "description",
        "raw_data_name",
        "bk_biz_id",
        "bk_biz_name",
        "updated_by",
        "updated_at",
        "data_category_alias",
        "data_scenario_alias",
        "data_source_alias",
        "raw_data_alias",
    ]
    for each_property in raw_data_property_list:
        item[each_property] = rawdata_dict.get(each_property)
    #  获取数据源的标签列表
    tag_search = rawdata_dict.get("tags", {})
    # 拿到格式化标签列表 & 对标签排序
    tag_list = format_tag_list(tag_search)
    tag_list = get_sort_tag_list(tag_list)
    item["tag_list"] = tag_list


def async_add_rt_attr(split_rtid_list, request):
    """
    并发查rt详情
    :param item:dict
    :return:
    """
    # 激活除了主线程以外的request
    activate_request(request)
    rt_list = MetaApi.result_tables.list({"result_table_ids": split_rtid_list})
    return rt_list
