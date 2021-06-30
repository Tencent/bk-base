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

from django.utils.translation import ugettext as _

from apps.api import DataManageApi
from apps.utils import cmp_to_key


def format_row(each_row):
    """
    格式化"标签选择器"内容
    :param each_row:
    :return:
    """
    # 默认（更多内容不展示时），is_more_show为False
    each_row["is_more_show"] = False
    # 五大分类tag top_n 的tooltip:将tag_alias和description拼接
    each_row["tooltip"] = tip_contat(each_row.get("tag_alias", ""), each_row.get("description", ""))
    # 如果当前行没有sub_list，则所有标签都在topn中展示出来，当前行就不显示"更多"
    each_row["is_inrow_more_show"] = True if (each_row.get("sub_list")) else False
    # 默认更多中的标签没有父分类，都是平铺的
    each_row["is_flat"] = True

    def format_sub_list(each_sub_tag):
        """
        标记每一类标签中sub_list的属性
        :param each_sub_tag:
        :param each_row:
        :return:
        """

        def format_item(each_item):
            each_item["is_click"] = False
            # 在"更多"里面的标签除了父分类就都不是top_n,因为更多重的父分类可能是top_n
            each_item["is_top10"] = False
            each_item["tooltip"] = tip_contat(each_item.get("tag_alias", ""), each_item.get("description", ""))
            if not each_item.get("be_category_id"):
                each_item["be_category_id"] = each_sub_tag.get("tag_id", -1)

        each_sub_tag["is_click"] = False
        each_sub_tag["tooltip"] = tip_contat(each_sub_tag.get("tag_alias", ""), each_sub_tag.get("description", ""))
        if not each_sub_tag.get("be_category_id"):
            # 只有topn标签才有be_category_id， 没有的话就将parent_id作为父分类
            each_sub_tag["be_category_id"] = each_row.get("tag_id", -1)
        if len(each_sub_tag["sub_list"]):
            each_row["is_flat"] = False

        list(map(format_item, each_sub_tag.get("sub_list", [])))

    def format_sub_top_list(each_tag):
        each_tag["is_click"] = False
        each_tag["is_top10"] = True
        each_tag["tooltip"] = tip_contat(each_tag.get("tag_alias", ""), each_tag.get("description", ""))

    list(map(format_sub_list, each_row.get("sub_list", [])))
    list(map(format_sub_top_list, each_row.get("sub_top_list", [])))


def format_row_top_zero(search_res_zero):
    for each_row in search_res_zero:
        # 首先把每一行的sub_list变成一个字典
        sub_list_dict = {}
        for each_sub_list in each_row.get("sub_list", []):
            sub_list_dict[each_sub_list.get("tag_code", "")] = each_sub_list

        # 如果某一行的第二层标签的sub_list为[]，则将该行第二层标签里面多加一个"其它"，然后将sub_list为[]的标签放进"其它"中
        for each_sub_list in each_row.get("sub_list", []):
            level_2_tag_code = each_sub_list.get("tag_code", "")
            if not each_sub_list.get("sub_list", []):
                # 判断其它在不在each_row.get('sub_list', [])中,加"其它"，将该二层标签放在其它里面，并从当前位置删掉
                if "other" not in sub_list_dict:
                    sub_list_dict["other"] = {"tag_code": "other", "tag_alias": _("其他"), "sub_list": [each_sub_list]}
                    del sub_list_dict[level_2_tag_code]
                # 判断"其它"已经在each_row.get('sub_list', [])中，则将该二层标签放在其它里面，并从当前位置删掉
                else:
                    sub_list_dict["other"]["sub_list"].append(each_sub_list)
                    del sub_list_dict[level_2_tag_code]
            else:
                continue

        # 先将sub_list从当前行删掉，然后将sub_list_dict的所有value push进去
        del each_row["sub_list"]
        each_row["sub_list"] = []
        for level_2_tag_code, each_sub_list in sub_list_dict.items():
            each_row["sub_list"].append(each_sub_list)
        each_row["sub_list"].sort(key=cmp_to_key(cmp_by_level_2_tag_code))


def tip_contat(tag_alias, description):
    # tooltip拼接
    tooltip_content = "{}（{}）".format(tag_alias, description) if description else tag_alias
    return tooltip_content


def cmp_by_level_2_tag_code(left, right):
    """
    tag按照第二列标签进行排序
    :param left: tag1
    :param right: tag2
    :return:-1,1,0
    """
    left = left["tag_code"]
    right = right["tag_code"]

    if left == "other":
        return 1
    elif right == "other":
        return -1
    else:
        return 0


def format_tag_selector(search_res, used_in_data_dict, overall_top_tag_list):
    business_tag = [
        each_row
        for each_row in search_res
        if each_row.get("tag_type") == "business" or each_row.get("tag_type") == "datamap_other"
    ]
    data_type_tag = [each_row for each_row in search_res if each_row.get("tag_type") == "data_type"]
    data_source_tag = [
        each_row for each_row in search_res if each_row.get("tag_type") == "datamap_source" and used_in_data_dict
    ]
    tag_type_info_list = DataManageApi.tag_type_info()
    business_dict = {}
    desc_dict = {}
    system_dict = {}
    for tag_type_dict in tag_type_info_list:
        if tag_type_dict["name"] == "business":
            business_dict = tag_type_dict
        elif tag_type_dict["name"] == "desc":
            desc_dict = tag_type_dict
        elif tag_type_dict["name"] == "system":
            system_dict = tag_type_dict
    tag_list = [
        {
            "title": business_dict.get("alias", ""),
            "description": business_dict.get("description", ""),
            "tag_list": business_tag,
            "action": {"multiple": True},
        },
        {
            "title": desc_dict.get("alias", ""),
            "description": desc_dict.get("description", ""),
            "tag_list": data_type_tag,
            "action": {"multiple": False},
        },
    ]
    if used_in_data_dict:
        tag_list.append(
            {
                "title": system_dict.get("alias", ""),
                "description": system_dict.get("description", ""),
                "tag_list": data_source_tag,
                "action": {"multiple": True},
            }
        )
    ret_dict = {"tag_list": tag_list, "overall_top_tag_list": overall_top_tag_list}
    return ret_dict


def get_tag_sort_count(used_in_data_dict, top, is_overall_topn_shown, overall_top):
    if not used_in_data_dict:
        search_res_dict = DataManageApi.tag_sort_count(
            {
                "top": top,
                "exclude_source": 1,
                "is_overall_topn_shown": is_overall_topn_shown,
                "overall_top": overall_top,
            }
        )
        search_res_zero_dict = DataManageApi.tag_sort_count({"top": 0, "exclude_source": 1})
    else:
        search_res_dict = DataManageApi.tag_sort_count(
            {"top": top, "is_overall_topn_shown": is_overall_topn_shown, "overall_top": overall_top}
        )
        search_res_zero_dict = DataManageApi.tag_sort_count({"top": 0})
    return search_res_dict, search_res_zero_dict
