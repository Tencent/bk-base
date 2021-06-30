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

from django.utils import timezone

from apps.api import DataManageApi, MetaApi
from apps.dataflow.handlers.business import Business


def history_search_attr(search_list):
    standard_dict, tag_dict, all_biz_dict, pro_dict, standard_content_dict = get_conf_dict()
    for each in search_list:
        each["time"] = timezone.localtime(each.get("time")).strftime("%Y-%m-%d %H:%M:%S")
        if each.get("created_at_start"):
            each["created_at_start"] = timezone.localtime(each.get("created_at_start")).strftime("%Y-%m-%d %H:%M:%S")
        if each.get("created_at_end"):
            each["created_at_end"] = timezone.localtime(each.get("created_at_end")).strftime("%Y-%m-%d %H:%M:%S")

        # if tag_code is standard_version_id, add standard_name attribute
        if str(each["tag_code"]).isdigit():
            each["standard_name"] = standard_dict.get(int(each["tag_code"]))
        if each.get("standard_content_id"):
            each["standard_content_name"] = standard_content_dict.get(int(each["standard_content_id"]))
        # if tag_code is tag_code
        elif each["tag_code"] != "virtual_data_mart" and each["tag_code"]:
            each["category_alias"] = tag_dict.get(each["tag_code"], "")
        tag_ids_alias = []
        for each_tag in each["tag_ids"]:
            if tag_dict.get(each_tag) and tag_dict.get(each_tag) not in tag_ids_alias:
                tag_ids_alias.append(tag_dict.get(each_tag))
        if tag_ids_alias:
            each["tag_ids_alias"] = tag_ids_alias

        # if bik_biz_id != None, get bk_biz_alias
        if each.get("bk_biz_id"):
            each["bk_biz_name"] = all_biz_dict.get(each.get("bk_biz_id"), "")
        # if project_id != None, get project_name
        if each.get("project_id"):
            each["project_name"] = pro_dict.get(each["project_id"])


def get_conf_dict():
    """
    获取标准、标签、业务、项目字典表
    :return:
    """
    # standard_version_id对应的字典
    standard_dict = {}
    standard_content_dict = {}
    # online标准列表
    standard_list = DataManageApi.standard_search({"is_online": 1})
    # 标签字典
    tag_dict = DataManageApi.tag_dict()
    # 业务字典
    all_biz_dict = Business.get_name_dict()
    # 项目
    pro_list = MetaApi.project_list.list({})
    pro_dict = {}
    for each_pro in pro_list:
        pro_dict[each_pro["project_id"]] = each_pro["project_name"]
    for each_stan in standard_list:
        standard_dict[each_stan["standard_version_id"]] = each_stan["standard_name"]
        for each_det in each_stan["detaildata"]:
            standard_content_dict[each_det["id"]] = each_det["standard_content_name"]
        for each_ind in each_stan["indicator"]:
            standard_content_dict[each_ind["id"]] = each_ind["standard_content_name"]
    return standard_dict, tag_dict, all_biz_dict, pro_dict, standard_content_dict
