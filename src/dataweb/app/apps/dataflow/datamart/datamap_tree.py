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


def get_type(node):
    """
    用于判断树形结构节点展示的展示模板类型
    共6种条件，6种条件的组合共有10种情况(加other共11种)
    按照优先级，判断条件依次为
    1.搜索命中：node.get("is_selected", -1) == 1
    2.标准节点：node.get('me_type', '') == 'standard'
    3.下面有标准的节点：node.get('has_standard', 0) == 1
    4.count=0的节点：node['dataset_count'] == 0
    5.根节点：node.get('loc', -2)==-1
    6.普通节点
    """
    if node.get("is_selected", -1) == 1:
        if node.get("me_type", "") == "standard":
            return "curstandard"
        if node.get("has_standard", 0) == 1:
            return "curhasstandard"
        return "current"
    if node.get("has_standard", 0) == 1 and node.get("loc", -2) == -1:
        return "rootstandard"
    if node.get("me_type", "") == "standard" and node["dataset_count"] == 0:
        return "zerostandard"
    if node.get("has_standard", 0) == 1 and node["dataset_count"] == 0:
        return "zerohasstandard"
    if node.get("me_type", "") == "standard":
        return "standard"
    if node.get("has_standard", 0) == 1:
        return "hasstandard"
    if node.get("loc", -2) == -1:
        return "root"
    if node["dataset_count"] == 0:
        return "zero"
    return "other"


def parse_tree_json(tree_nodes, relations, entity_attribute_value, record, column):
    """递归遍历tree_nodes,拿到用于dataflow展示的relations和entity_attribute_value"""
    if not tree_nodes or len(tree_nodes) == 0:
        return
    for each_tree_node in tree_nodes:
        if record.get(column, 0) == 0:
            record[column] = 0
        # is_selected = 1搜索到的节点，is_selected = 2搜索到的路径节点，is_selected = 3搜索到的路径兄弟节点，is_selected = 1其他节点

        # 用于判断节点展示的类型
        each_tree_node["type"] = get_type(each_tree_node)

        tpt_group = "default"
        if each_tree_node["category_name"] == "virtual_data_mart":
            tpt_group = "root"
        entity_attribute_value.append(
            {
                "id": each_tree_node["category_name"],
                "code": each_tree_node["category_name"],
                "name": each_tree_node["category_alias"],
                "is_selected": each_tree_node.get("is_selected", 0),
                "parent_zero": each_tree_node.get("parent_zero", 0),
                "direction": each_tree_node.get("direction", "right"),
                "child_show": each_tree_node.get("child_show", -1),
                "type": each_tree_node["type"],
                "count": each_tree_node["dataset_count"],
                "parent_code": each_tree_node["parent_code"],
                "column": column,
                "row": record[column],
                "tptGroup": tpt_group,
                "has_standard": each_tree_node.get("has_standard", 0),
            }
        )
        record[column] += 1

        childs = each_tree_node.get("sub_list", [])
        for each_child in childs:
            relations.append(
                {"fromEntityId": each_tree_node["category_name"], "toEntityId": each_child["category_name"]}
            )
        if childs:
            parse_tree_json(childs, relations, entity_attribute_value, record, column + 1)


def filter_count_zero(tree_node):
    """过滤树中count=0的节点"""
    if (not tree_node) or ("sub_list" not in tree_node) or (len(tree_node["sub_list"]) == 0):
        return
    childs = tree_node.get("sub_list", [])
    for each_child in childs[:]:
        # 删除tree_node下面的节点each_child
        if each_child.get("dataset_count") == 0:
            childs.remove(each_child)

        filter_count_zero(each_child)


def label_brother_func(tree_node):
    """将选中节点的兄弟节点selected属性标记为3"""
    if tree_node.get("is_selected", 0) not in (1, 2):
        # 它的孩子节点中有选中的节点
        return

    is_selected = False
    for child in tree_node["sub_list"]:
        if (
            child.get("is_selected", -1) == 1
            or child.get("is_selected", -1) == 2
            or tree_node.get("category_alias") == _("数据集市")
        ):
            # 当前节点的孩子中有is_selected =1 / is_selected =2,则记为child_show = 1，表示当前节点有孩子会显示在图中
            tree_node["child_show"] = 1
            is_selected = True
            break
    # 如果它的孩子有选中的节点，则将它的所有没select属性的孩子标记成3
    if is_selected:
        for child in tree_node["sub_list"]:
            if child.get("is_selected", 0) == 0:
                child["is_selected"] = 3


def label_child_is_zero_func(tree_node, child):
    """标记节点 父节点是否为0 的属性"""
    if tree_node["dataset_count"] == 0 or tree_node.get("parent_zero", -1) == 1:
        child["parent_zero"] = 1
    else:
        child["parent_zero"] = 0


def label_child_direction_func(tree_node, child):
    """标记节点 direction 属性"""
    if tree_node["loc"] == 0 or tree_node.get("direction", "") == "left":
        child["direction"] = "left"
    else:
        if child["loc"] == 0:
            child["direction"] = "left"
        else:
            child["direction"] = "right"


def label_tree(tree_node):
    if (not tree_node) or ("sub_list" not in list(tree_node.keys())) or (len(tree_node["sub_list"]) == 0):
        return

    if tree_node["loc"] == 0:
        tree_node["direction"] = "left"

    label_brother_func(tree_node)

    for child in tree_node["sub_list"]:
        label_child_is_zero_func(tree_node, child)
        label_child_direction_func(tree_node, child)
        label_tree(child)
