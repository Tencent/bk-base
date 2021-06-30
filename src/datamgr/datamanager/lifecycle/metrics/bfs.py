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

import logging

from api import meta_api

logger = logging.getLogger(__name__)


def get_every_level_nodes(input, qualified_name):
    """
    广度优先搜索，拿到过滤掉数据处理节点和虚拟节点后每一层的节点
    :param input: dict {
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
    :param qualified_name: dataset_type + '_' + dataset_id
    :return: dict {0: 'raw_data_100600', 1: ['result_table_125_adsad']}
    """
    relations = input["relations"]
    root_id = qualified_name
    nodes = {}
    for relation in relations:
        to_entity_id = relation["to"]
        from_entity_id = relation["from"]
        if to_entity_id not in nodes.keys():
            child_node = {}
            child_node["id"] = to_entity_id
            child_node["children"] = []
            nodes[to_entity_id] = child_node
        if from_entity_id not in nodes.keys():
            parent_node = {}
            parent_node["id"] = from_entity_id
            parent_node["children"] = []
            nodes[from_entity_id] = parent_node
        nodes[from_entity_id]["children"].append(nodes[to_entity_id])

    root = nodes.get(root_id)
    root["column"] = 0
    list_node = []
    list_node.append(root)
    while len(list_node):
        tmp_node = list_node.pop(0)
        column = tmp_node.get("column", -1)
        for child in tmp_node["children"]:
            if child.get("column"):
                continue
            # the min depth of node
            child["column"] = column + 1
            list_node.append(child)
    column_dict = {0: root_id}
    for tmp_node in list(nodes.keys()):
        if "column" not in nodes[tmp_node]:
            logger.warning(
                "node {} does not have column attribute, qualified_name:{}".format(
                    tmp_node, qualified_name
                )
            )
        tmp_column = nodes[tmp_node]["column"]
        if tmp_column in column_dict:
            if nodes[tmp_node]["id"] not in column_dict.get(tmp_column, []):
                column_dict[tmp_column].append(nodes[tmp_node]["id"])
        else:
            column_dict[tmp_column] = [nodes[tmp_node]["id"]]
    return column_dict


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
            topo_dict[child] = {
                "from_relation": [],
                "to_relation": [],
                "parents": [],
                "children": [],
            }
        if parent not in topo_dict:
            topo_dict[parent] = {
                "from_relation": [],
                "to_relation": [],
                "parents": [],
                "children": [],
            }
        # 记录箭头指向某一节点的线
        topo_dict[child]["to_relation"].append(each_relation)
        topo_dict[child]["parents"].append(parent)
        # 记录箭头指出某一节点的线
        topo_dict[parent]["from_relation"].append(each_relation)
        topo_dict[parent]["children"].append(child)
    return topo_dict


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

    keys = search_dict.get("nodes", {}).keys()
    for key in list(keys):
        value = search_dict["nodes"][key]
        if value["type"] == "data_processing":
            del search_dict.get("nodes", {})[key]
            children = []
            parents = []
            from_relation = []
            to_relation = []
            if key in topo_dict:
                children = topo_dict[key].get("children", [])
                parents = topo_dict[key].get("parents", [])
                from_relation = topo_dict[key].get("from_relation", [])
                to_relation = topo_dict[key].get("to_relation", [])

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


def get_node_count(dataset_id, dataset_type):
    """
    拿到过滤掉dp的每一层节点数
    :param dataset_id: rt_id/data_id
    :param dataset_type: result_table/raw_data
    :return:
    """
    try:
        search_dict = meta_api.lineage(
            {
                "type": dataset_type,
                "qualified_name": dataset_id,
                "depth": -1,
                "backend_type": "dgraph",
                "direction": "OUTPUT",
                "only_user_entity": True,
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error(
            "dataset_id:{} get_node_count by meta lineage error:{}".format(
                dataset_id, e.message
            )
        )
        return {}
    if (not search_dict) or (not search_dict.get("relations", [])):
        return {}
    # 过滤掉数据处理节点
    filter_dp(search_dict)
    if not search_dict.get("relations", []):
        return {}
    # 广度优先搜索拿到广度指标中每一层的节点
    node_count_dict = get_every_level_nodes(
        search_dict, "{}_{}".format(dataset_type, dataset_id)
    )
    return node_count_dict
