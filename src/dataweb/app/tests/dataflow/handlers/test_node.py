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
from apps.dataflow.handlers.node import (
    AlgorithmModelNode,
    BaseNode,
    CleanNode,
    ESNode,
    HdfsNode,
    MysqlNode,
    NodeFactory,
    OfflineNode,
    RealtimeNode,
    RTSourceNode,
    StorageNode,
)
from tests.test_base import BaseTestCase


class TestIeodNode(BaseTestCase):
    def test_init_by_data(self):
        data = {"flow_id": self.en_faker.random_int(), "node_id": self.en_faker.random_int()}
        b = BaseNode(flow_id=123, node_id=456)
        result = b.init_by_data(data=data)
        self.assertEqual(result.flow_id, data["flow_id"])
        self.assertEqual(result.node_id, data["node_id"])

    def test_get_node(self):
        node = {
            "node_type": "clean",
            "node_id": self.en_faker.random_int(),
            "flow_id": self.en_faker.random_int(),
        }
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), CleanNode)
        node["node_type"] = "realtime"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), RealtimeNode)
        node["node_type"] = "mysql_storage"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), MysqlNode)
        node["node_type"] = "tspider_storage"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), StorageNode)
        node["node_type"] = "hdfs_storage"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), HdfsNode)
        node["node_type"] = "offline"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), OfflineNode)
        node["node_type"] = "elastic_storage"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), ESNode)
        node["node_type"] = "rtsource"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), RTSourceNode)
        node["node_type"] = "algorithm_model"
        result = NodeFactory.get_node(node=node)
        self.assertEqual(type(result), AlgorithmModelNode)
