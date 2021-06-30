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
from abc import ABCMeta, abstractmethod

from .process_nodes import (
    AccessCustomTemplate,
    AccessNode,
    AuthProjectDataNode,
    CleanNode,
    CleanTemplate,
    DataModelInstNode,
    DataModelNode,
    IgniteStorageNode,
    IgniteStorageTemplate,
    SimpleTemplate,
)

logger = logging.getLogger()


class BaseProcessor(metaclass=ABCMeta):
    """
    数据处理基类
    """

    @abstractmethod
    def build(self):
        """
        构建数据处理流程
        """
        pass


class BKDFlowProcessor(BaseProcessor):

    """
    数据平台数据流构建流程，可以按需注册处理节点和参数模型，比如

    - 1. 数据接入节点
    - 2. 数据清洗节点
    - 3. 数据计算节点
            - 3.1 DataFlow 计算节点
            - 3.2 数据模型计算节点
    """

    mapping_process_node = dict()
    mapping_process_template = dict()

    @classmethod
    def regiter_process_node(cls, node):
        cls.mapping_process_node[node.__name__] = node

    @classmethod
    def regiter_process_template(cls, template):
        cls.mapping_process_template[template.__name__] = template

    def __init__(self, pipeline: list):
        self.pipeline = pipeline

    def build(self):
        node_output = dict()

        for index, node in enumerate(self.pipeline):
            logger.info(f"[BKDFlowProcessor] Start to process node({node})")

            process_node = self.mapping_process_node[node["process_node"]]
            process_template = self.mapping_process_template[node["process_template"]]
            process_context = self.handle_node_context(
                node["process_context"], node_output
            )

            logger.info(
                f"[BKDFlowProcessor] Using {process_node} & {process_template} & {process_context} "
            )

            params_template = process_template(process_context)
            node = process_node(params_template)

            logger.info(f"[BKDFlowProcessor] Build node({node})")
            node_output[f"${index}"] = node.build()
            logger.info(f"[BKDFlowProcessor] Succeed to build node({node})")

    def handle_node_context(self, node_context, node_output):
        """
        处理节点上下文，主要是解决部分后置节点依赖前置节点，主要是替换形如 $0.raw_data_id 等占位符
        """
        for k, v in node_context.items():
            if type(v) is str and v.startswith("$"):
                node_output_index = v.split(".")[0]
                node_output_attr = v.split(".")[1]

                node_context[k] = node_output[node_output_index][node_output_attr]
        return node_context


BKDFlowProcessor.regiter_process_node(AccessNode)
BKDFlowProcessor.regiter_process_node(CleanNode)
BKDFlowProcessor.regiter_process_node(IgniteStorageNode)
BKDFlowProcessor.regiter_process_node(DataModelNode)
BKDFlowProcessor.regiter_process_node(DataModelInstNode)
BKDFlowProcessor.regiter_process_node(AuthProjectDataNode)
BKDFlowProcessor.regiter_process_template(AccessCustomTemplate)
BKDFlowProcessor.regiter_process_template(CleanTemplate)
BKDFlowProcessor.regiter_process_template(IgniteStorageTemplate)
BKDFlowProcessor.regiter_process_template(SimpleTemplate)
