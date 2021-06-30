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

from bkbase.dataflow.one_model.topo.deeplearning_node import Builder, Node


class ModelTransformerNode(Node):
    def __init__(self, builder):
        self.user_main_module = builder.user_main_module
        self.user_args = builder.user_args
        self.process_type = builder.process_type
        self.feature_shape = builder.feature_shape


class ModelTransformerNodeBuilder(Builder):
    def __init__(self, info):
        super().__init__(info)
        processor = info["processor"]
        self.user_main_module = processor["user_main_module"]
        self.user_args = processor["args"]
        self.process_type = processor["type"]
        self.feature_shape = self.user_args["feature_shape"] if "feature_shape" in self.user_args else None
        self.label_shape = self.user_args["label_shape"] if "label_shape" in self.user_args else None

    def build(self):
        return ModelTransformerNode(self)
