/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.dataflow.ml.util;

import com.tencent.bk.base.dataflow.core.topo.Node;

public class ModelNodeUtil {

    /**
     * 获取当前节点的父节点，且父节点为数据节点。
     * 规则：只有一个父节点时，上游节点必为数据节点
     *
     * @param currentNode 当前节点
     * @return 数据父节点
     */
    public static Node getSingleParentDataNode(Node currentNode) {
        if (currentNode.getParents().size() == 1) {
            return currentNode.getParents().get(0);
        }
        for (Node node : currentNode.getParents()) {
            if (node.getType().equals("data") || node.getType().equals("batch")) {
                return node;
            }
        }
        throw new RuntimeException("Not found the parent data node for " + currentNode.getNodeId());
    }

    /**
     * 获取当前节点的父节点，且父节点为模型节点。
     *
     * @param currentNode 当前节点
     * @return 模型父节点
     */
    public static Node getSingleParentModelNode(Node currentNode) {
        for (Node node : currentNode.getParents()) {
            if (node.getType().equals("model")) {
                return node;
            }
        }
        throw new RuntimeException("Not found the parent model node for " + currentNode.getNodeId());
    }

}
