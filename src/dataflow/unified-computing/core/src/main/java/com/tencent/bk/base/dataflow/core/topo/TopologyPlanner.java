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

package com.tencent.bk.base.dataflow.core.topo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyPlanner.class);


    public TopologyPlanner() {

    }

    /**
     * 构建执行计划
     */
    public void makePlan(Topology topology) {
        LOGGER.info("Build flow.");
        JobRegister.register(topology.getJobId());
        orderTransformNodes(topology);
    }


    /**
     * 收集所有的transform node
     */
    private void orderTransformNodes(Topology topology) {
        Collection<SourceNode> sourceNodes = topology.getSourceNodes().values();
        List<TransformNode> transformNodes = new ArrayList<>();
        for (Node sourceNode : sourceNodes) {
            orderSourceChildren(sourceNode, transformNodes);
        }
        topology.setTransformNodes(transformNodes);
    }

    /**
     * 收集parent node的child node
     * 将transform node收集至 transformNodes
     * 子node有资格被收集在 transformNodes 里面的规则是：
     * 属于body的父node，已经全部被收集到 transformNodes 当中
     *
     * @param parent parent node
     */
    private void orderSourceChildren(Node parent, List<TransformNode> transformNodes) {
        for (Node child : parent.getChildren()) {
            if (isParentCollected(child, transformNodes) && !transformNodes.contains(child)
                    && child instanceof TransformNode) {
                transformNodes.add((TransformNode) child);
            }
            if (!child.getChildren().isEmpty() && child instanceof TransformNode) {
                orderSourceChildren(child, transformNodes);
            }
        }
    }

    /**
     * 判断 transformNodes 中是否将父rt收集完毕，用于判断多个node的情况
     * 判断条件：
     * 1.父node不是source，并且transformNodes没有收集该父node
     * 2.当父node收集完毕才能收集子node
     *
     * @param child 子表
     * @return 是否将node收集完毕
     */
    private boolean isParentCollected(Node child, List<TransformNode> transformNodes) {
        for (Node parent : child.getParents()) {
            if (!(parent instanceof SourceNode)
                    && !transformNodes.contains(parent)) {
                return false;
            }
        }
        return true;
    }

}
