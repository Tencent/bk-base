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

package com.tencent.bk.base.dataflow.jobnavi.workflow.optimize;

import com.tencent.bk.base.dataflow.jobnavi.workflow.WorkflowTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractOptimizeExecutor {

    private AbstractLogicalNode node;

    public AbstractLogicalNode getNode() {
        return node;
    }

    public void setNode(AbstractLogicalNode node) {
        this.node = node;
    }

    public abstract void optimize(ExecutionGraph executeGraph, List<String> finishNode, List<String> parents,
            WorkflowTask context) throws NaviException;

    public boolean checkParents(List<String> parents, List<String> finishNode) {
        if (null == parents) {
            return true;
        }
        for (String parent : parents) {
            if (!finishNode.contains(parent)) {
                return false;
            }
        }
        return true;
    }

    public void addNode(ExecutionGraph executeGraph, AbstractExecutionNode executionNode, List<String> parents) {
        Map<String, AbstractExecutionNode> nodeMap = executeGraph.getNodeMap();
        nodeMap.put(executionNode.getId(), executionNode);
        executeGraph.setNodeMap(nodeMap);
        List<AbstractExecutionNode> parentList = new ArrayList<>();
        for (String parent : parents) {
            addChildren(executeGraph, executeGraph.getNodeMap().get(parent), executionNode);
            parentList.add(executeGraph.getNodeMap().get(parent));
        }
        executionNode.setParents(parentList);
    }

    public void addHead(ExecutionGraph executeGraph, AbstractExecutionNode executionNode) {
        List<AbstractExecutionNode> head = executeGraph.getHead();
        List<AbstractExecutionNode> parents = new ArrayList<>();
        executionNode.setParents(parents);
        head.add(executionNode);
        executeGraph.setHead(head);
        Map<String, AbstractExecutionNode> nodeMap = executeGraph.getNodeMap();
        nodeMap.put(executionNode.getId(), executionNode);
        executeGraph.setNodeMap(nodeMap);
    }

    public void addChildren(ExecutionGraph executeGraph,
            AbstractExecutionNode executionNode,
            AbstractExecutionNode childrenNode) {
        List<AbstractExecutionNode> children = executeGraph.getNodeMap().get(executionNode.getId()).getChildren();
        children.add(childrenNode);
        executeGraph.getNodeMap().get(executionNode.getId()).setChildren(children);
    }


}
