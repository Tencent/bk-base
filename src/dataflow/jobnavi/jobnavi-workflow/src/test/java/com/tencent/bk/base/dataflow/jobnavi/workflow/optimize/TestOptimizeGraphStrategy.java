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

import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.JobGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.SubtaskLogicalNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestOptimizeGraphStrategy {

    @Test
    public void testOptimizeGraph1() {
        try {
            SubtaskLogicalNode logicalNode1 = new SubtaskLogicalNode();
            logicalNode1.setId("subtask1");
            logicalNode1.setTypeId("spark-sql");
            logicalNode1.setSubtaskInfo("");

            SubtaskLogicalNode logicalNode2 = new SubtaskLogicalNode();
            logicalNode2.setId("subtask2");
            logicalNode2.setTypeId("spark-sql");
            logicalNode2.setSubtaskInfo("");

            List<String> children = new ArrayList<>();
            children.add(logicalNode2.getId());
            //logicalNode1.setChildren(children);
            List<String> parent = new ArrayList<>();
            parent.add(logicalNode1.getId());

            JobGraph jobGraph = new JobGraph();
            List<AbstractLogicalNode> head = new ArrayList<>();
            head.add(logicalNode1);
            jobGraph.setHead(head);
            Map<String, List<String>> parentMap = new HashMap<>();
            parentMap.put("subtask2", parent);

            Map<String, AbstractLogicalNode> nodeMap = new HashMap<>();
            nodeMap.put(logicalNode1.getId(), logicalNode1);
            nodeMap.put(logicalNode2.getId(), logicalNode2);
            jobGraph.setNodeMap(nodeMap);
            jobGraph.setParentMap(parentMap);
            OptimizeGraphStrategy strategy = new OptimizeGraphStrategy(jobGraph, null);
            ExecutionGraph executionGraph = strategy.optimizeGraph();
            Assert.assertEquals(1, 1);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
