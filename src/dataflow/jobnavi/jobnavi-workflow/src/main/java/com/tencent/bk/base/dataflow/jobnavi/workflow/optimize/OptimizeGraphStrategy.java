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
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.JobGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class OptimizeGraphStrategy {

    private static final Logger logger = Logger.getLogger(OptimizeGraphStrategy.class);
    private final List<String> optimizeList = new ArrayList<>();
    private final JobGraph jobGraph;
    private final WorkflowTask context;

    public OptimizeGraphStrategy(JobGraph jobGraph, WorkflowTask context) {
        this.jobGraph = jobGraph;
        this.context = context;
    }

    public ExecutionGraph optimizeGraph() throws Exception {
        ExecutionGraph executeGraph = new ExecutionGraph();
        executeGraph.setConfiguration(jobGraph.getConf());
        executeGraph.setParameter(jobGraph.getParameter());
        for (AbstractLogicalNode node : this.jobGraph.getHead()) {
            optimize(executeGraph, node, context);
        }
        return executeGraph;
    }

    private void optimize(ExecutionGraph executeGraph,
            AbstractLogicalNode node,
            WorkflowTask context) throws Exception {
        try {
            AbstractOptimizeExecutor optimizeExecutor = OptimizeFactory.getOptimizeExecutor(node);
            optimizeExecutor
                    .optimize(executeGraph, this.optimizeList, this.jobGraph.getParentMap().get(node.getId()), context);
            this.optimizeList.add(node.getId());
            if (node.getChildren() == null) {
                return;
            }
            for (AbstractLogicalNode child : node.getChildren()) {
                optimize(executeGraph, child, context);
            }
        } catch (Exception e) {
            logger.error("fail to optimize the node:" + node.getId(), e);
            throw e;
        }
    }


}
