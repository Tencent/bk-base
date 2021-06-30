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

import com.tencent.bk.base.dataflow.jobnavi.workflow.TriggerRule;
import com.tencent.bk.base.dataflow.jobnavi.workflow.WorkflowTask;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExpStateExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractStateExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.SubtaskExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.SubtaskLogicalNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizeSubtaskExecutor extends AbstractOptimizeExecutor {

    private static final String PROCESSOR_SPILT = "|";
    private static final String SUBTASK_PREFIX = "WORKFLOW";

    public OptimizeSubtaskExecutor(AbstractLogicalNode node) {
        this.setNode(node);
    }

    private static String genSubtaskScheduleId(String scheduleId, Long scheduleTime, String processorId) {
        return SUBTASK_PREFIX + PROCESSOR_SPILT + scheduleId + PROCESSOR_SPILT + scheduleTime + PROCESSOR_SPILT
                + processorId;
    }

    @Override
    public void optimize(ExecutionGraph executeGraph, List<String> finishNode, List<String> parents,
            WorkflowTask context) {
        int count = 0;
        if (!checkParents(parents, finishNode)) {
            return;
        }
        SubtaskExecutionNode subtaskNode = new SubtaskExecutionNode();
        subtaskNode.setId(this.getNode().getId());
        if (context != null) {
            subtaskNode.setScheduleId(
                    genSubtaskScheduleId(context.getScheduleId(), context.getScheduleTime(), this.getNode().getId()));
        } else {
            subtaskNode.setScheduleId(this.getNode().getId());
        }
        subtaskNode.setTypeId(((SubtaskLogicalNode) this.getNode()).getTypeId());
        subtaskNode.setSubtaskInfo(((SubtaskLogicalNode) this.getNode()).getSubtaskInfo());
        subtaskNode.setRetryQuota(((SubtaskLogicalNode) this.getNode()).getRetryQuota());
        subtaskNode.setRetryInterval(((SubtaskLogicalNode) this.getNode()).getRetryInterval());
        subtaskNode.setNodeLabel(((SubtaskLogicalNode) this.getNode()).getNodeLabel());
        if (parents == null) {
            addHead(executeGraph, subtaskNode);
        } else {
            List<String> parentList = new ArrayList<>();
            for (String parent : parents) {
                AbstractExecutionNode parentNode = executeGraph.getNodeMap().get(parent);
                if (parentNode instanceof AbstractStateExecutionNode) {
                    parentList.add(parentNode.getId());
                } else if (parentNode instanceof SubtaskExecutionNode) {
                    ExpStateExecutionNode expStateExecutionNode = new ExpStateExecutionNode();
                    expStateExecutionNode.setId("optimize#" + this.getNode().getId() + "#" + count);
                    count = count + 1;
                    expStateExecutionNode.setTriggerRule(TriggerRule.all_done);
                    List<String> addParent = new ArrayList<>();
                    addParent.add(parentNode.getId());
                    Map<String, List<String>> stateContent = new HashMap<>();
                    List<String> addChild = new ArrayList<>();
                    addChild.add(this.getNode().getId());
                    stateContent.put(parentNode.getId() + " == \"finished\"", addChild);
                    expStateExecutionNode.setStateContent(stateContent);
                    addNode(executeGraph, expStateExecutionNode, addParent);
                    parentList.add(expStateExecutionNode.getId());
                }
            }
            addNode(executeGraph, subtaskNode, parentList);
        }
    }
}
