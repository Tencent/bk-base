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
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExpStateExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.StateLogicalNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class OptimizeStateExecutor extends AbstractOptimizeExecutor {

    private static final Logger logger = Logger.getLogger(OptimizeStateExecutor.class);

    public OptimizeStateExecutor(AbstractLogicalNode node) {
        this.setNode(node);
    }

    @Override
    public void optimize(ExecutionGraph executeGraph, List<String> finishNode, List<String> parents,
            WorkflowTask context) throws NaviException {
        if (!checkParents(parents, finishNode)) {
            return;
        }
        ExpStateExecutionNode expStateExecutionNode = new ExpStateExecutionNode();
        expStateExecutionNode.setId(this.getNode().getId());
        expStateExecutionNode.setTriggerRule(TriggerRule.one_done);
        if (((StateLogicalNode) this.getNode()).getStateType().equals("depend_rule")) {
            for (Map.Entry<String, List<String>> entry : ((StateLogicalNode) this.getNode()).getStateContent()
                    .entrySet()) {
                if (entry.getKey().contains("all_finished")
                        || entry.getKey().contains("all_done")
                        || entry.getKey().contains("all_failed")) {
                    expStateExecutionNode.setTriggerRule(TriggerRule.all_done);
                    logger.info("trigger rule : all_done");
                }
            }
            Map<String, List<String>> stateContent = ((StateLogicalNode) this.getNode()).getStateContent();
            Map<String, List<String>> optimizeStateContent = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : stateContent.entrySet()) {
                List<String> resultList = entry.getValue();
                String stateExp = entry.getKey();
                try {
                    logger.info("try to transfer depend rule");
                    stateExp = stateExp.replace("all_finished", transfer("all_finished", parents));
                    stateExp = stateExp.replace("all_failed", transfer("all_failed", parents));
                    stateExp = stateExp.replace("all_done", transfer("all_done", parents));
                    stateExp = stateExp.replace("one_finished", transfer("one_finished", parents));
                    stateExp = stateExp.replace("one_failed", transfer("one_failed", parents));
                    stateExp = stateExp.replace("one_done", transfer("one_done", parents));
                    optimizeStateContent.put(stateExp, resultList);
                } catch (NaviException e) {
                    logger.error("fail to transfer the depend rule", e);
                    throw e;
                }
            }
            expStateExecutionNode.setStateContent(optimizeStateContent);
        }
        addNode(executeGraph, expStateExecutionNode, parents);

    }

    private String transfer(String depend, List<String> parents) throws NaviException {
        StringBuffer stringBuffer = new StringBuffer();
        int flag = 0;
        if ("all_finished".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append(parent + " == \"finished\"");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("&&" + parent + " == \"finished\"");
                }
            }
            logger.info("return");
            stringBuffer.append(")");
            return stringBuffer.toString();
        } else if ("all_failed".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append(parent + " == \"failed\"");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("&&" + parent + " == \"failed\"");
                }
            }
            stringBuffer.append(")");
            return stringBuffer.toString();
        } else if ("all_done".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append("(" + parent + " == \"finished\" || " + parent + " == \"failed\" || " + parent
                            + " == \"disabled\" || "
                            + parent + " == \"failed_succeeded\"" + ")");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("&&" + "(" + parent + " == \"finished\" || " + parent + " == \"failed\" || "
                            + parent + " == \"disabled\" || "
                            + parent + " == \"failed_succeeded\"" + ")");
                }
            }
            stringBuffer.append(")");
            return stringBuffer.toString();
        } else if ("one_finished".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append(parent + " == \"finished\"");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("||" + parent + " == \"finished\"");
                }
            }
            stringBuffer.append(")");
            return stringBuffer.toString();
        } else if ("one_failed".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append(parent + " == \"failed\"");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("||" + parent + " == \"failed\"");
                }
            }
            stringBuffer.append(")");
            return stringBuffer.toString();
        } else if ("one_done".equals(depend)) {
            stringBuffer.append("(");
            for (String parent : parents) {
                if (flag == 0) {
                    stringBuffer.append(parent + " == \"finished\" || " + parent + " == \"failed\" || " + parent
                            + " == \"disabled\" || "
                            + parent + " == \"failed_succeeded\"");
                    flag = flag + 1;
                } else {
                    stringBuffer.append("||" + parent + " == \"finished\" || " + parent + " == \"failed\" || " + parent
                            + " == \"disabled\" || "
                            + parent + " == \"failed_succeeded\"");
                }
            }
            stringBuffer.append(")");
            return stringBuffer.toString();
        }
        String message = "can not support this depend rule";
        logger.error(message);
        throw new NaviException(message);

    }
}
