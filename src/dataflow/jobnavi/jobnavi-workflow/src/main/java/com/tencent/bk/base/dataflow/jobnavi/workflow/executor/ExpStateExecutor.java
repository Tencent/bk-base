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

package com.tencent.bk.base.dataflow.jobnavi.workflow.executor;

import com.tencent.bk.base.dataflow.jobnavi.workflow.TriggerRule;
import com.tencent.bk.base.dataflow.jobnavi.workflow.WorkflowTask;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExpStateExecutionNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;


public class ExpStateExecutor extends AbstractNodeExecutor {

    private static final Logger logger = Logger.getLogger(ExpStateExecutor.class);

    public ExpStateExecutor(AbstractExecutionNode node) {
        this.setNode(node);
    }

    public static Object convertToCode(String jexlExp, Map<String, Object> map) {
        JexlEngine jexl = new JexlEngine();
        Expression e = jexl.createExpression(jexlExp);
        JexlContext jc = new MapContext();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            jc.set(entry.getKey(), entry.getValue());
        }
        if (null == e.evaluate(jc)) {
            return "";
        }
        return e.evaluate(jc);
    }

    @Override
    public boolean ifTrigger(WorkflowTask workflowTask) {
        if (workflowTask.getResultList().containsKey(this.getNode().getId())) {
            logger.info("not the first time to start the state node");
            return false;
        }
        Map<String, TaskStatus> status = workflowTask.getStatus();
        if (((ExpStateExecutionNode) this.getNode()).getTriggerRule() == TriggerRule.one_done) {
            return true;
        } else if (((ExpStateExecutionNode) this.getNode()).getTriggerRule() == TriggerRule.all_done) {
            //check if all parents are completed
            for (AbstractExecutionNode node : this.getNode().getParents()) {
                if (status.get(node.getId()) != TaskStatus.failed && status.get(node.getId()) != TaskStatus.disabled
                        && status.get(node.getId()) != TaskStatus.failed_succeeded
                        && status.get(node.getId()) != TaskStatus.finished
                        && status.get(node.getId()) != TaskStatus.killed) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    @Override
    public void start(WorkflowTask workflowTask) throws Exception {
        try {
            logger.info("start state node" + this.getNode().getId());
            Map<String, TaskStatus> status = workflowTask.getStatus();
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, TaskStatus> entry : status.entrySet()) {
                map.put(entry.getKey(), entry.getValue());
            }
            Map<String, List<String>> expressionList = ((ExpStateExecutionNode) this.getNode()).getStateContent();
            List<String> result = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : expressionList.entrySet()) {
                logger.info("Expression:" + entry.getKey());
                String expression = entry.getKey();
                //evaluate state result
                Object code = convertToCode(expression, map);
                if (code != null) {
                    logger.info(expression + " " + code.toString());
                    if (Boolean.parseBoolean(code.toString())) {
                        //add all trigger children
                        result.addAll(entry.getValue());
                    }
                }
            }
            if (result.isEmpty() && !workflowTask.checkTrigger(this.getNode().getId())) {
                logger.info("can not trigger the following nodes");
                return;
            }
            workflowTask.stateStartNext(this.getNode().getId(), result);
        } catch (Exception e) {
            logger.info("fail to start state node:" + this.getNode().getId());
            throw e;
        }
    }
}
