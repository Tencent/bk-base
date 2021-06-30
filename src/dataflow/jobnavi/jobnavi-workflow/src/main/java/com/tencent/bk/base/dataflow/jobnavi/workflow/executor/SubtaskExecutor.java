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

import com.tencent.bk.base.dataflow.jobnavi.workflow.WorkflowTask;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.SubtaskExecutionNode;
import org.apache.log4j.Logger;

public class SubtaskExecutor extends AbstractNodeExecutor {

    private static final Logger logger = Logger.getLogger(SubtaskExecutor.class);

    public SubtaskExecutor(AbstractExecutionNode node) {
        this.setNode(node);
    }

    @Override
    public boolean ifTrigger(WorkflowTask workflowTask) {
        if (this.getNode().getParents() == null) {
            return true;
        }
        //check depended parent node result
        for (AbstractExecutionNode node : this.getNode().getParents()) {
            if (!workflowTask.getResultList().containsKey(node.getId())) {
                return false;
            }
            if (!workflowTask.getResultList().get(node.getId()).contains(this.getNode().getId())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void start(final WorkflowTask workflowTask) throws Exception {
        try {
            final SubtaskExecutionNode node = (SubtaskExecutionNode) this.getNode();
            logger.info("create subtask: " + this.getNode().getId());
            final String subtaskScheduleId = node.getScheduleId();
            SubtaskMonitor.create(node.getScheduleId(), node);
            Long executeId = SubtaskMonitor.execute(subtaskScheduleId, workflowTask.getScheduleTime());
            workflowTask.updateExecIdMap(this.getNode().getId(), executeId);
            try {
                workflowTask.doWorkflowSavePoints();
            } catch (Exception e) {
                logger.warn("fail to do savepoint", e);
            }
            startMonitor(workflowTask, node, subtaskScheduleId, executeId);
        } catch (Exception e) {
            logger.error("create subtask " + this.getNode().getId() + " failed", e);
            throw e;
        }
    }

    private void startMonitor(final WorkflowTask workflowTask, final SubtaskExecutionNode node,
            final String subtaskScheduleId, final Long executeId) {
        TaskMonitorThread thread = new TaskMonitorThread(workflowTask, node, subtaskScheduleId, executeId);
        Thread monitor = new Thread(thread, node.getId() + "_" + executeId);
        logger.info("start monitor: " + node.getId());
        workflowTask.addMonitorThread(node.getId(), monitor);
        monitor.start();
    }

    public void recovery(final WorkflowTask workflowTask, final Long executeId) {
        final SubtaskExecutionNode node = (SubtaskExecutionNode) this.getNode();
        workflowTask.updateExecIdMap(this.getNode().getId(), executeId);
        try {
            workflowTask.doWorkflowSavePoints();
        } catch (Exception e) {
            logger.error("fail to do savepoint", e);
        }
        startMonitor(workflowTask, node, node.getScheduleId(), executeId);
    }

    static class TaskMonitorThread implements Runnable {

        WorkflowTask workflowTask;
        SubtaskExecutionNode node;
        String subtaskScheduleId;
        Long executeId;

        private TaskMonitorThread(WorkflowTask workflowTask, SubtaskExecutionNode node, String subtaskScheduleId,
                Long executeId) {
            this.workflowTask = workflowTask;
            this.node = node;
            this.subtaskScheduleId = subtaskScheduleId;
            this.executeId = executeId;
        }

        @Override
        public void run() {
            logger.info("monitor subtask: " + node.getId());
            TaskStatus status = workflowTask.getStatus().get(node.getId());
            ExecuteResult result;
            try {
                do {
                    Thread.sleep(1000);
                    logger.info(node.getId() + " current status is : " + status.name());
                    workflowTask.updateNodeStatus(node.getId(), status);
                    result = SubtaskMonitor.getExecuteResult(executeId);
                    logger.info(node.getId() + " get result is : " + JsonUtils.writeValueAsString(result.toHTTPJson()));
                    status = result.getStatus();
                    if (status == TaskStatus.failed && node.getRetryQuota() > 0) {
                        long currentTIme = System.currentTimeMillis();
                        long baseTime = result.getUpdatedAt();
                        long interval = CronUtil.parsePeriodStringToMills(node.getRetryInterval());
                        if (baseTime + interval > currentTIme) {
                            long waitTimeMills = baseTime + interval - currentTIme;
                            logger.info(
                                    "subtask:" + subtaskScheduleId + " will be retried after " + waitTimeMills + " ms");
                            Thread.sleep(waitTimeMills);
                        }
                        logger.info("retrying subtask:" + subtaskScheduleId + ", retry quota:" + node.getRetryQuota());
                        executeId = SubtaskMonitor.execute(subtaskScheduleId, workflowTask.getScheduleTime());
                        workflowTask.updateExecIdMap(this.node.getId(), executeId);
                        node.setRetryQuota(node.getRetryQuota() - 1);
                        try {
                            workflowTask.doWorkflowSavePoints();
                        } catch (Exception e) {
                            logger.error("fail to do savepoint", e);
                        }
                        result = SubtaskMonitor.getExecuteResult(executeId);
                        logger.info(
                                node.getId() + " get result is : " + JsonUtils.writeValueAsString(result.toHTTPJson()));
                        status = result.getStatus();
                    }
                } while (!Thread.currentThread().isInterrupted() && (status == TaskStatus.preparing
                        || status == TaskStatus.running || status == TaskStatus.none));
                logger.info(node.getId() + " finished , status is : " + status.name());
                SubtaskMonitor.delete(subtaskScheduleId);
            } catch (Exception e) {
                status = TaskStatus.failed;
                logger.error("monitor task error.", e);
            }

            if (!Thread.currentThread().isInterrupted()) {
                //subtask finished, trigger children node
                workflowTask.subtaskStartNext(node.getId(), status);
            }
            workflowTask.removeMonitorThread(node.getId());
        }
    }

}
