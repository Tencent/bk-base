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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.conf.TaskInfoConstants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.recovery.RunnerTaskRecoveryManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.RunningTaskCounterService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyResult;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event.RunningTaskEventBuffer;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import org.apache.log4j.Logger;

public class TaskRunningProcessor implements TaskProcessor {

    private static final Logger LOGGER = Logger.getLogger(TaskRunningProcessor.class);

    @Override
    public TaskEventResult process(TaskEventInfo taskEventInfo) {
        TaskEvent event = taskEventInfo.getTaskEvent();
        long executeId = event.getContext().getExecuteInfo().getId();
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        long scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
        //check parent status
        if (DependencyManager.isDependency(scheduleId)) {
            LOGGER.info("checking dependence of task:[" + executeId + "][" + scheduleId + "][" + scheduleTime + "]");
            DependencyResult result;
            try {
                result = DependencyManager.getDependencyStatus(scheduleId, scheduleTime);
            } catch (NaviException e) {
                LOGGER.error("Get depend status error.", e);
                return errorResult(e.getMessage());
            }

            if (result == DependencyResult.not_scheduled || result == DependencyResult.waiting
                    || result == DependencyResult.running) {
                return errorResult("parent execute may not finished.");
            } else if (result == DependencyResult.skipped) {
                TaskEventResult errorResult = errorResult("Cannot find depend task. change status to skipped");
                errorResult.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.skipped));
                return errorResult;
            } else if (result == DependencyResult.terminated) {
                TaskEventResult errorResult = errorResult("Parent execute failed. change this execute to disabled");
                errorResult.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.disabled));
                return errorResult;
            } else {
                return runningTask(taskEventInfo);
            }
        } else {
            return runningTask(taskEventInfo);
        }
    }

    protected TaskEventResult runningTask(TaskEventInfo taskEventInfo) {
        RunnerInfo runnerInfo = null;
        TaskEvent event = taskEventInfo.getTaskEvent();
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        long executeId = event.getContext().getExecuteInfo().getId();
        LOGGER.info("trying to run task:[" + executeId + "][" + scheduleId + "]");
        TaskEventResult executeResult = new TaskEventResult();
        TaskMode taskMode = event.getContext().getTaskInfo().getType().getTaskMode();
        boolean isProcess = taskMode == TaskMode.process;
        try {
            String nodeLabel = getNodeLabel(event.getContext().getTaskInfo());
            LOGGER.info("node label of task:[" + executeId + "][" + scheduleId + "] is " + nodeLabel);
            runnerInfo = HeartBeatManager.getBestNode(nodeLabel, isProcess);
            if (runnerInfo == null) {
                LOGGER.info("No active Runner for task:[" + executeId + "][" + scheduleId
                        + "], move running event to event buffer");
                RunningTaskEventBuffer.waitForAvailableRunner(taskEventInfo);
                executeResult.setSuccess(false);
                executeResult.setProcessInfo("No active Runner for task:[" + executeId + "][" + scheduleId + "].");
                executeResult.setProcessed(false);
                return executeResult;
            }
            event.getContext().getExecuteInfo().setHost(runnerInfo.getRunnerId());
            //reserve running task quota before sending event to Runner
            if (!reserveRunningTaskQuota(taskEventInfo, runnerInfo)) {
                LOGGER.info("No running task quota for task:[" + executeId + "][" + scheduleId + "]");
                executeResult.setSuccess(false);
                executeResult.setProcessInfo("No running task quota for task:[" + executeId + "][" + scheduleId + "]");
                executeResult.setProcessed(false);
                return executeResult;
            }
            TaskEventResult runnerExecuteResult;
            try {
                MetaDataManager.getJobDao()
                        .updateExecuteHost(event.getContext().getExecuteInfo().getId(), runnerInfo.getRunnerId());
                runnerExecuteResult = TaskStateManager.sendEventToRunner(taskEventInfo);
            } catch (Exception e) {
                executeResult.setSuccess(false);
                executeResult
                        .setProcessInfo("failed to send running event of task:[" + executeId + "][" + scheduleId + "] "
                                + "to Runner, move running event to event buffer, detail:" + e);
                executeResult.setProcessed(false);
                LOGGER.error(executeResult.getProcessInfo());
                releaseRunningTaskQuota(taskEventInfo, runnerInfo);
                RunningTaskEventBuffer.waitForAvailableRunner(taskEventInfo);
                return executeResult;
            }
            if (!runnerExecuteResult.isSuccess()) {
                releaseRunningTaskQuota(taskEventInfo, runnerInfo);
                if (TaskInfoConstants.RUNNING_TASK_FULL_INFO.equals(runnerExecuteResult.getProcessInfo())
                        || TaskInfoConstants.RUNNER_UNAVAILABLE_INFO.equals(runnerExecuteResult.getProcessInfo())) {
                    HeartBeatManager.setRunnerStatus(runnerInfo.getRunnerId(), RunnerStatus.unavailable);
                    LOGGER.info("Runner not available for task:[" + executeId + "][" + scheduleId + "] "
                            + "(" + runnerExecuteResult.getProcessInfo() + "), move running event to event buffer");
                    RunningTaskEventBuffer.waitForAvailableRunner(taskEventInfo);
                    runnerExecuteResult.setProcessed(false);
                    return runnerExecuteResult;
                } else {
                    executeResult = processError(event, executeResult.getProcessInfo());
                    if (runnerExecuteResult.getNextEvents().size() > 0) {
                        executeResult.getNextEvents().addAll(runnerExecuteResult.getNextEvents());
                    }
                }
            } else {
                try {
                    MetaDataManager.getJobDao().updateExecuteRunningTime(event.getContext().getExecuteInfo().getId());
                } catch (NaviException e) {
                    LOGGER.warn("Failed to update start time of execute:" + executeId, e);
                }
                RunnerTaskRecoveryManager
                        .registerTask(runnerInfo.getRunnerId(), event.getContext().getExecuteInfo().getId());
                executeResult = runnerExecuteResult;
            }
            return executeResult;
        } catch (Throwable e) {
            LOGGER.error("Task running process error.", e);
            try {
                return processError(event, e.getMessage());
            } catch (NaviException e1) {
                LOGGER.error("Get ErrorEvent error.", e1);
                return errorResult("Get ErrorEvent error." + e1.getMessage());
            }
        }
    }

    protected TaskEventResult errorResult(String processInfo) {
        TaskEventResult result = new TaskEventResult();
        result.setProcessInfo(processInfo);
        result.setSuccess(false);
        return result;
    }


    /**
     * generate error result
     *
     * @param event
     * @param errorInfo
     * @return error result
     * @throws NaviException
     */
    protected TaskEventResult processError(TaskEvent event, String errorInfo) throws NaviException {
        StringBuilder processInfo = new StringBuilder();
        processInfo.append("[ERROR] execute ");
        processInfo.append(event.getContext().getTaskInfo().getScheduleId());
        processInfo.append(" error. time is ");
        processInfo.append(event.getContext().getTaskInfo().getScheduleTime());
        if (errorInfo != null) {
            processInfo.append(" reason:");
            processInfo.append(errorInfo);
        }
        TaskEventResult result = errorResult(processInfo.toString());
        result.setProcessInfo(processInfo.toString());
        result.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.failed));
        return result;
    }

    protected synchronized boolean reserveRunningTaskQuota(TaskEventInfo taskEventInfo, RunnerInfo runnerInfo) {
        //reserve running quota on corresponding schedule
        String scheduleId = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getScheduleId();
        long executeId = taskEventInfo.getTaskEvent().getContext().getExecuteInfo().getId();
        if (!RunningTaskCounterService.validateRunningTaskCountLimit(scheduleId)) {
            LOGGER.info("Running count of task:" + scheduleId
                    + " reach the max limit, move running event of execute:" + executeId + " to event buffer");
            RunningTaskEventBuffer.waitForScheduleQuota(scheduleId, taskEventInfo);
            return false;
        }
        //reserve running quota on Runner
        TaskMode taskMode = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getType().getTaskMode();
        switch (taskMode) {
            case process:
                if (runnerInfo.getTaskNum() < runnerInfo.getMaxTaskNum()) {
                    runnerInfo.setTaskNum(runnerInfo.getTaskNum() + 1);
                } else {
                    LOGGER.info("Running process task count of Runner:" + runnerInfo.getRunnerId()
                            + " reach the max limit, move running event of execute:" + executeId + " to event buffer");
                    RunningTaskEventBuffer.waitForAvailableRunner(taskEventInfo);
                    return false;
                }
                break;
            case thread:
                if (runnerInfo.getTaskThreadNum() < runnerInfo.getMaxThreadTaskNum()) {
                    runnerInfo.setTaskThreadNum(runnerInfo.getTaskThreadNum() + 1);
                } else {
                    LOGGER.info("Running thread task count of Runner:" + runnerInfo.getRunnerId()
                            + " reach the max limit, move running event of execute:" + executeId + " to event buffer");
                    RunningTaskEventBuffer.waitForAvailableRunner(taskEventInfo);
                    return false;
                }
                break;
            default:
                break;
        }
        RunningTaskCounterService.plusRunningTask(scheduleId);
        HeartBeatManager.updateHeartBeat(runnerInfo, false);
        return true;
    }

    protected synchronized void releaseRunningTaskQuota(TaskEventInfo taskEventInfo, RunnerInfo runnerInfo) {
        //release running quota on corresponding schedule
        String scheduleId = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getScheduleId();
        RunningTaskCounterService.minusRunningTask(scheduleId);
        //release running quota on Runner
        TaskMode taskMode = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getType().getTaskMode();
        switch (taskMode) {
            case process:
                runnerInfo.setTaskNum(runnerInfo.getTaskNum() - 1);
                break;
            case thread:
                runnerInfo.setTaskThreadNum(runnerInfo.getTaskThreadNum() - 1);
                break;
            default:
                break;
        }
        HeartBeatManager.updateHeartBeat(runnerInfo, false);
    }

    protected String getNodeLabel(TaskInfo taskInfo) {
        return taskInfo.getNodeLabel();
    }
}
