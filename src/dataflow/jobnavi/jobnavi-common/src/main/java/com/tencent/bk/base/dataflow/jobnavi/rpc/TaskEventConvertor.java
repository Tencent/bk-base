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

package com.tencent.bk.base.dataflow.jobnavi.rpc;

import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import java.util.ArrayList;
import java.util.List;

public class TaskEventConvertor {

    /**
     * convert thrift event to common event
     *
     * @param event
     * @return common event
     * @throws NaviException
     */
    public static com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent toCommonEvent(TaskEvent event)
            throws NaviException {
        com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent taskEvent = TaskEventFactory
                .generateTaskEvent(event.getEventName());
        taskEvent.setEventName(event.getEventName());

        com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext context
                = new com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext();
        context.setEventInfo(event.getContext().getEventInfo());

        com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo executeInfo
                = new com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo();
        executeInfo.setId(event.getContext().getExecuteInfo().getId());
        executeInfo.setHost(event.getContext().getExecuteInfo().getHost());
        executeInfo.setRank(event.getContext().getExecuteInfo().getRank());
        context.setExecuteInfo(executeInfo);

        com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo taskInfo
                = new com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo();
        taskInfo.setScheduleId(event.getContext().getTaskInfo().getScheduleId());
        taskInfo.setScheduleTime(event.getContext().getTaskInfo().getScheduleTime());
        taskInfo.setDataTime(event.getContext().getTaskInfo().getDataTime());
        taskInfo.setDecommissionTimeout(event.getContext().getTaskInfo().getDecommissionTimeout());
        taskInfo.setNodeLabel(event.getContext().getTaskInfo().getNodeLabel());

        com.tencent.bk.base.dataflow.jobnavi.state.TaskType taskType
                = new com.tencent.bk.base.dataflow.jobnavi.state.TaskType();
        taskType.setName(event.getContext().getTaskInfo().getType().getName());
        taskType.setTag(event.getContext().getTaskInfo().getType().getTag());
        taskType.setDescription(event.getContext().getTaskInfo().getType().getDescription());
        taskType.setMain(event.getContext().getTaskInfo().getType().getMain());
        taskType.setEnv(event.getContext().getTaskInfo().getType().getEnv());
        if (event.getContext().getTaskInfo().getType().getLanguage() != null) {
            taskType.setLanguage(com.tencent.bk.base.dataflow.jobnavi.state.Language
                    .valueOf(event.getContext().getTaskInfo().getType().getLanguage().toString()));
        }
        String taskTypeName = event.getContext().getTaskInfo().getType().getTaskMode().name();
        taskType.setTaskMode(TaskMode.valueOf(taskTypeName));
        taskInfo.setType(taskType);

        taskInfo.setExtraInfo(event.getContext().getTaskInfo().getExtraInfo());

        com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo taskRecoveryInfo
                = new com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo();
        taskRecoveryInfo.setRecoveryTimes(event.getContext().getTaskInfo().getRecoveryInfo().getRecoveryTimes());
        taskRecoveryInfo.setMaxRecoveryTimes(event.getContext().getTaskInfo().getRecoveryInfo().getMaxRecoveryTimes());
        taskRecoveryInfo.setIntervalTime(event.getContext().getTaskInfo().getRecoveryInfo().getIntervalTime());
        taskRecoveryInfo.setRecoveryEnable(event.getContext().getTaskInfo().getRecoveryInfo().isRecoveryEnable());
        taskInfo.setRecoveryInfo(taskRecoveryInfo);
        context.setTaskInfo(taskInfo);

        taskEvent.setContext(context);

        if (event.getChangeStatus() != null) {
            taskEvent.setChangeStatus(
                    com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus.valueOf(event.getChangeStatus().toString()));
        }

        return taskEvent;
    }

    /**
     * convert common event to thrift event
     *
     * @param event
     * @return thrift event
     */
    public static TaskEvent toThriftEvent(com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent event) {
        TaskEvent taskEvent = new TaskEvent();
        taskEvent.setEventName(event.getEventName());

        EventContext context = new EventContext();
        context.setEventInfo(event.getContext().getEventInfo());

        ExecuteInfo executeInfo = new ExecuteInfo();
        executeInfo.setId(event.getContext().getExecuteInfo().getId());
        executeInfo.setHost(event.getContext().getExecuteInfo().getHost());
        executeInfo.setRank(event.getContext().getExecuteInfo().getRank());
        context.setExecuteInfo(executeInfo);

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setScheduleId(event.getContext().getTaskInfo().getScheduleId());
        taskInfo.setScheduleTime(event.getContext().getTaskInfo().getScheduleTime());
        taskInfo.setDataTime(event.getContext().getTaskInfo().getDataTime());
        taskInfo.setDecommissionTimeout(event.getContext().getTaskInfo().getDecommissionTimeout());
        taskInfo.setNodeLabel(event.getContext().getTaskInfo().getNodeLabel());

        TaskType taskType = new TaskType();
        taskType.setName(event.getContext().getTaskInfo().getType().getName());
        taskType.setTag(event.getContext().getTaskInfo().getType().getTag());
        taskType.setDescription(event.getContext().getTaskInfo().getType().getDescription());
        taskType.setMain(event.getContext().getTaskInfo().getType().getMain());
        taskType.setEnv(event.getContext().getTaskInfo().getType().getEnv());
        if (event.getContext().getTaskInfo().getType().getLanguage() != null) {
            taskType.setLanguage(Language.valueOf(event.getContext().getTaskInfo().getType().getLanguage().toString()));
        }
        String taskTypeName = event.getContext().getTaskInfo().getType().getTaskMode().name();
        taskType.setTaskMode(com.tencent.bk.base.dataflow.jobnavi.rpc.TaskMode.valueOf(taskTypeName));
        taskInfo.setType(taskType);

        taskInfo.setExtraInfo(event.getContext().getTaskInfo().getExtraInfo());

        TaskRecoveryInfo taskRecoveryInfo = new TaskRecoveryInfo();
        taskRecoveryInfo.setRecoveryTimes(event.getContext().getTaskInfo().getRecoveryInfo().getRecoveryTimes());
        taskRecoveryInfo.setMaxRecoveryTimes(event.getContext().getTaskInfo().getRecoveryInfo().getMaxRecoveryTimes());
        taskRecoveryInfo.setIntervalTime(event.getContext().getTaskInfo().getRecoveryInfo().getIntervalTime());
        taskRecoveryInfo.setRecoveryEnable(event.getContext().getTaskInfo().getRecoveryInfo().isRecoveryEnable());
        taskInfo.setRecoveryInfo(taskRecoveryInfo);
        context.setTaskInfo(taskInfo);

        taskEvent.setContext(context);
        if (event.getChangeStatus() != null) {
            taskEvent.setChangeStatus(TaskStatus.valueOf(event.getChangeStatus().toString()));
        }

        return taskEvent;
    }

    /**
     * convert common task event result to thrift event result
     * @param result
     * @return thrift event result
     */
    public static TaskEventResult toThriftResult(
            com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult result) {
        TaskEventResult taskEventResult = new TaskEventResult();
        taskEventResult.setSuccess(result.isSuccess());
        taskEventResult.setProcessInfo(result.getProcessInfo());
        List<TaskEvent> events = new ArrayList<>();
        for (com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent event : result.getNextEvents()) {
            events.add(toThriftEvent(event));
        }
        taskEventResult.setNextEvents(events);
        return taskEventResult;
    }

    /**
     * convert thrift event result to common result
     * @param result
     * @return common result
     * @throws NaviException
     */
    public static com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult toCommonResult(
            TaskEventResult result) throws NaviException {
        com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult taskEventResult
                = new com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult();
        taskEventResult.setSuccess(result.isSuccess());
        taskEventResult.setProcessInfo(result.getProcessInfo());
        List<com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent> events = new ArrayList<>();
        for (TaskEvent event : result.getNextEvents()) {
            events.add(toCommonEvent(event));
        }
        taskEventResult.addNextEvents(events);
        return taskEventResult;
    }
}
