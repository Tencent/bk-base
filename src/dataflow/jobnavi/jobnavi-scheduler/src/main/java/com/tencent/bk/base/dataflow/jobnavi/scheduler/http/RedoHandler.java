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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class RedoHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(RedoHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String scheduleId = request.getParameter("schedule_id");
        long scheduleTime = Long.parseLong(request.getParameter("schedule_time"));
        boolean isRunDepend = Boolean.parseBoolean(request.getParameter("is_run_depend"));
        boolean isForce = Boolean.parseBoolean(request.getParameter("force"));
        final Set<TaskStatus> excludeStatuses = new HashSet<>();
        String excludeStatusesStr = request.getParameter("exclude_statuses");
        if (excludeStatusesStr != null) {
            for (String excludeStatus : excludeStatusesStr.split(",")) {
                excludeStatuses.add(TaskStatus.valueOf(excludeStatus));
            }
        }

        RedoHandler.Priority priority = Priority.normal;
        if (request.getParameter("priority") != null) {
            try {
                priority = RedoHandler.Priority.valueOf(request.getParameter("priority"));
            } catch (IllegalArgumentException e) {
                LOGGER.error("invalid rerun priority:" + request.getParameter("priority") + ", detail:", e);
            }
        }
        try {
            List<TaskEvent> executes = new ArrayList<>();
            generateExecute(scheduleId, scheduleTime, executes, priority, isForce, isRunDepend, excludeStatuses);
            //create execute
            for (TaskEvent event : executes) {
                MetaDataManager.getJobDao().addExecute(event);
                String currentScheduleId = event.getContext().getTaskInfo().getScheduleId();
                long dataTime = event.getContext().getTaskInfo().getDataTime();
                ExecuteStatusCache.putStatus(currentScheduleId, dataTime, TaskStatus.preparing);
            }
            //send running events
            for (TaskEvent event : executes) {
                sendEvent(event);
            }
            //generated result message
            List<Map<String, Object>> result = new ArrayList<>();
            for (TaskEvent event : executes) {
                Map<String, Object> context = event.getContext().toHttpJson();
                result.add(context);
            }
            writeData(true, "redo [" + scheduleId + "@" + scheduleTime + "] success.", HttpReturnCode.DEFAULT,
                    JsonUtils.writeValueAsString(result), response);
        } catch (Exception e) {
            LOGGER.error("redo error. ", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }

    private void sendEvent(TaskEvent event) throws NaviException {
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        long executeId = event.getContext().getExecuteInfo().getId();
        try {
            //send running event
            TaskEvent runningEvent = DefaultTaskEventBuilder.changeEvent(event, TaskStatus.running);
            //rerun not support recovery
            runningEvent.getContext().getTaskInfo().setRecoveryInfo(new TaskRecoveryInfo());
            TaskStateManager.dispatchEvent(runningEvent);
        } catch (Exception e) {
            String message = "redo " + scheduleId + " execute " + executeId + " error. ";
            LOGGER.error(message, e);
            DefaultTaskEvent failedEvent = DefaultTaskEventBuilder.changeEvent(event, TaskStatus.failed);
            failedEvent.getContext().setEventInfo(message + e.getMessage());
            TaskStateManager.dispatchEvent(failedEvent);
        }
    }

    private void generateExecute(String scheduleId, long scheduleTime, List<TaskEvent> executes,
            RedoHandler.Priority priority, boolean isForce, boolean isRunDepend, Set<TaskStatus> excludeStatuses)
            throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        //set rank according to priority
        double rank = 0;
        switch (priority) {
            case low:
                rank = 0;
                break;
            case normal:
                rank = DefaultTaskEventBuilder.getDefaultEventRank();
                break;
            case high:
                rank = DefaultTaskEventBuilder.getDefaultEventRank() * 2;
                break;
            default:
                break;
        }
        if (scheduleId == null) {
            return;
        }
        ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
        if (scheduleInfo == null) {
            throw new NaviException("Can not find schedule [" + scheduleId + "]");
        }
        validateScheduleInfo(scheduleInfo);
        TaskInfo taskInfo = schedule.buildTaskInfo(scheduleInfo, scheduleTime);
        if (generateEvent(taskInfo, rank, executes, excludeStatuses, isForce) == null || !isRunDepend) {
            return;
        }
        //generate child events
        generateChildEvents(executes, rank, excludeStatuses, isForce);
    }

    private TaskEvent generateEvent(TaskInfo taskInfo, double rank, List<TaskEvent> generatedEvents,
        Set<TaskStatus> excludeStatuses, boolean isForce) throws NaviException {
        String scheduleId = taskInfo.getScheduleId();
        long scheduleTime = taskInfo.getScheduleTime();
        Schedule schedule = ScheduleManager.getSchedule();
        if (needRedo(scheduleId, scheduleTime, excludeStatuses) || isForce) {
            TaskEvent preparingEvent = schedule.buildPreparingEvent(taskInfo, rank);
            if (!isExecuteContain(preparingEvent, generatedEvents)) {
                LOGGER.info("add schedule [" + scheduleId + "] rerun at time : " + scheduleTime);
                generatedEvents.add(preparingEvent);
            }
            return preparingEvent;
        }
        return null;
    }

    private void generateChildEvents(List<TaskEvent> generatedEvents, double rank,
        Set<TaskStatus> excludeStatuses, boolean isForce) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        Queue<TaskEvent> bfsQueue = new LinkedList<>(generatedEvents);
        while (!bfsQueue.isEmpty()) {
            TaskEvent event = bfsQueue.poll();
            if (event == null) {
                continue;
            }
            String scheduleId = event.getContext().getTaskInfo().getScheduleId();
            long dataTime = event.getContext().getTaskInfo().getDataTime();
            List<TaskInfo> childrenTaskInfo = DependencyManager.generateChildrenTaskInfo(scheduleId, dataTime);
            if (childrenTaskInfo == null) {
                continue;
            }
            for (TaskInfo childTaskInfo : childrenTaskInfo) {
                String childId = childTaskInfo.getScheduleId();
                if (scheduleId.equals(childId)) {
                    continue;
                }
                ScheduleInfo childInfo = schedule.getScheduleInfo(childId);
                if (childInfo == null) {
                    continue;
                }
                validateScheduleInfo(childInfo);
                Long nextScheduleTime = schedule.getScheduleTimes().get(childId);
                if (nextScheduleTime != null && childTaskInfo.getScheduleTime() >= nextScheduleTime) {
                    continue;
                }
                TaskEvent preparingEvent =
                    generateEvent(childTaskInfo, rank, generatedEvents, excludeStatuses, isForce);
                if (preparingEvent != null) {
                    bfsQueue.add(preparingEvent);
                }
            }
        }
    }

    private void validateScheduleInfo(ScheduleInfo scheduleInfo) throws NaviException {
        String scheduleId = scheduleInfo.getScheduleId();
        if (!scheduleInfo.isActive()) {
            LOGGER.error("schedule [" + scheduleId + "] is disabled. skip redo.");
            throw new NaviException("schedule [" + scheduleId + "] is disabled. skip redo.");
        }
        if (scheduleInfo.getPeriod() == null) {
            LOGGER.error("schedule [" + scheduleId + "] is not period schedule. skip rerun.");
            throw new NaviException("schedule [" + scheduleId + "] is not period schedule. skip redo.");
        }
    }

    private boolean isExecuteContain(TaskEvent event, List<TaskEvent> executes) {
        for (TaskEvent execute : executes) {
            if (execute.getContext().getTaskInfo().getScheduleId()
                    .equals(event.getContext().getTaskInfo().getScheduleId())
                    && execute.getContext().getTaskInfo().getScheduleTime() == event.getContext().getTaskInfo()
                    .getScheduleTime()) {
                return true;
            }
        }
        return false;
    }

    private boolean needRedo(String scheduleId, Long scheduleTime, Set<TaskStatus> excludeStatuses)
            throws NaviException {
        AbstractJobDao dao = MetaDataManager.getJobDao();
        TaskStatus status = dao.getExecuteStatus(scheduleId, scheduleTime);
        if (status == TaskStatus.none || status == TaskStatus.preparing || status == TaskStatus.running
                || status == TaskStatus.recovering || status == TaskStatus.decommissioned
                || status == TaskStatus.lost || status == TaskStatus.retried) {
            LOGGER.info("schedule [" + scheduleId + "] at schedule time " + scheduleTime
                    + " is not scheduled or not finished.");
            return false;
        } else if (excludeStatuses.contains(status)) {
            LOGGER.info("schedule [" + scheduleId + "] at schedule time " + scheduleTime + " is " + status
                    + " which is excluded");
            return false;
        }
        return true;
    }

    //redo priority
    private enum Priority {
        low, normal, high
    }
}
