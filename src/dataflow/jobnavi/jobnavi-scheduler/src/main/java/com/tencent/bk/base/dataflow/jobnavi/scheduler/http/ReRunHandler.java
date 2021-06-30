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
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyResult;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class ReRunHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(ReRunHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String rerunListStr = (String) params.get("rerun_processings");
        String rerunModel = (String) params.get("rerun_model");
        Long startTime = (Long) params.get("start_time");
        Long endTime = (Long) params.get("end_time");
        Priority priority = Priority.low;
        if (params.get("priority") != null) {
            try {
                priority = Priority.valueOf((String) params.get("priority"));
            } catch (IllegalArgumentException e) {
                LOGGER.error("invalid rerun priority:" + params.get("priority") + ", detail:", e);
            }
        }
        final Set<TaskStatus> excludeStatuses = new HashSet<>();
        String excludeStatusesStr = request.getParameter("exclude_statuses");
        if (excludeStatusesStr != null) {
            for (String excludeStatus : excludeStatusesStr.split(",")) {
                excludeStatuses.add(TaskStatus.valueOf(excludeStatus));
            }
        }
        try {
            List<String> rerunList = Arrays.asList(rerunListStr.split(","));
            List<TaskEvent> executes = new ArrayList<>();
            generateExecute(rerunList, rerunModel, startTime, endTime, executes, priority, excludeStatuses);
            //create execute
            for (TaskEvent event : executes) {
                MetaDataManager.getJobDao().addExecute(event);
                String scheduleId = event.getContext().getTaskInfo().getScheduleId();
                long dataTime = event.getContext().getTaskInfo().getDataTime();
                ExecuteStatusCache.putStatus(scheduleId, dataTime, TaskStatus.preparing);
            }
            //send running events
            for (TaskEvent event : executes) {
                sendStatusEvent(event);
            }
            //generated result message
            List<Map<String, Object>> result = new ArrayList<>();
            for (TaskEvent event : executes) {
                Map<String, Object> context = event.getContext().toHttpJson();
                result.add(context);
            }
            writeData(true, "rerun success.", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(result), response);
        } catch (Exception e) {
            LOGGER.error("rerun error. ", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }

    private void sendStatusEvent(TaskEvent event) throws NaviException {
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        long scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
        long executeId = event.getContext().getExecuteInfo().getId();
        try {
            Schedule schedule = ScheduleManager.getSchedule();
            ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
            List<DependencyInfo> parents = scheduleInfo.getParents();
            for (DependencyInfo parent : parents) {
                if (schedule.getScheduleInfo(parent.getParentId()) == null
                        || DependencyManager.getParentStatus(scheduleInfo, parent, scheduleTime)
                        == DependencyResult.not_scheduled) {
                    DefaultTaskEvent skippedEvent = DefaultTaskEventBuilder.changeEvent(event, TaskStatus.skipped);
                    skippedEvent.getContext().setEventInfo("parent task not scheduled yet, skip rerun");
                    TaskStateManager.dispatchEvent(skippedEvent);
                    return;
                }
            }
            //send running event
            TaskEvent runningEvent = DefaultTaskEventBuilder.changeEvent(event, TaskStatus.running);
            //rerun not support recovery
            runningEvent.getContext().getTaskInfo().setRecoveryInfo(new TaskRecoveryInfo());
            TaskStateManager.dispatchEvent(runningEvent);
        } catch (Exception e) {
            String message = "rerun " + scheduleId + " execute " + executeId + " error. ";
            LOGGER.error(message, e);
            DefaultTaskEvent failedEvent = DefaultTaskEventBuilder.changeEvent(event, TaskStatus.failed);
            failedEvent.getContext().setEventInfo(message + e.getMessage());
            TaskStateManager.dispatchEvent(failedEvent);
        }
    }

    private void generateExecute(List<String> scheduleIdList, String model, Long startTime, Long endTime,
            List<TaskEvent> executes, Priority priority, Set<TaskStatus> excludeStatuses) throws NaviException {
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
        //find heads
        List<String> heads = getHeads(scheduleIdList, endTime);
        //generate head events
        for (String scheduleId : heads) {
            ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
            Long currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, startTime, false);
            Period period = scheduleInfo.getPeriod();
            while (currentScheduleTime <= endTime) {
                TaskInfo taskInfo = schedule.buildTaskInfo(scheduleInfo, currentScheduleTime);
                generateEvent(taskInfo, period.getTimezone(), rank, executes, excludeStatuses);
                currentScheduleTime = currentScheduleTime + 1;
                currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, currentScheduleTime, false);
            }
        }
        //generate child events
        generateChildEvents(executes, scheduleIdList, model, rank, excludeStatuses);
    }

    private List<String> getHeads(List<String> scheduleIdList, Long endTime) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        List<String> heads = new ArrayList<>();
        for (String scheduleId : scheduleIdList) {
            if (scheduleId == null) {
                continue;
            }
            ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
            if (scheduleInfo == null) {
                throw new NaviException("Can not find schedule [" + scheduleId + "]");
            }
            validateScheduleInfo(scheduleInfo);
            Period period = scheduleInfo.getPeriod();
            Long nextScheduleTime = schedule.getScheduleTimes().get(scheduleId);
            if (nextScheduleTime != null && endTime >= nextScheduleTime) {
                String prettyTime = CronUtil
                    .getPrettyTime(nextScheduleTime, "yyyy-MM-dd HH:mm:ss", period.getTimezone());
                throw new NaviException(
                    "schedule [" + scheduleId + "] at schedule time " + prettyTime
                        + " is not scheduled. Please make sure that end time should smaller than it.");
            }
            List<DependencyInfo> parents = scheduleInfo.getParents();
            boolean isHead = true;
            for (DependencyInfo parent : parents) {
                if (scheduleIdList.contains(parent.getParentId())
                    && !scheduleId.equals(parent.getParentId())) { //exclude self dependence
                    isHead = false;
                    break;
                }
            }
            if (isHead) {
                heads.add(scheduleId);
            }
        }
        return heads;
    }

    private TaskEvent generateEvent(TaskInfo taskInfo, TimeZone timeZone, double rank, List<TaskEvent> generatedEvents,
        Set<TaskStatus> excludeStatuses) throws NaviException {
        String scheduleId = taskInfo.getScheduleId();
        long scheduleTime = taskInfo.getScheduleTime();
        Schedule schedule = ScheduleManager.getSchedule();
        String schedulePrettyTime = CronUtil.getPrettyTime(scheduleTime, "yyyy-MM-dd HH:mm:ss", timeZone);
        if (needRerun(scheduleId, scheduleTime, schedulePrettyTime, excludeStatuses)) {
            TaskEvent preparingEvent = schedule.buildPreparingEvent(taskInfo, rank);
            if (!isExecuteContain(preparingEvent, generatedEvents)) {
                LOGGER.info("add schedule [" + scheduleId + "] rerun at time : " + schedulePrettyTime);
                generatedEvents.add(preparingEvent);
            }
            return preparingEvent;
        }
        return null;
    }

    private void generateChildEvents(List<TaskEvent> generatedEvents, List<String> rerunIdList, String rerunModel,
        double rank, Set<TaskStatus> excludeStatuses) throws NaviException {
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
                if (rerunIdList.contains(childId) || "all_child".equals(rerunModel)) {
                    ScheduleInfo childInfo = schedule.getScheduleInfo(childId);
                    if (childInfo == null) {
                        continue;
                    }
                    validateScheduleInfo(childInfo);
                    Period period = childInfo.getPeriod();
                    Long nextScheduleTime = schedule.getScheduleTimes().get(childId);
                    if (nextScheduleTime != null && childTaskInfo.getScheduleTime() >= nextScheduleTime) {
                        continue;
                    }
                    TaskEvent preparingEvent =
                        generateEvent(childTaskInfo, period.getTimezone(), rank, generatedEvents, excludeStatuses);
                    if (preparingEvent != null) {
                        bfsQueue.add(preparingEvent);
                    }
                }
            }
        }
    }

    private void validateScheduleInfo(ScheduleInfo scheduleInfo) throws NaviException {
        String scheduleId = scheduleInfo.getScheduleId();
        if (!scheduleInfo.isActive()) {
            LOGGER.error("schedule [" + scheduleId + "] is disabled. skip rerun.");
            throw new NaviException("schedule [" + scheduleId + "] is disabled. skip rerun.");
        }
        if (scheduleInfo.getPeriod() == null) {
            LOGGER.error("schedule [" + scheduleId + "] is not period schedule. skip rerun.");
            throw new NaviException("schedule [" + scheduleId + "] is not period schedule. skip rerun.");
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

    private boolean needRerun(String scheduleId, Long scheduleTime, String currentPrettyTime,
            Set<TaskStatus> excludeStatuses) throws NaviException {
        AbstractJobDao dao = MetaDataManager.getJobDao();
        TaskStatus status = dao.getExecuteStatus(scheduleId, scheduleTime);
        if (status == TaskStatus.preparing || status == TaskStatus.running
                || status == TaskStatus.recovering || status == TaskStatus.decommissioned
                || status == TaskStatus.lost || status == TaskStatus.retried) {
            throw new NaviException("schedule [" + scheduleId + "] at schedule time " + currentPrettyTime
                    + " is not finished, please retry it later.");
        } else if (excludeStatuses.contains(status)) {
            LOGGER.info("schedule [" + scheduleId + "] at schedule time " + currentPrettyTime + " is " + status);
            throw new NaviException(
                    "schedule [" + scheduleId + "] at schedule time " + currentPrettyTime + " is " + status
                            + " which is excluded");
        }
        return true;
    }

    //rerun priority
    private enum Priority {
        low, normal, high
    }
}
