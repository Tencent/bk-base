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
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

/**
 * 自依赖实例补齐接口类
 */
public class PatchHandler extends AbstractHttpHandler {

    public static final int RESULT_LIST_LIMIT = 24;
    private static final Logger LOGGER = Logger.getLogger(PatchHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        if ("list".equals(operate)) { //list instances to be patched
            String scheduleId = request.getParameter("schedule_id");
            String startTimeStr = request.getParameter("start_time");
            String endTimeStr = request.getParameter("end_time");
            String status = request.getParameter("status");
            if (StringUtils.isEmpty(scheduleId) || StringUtils.isEmpty(startTimeStr) || StringUtils
                    .isEmpty(endTimeStr)) {
                writeData(false, "[schedule_id][start_time][end_time] required. ", response);
                return;
            }
            try {
                List<Map<String, Object>> result;
                Long startTime = Long.parseLong(startTimeStr);
                Long endTime = Long.parseLong(endTimeStr);
                List<ExecuteResult> executeResults;
                if (StringUtils.isNotEmpty(status)) {
                    if (TaskStatus.values(status) == null) {
                        writeData(false, "status [" + status + "] is not validate. ", response);
                        return;
                    }
                    result = dao
                            .queryExecuteByTimeRangeAndStatus(scheduleId, startTime, endTime, TaskStatus.values(status),
                                    RESULT_LIST_LIMIT);
                } else {
                    result = dao.queryExecuteByTimeRange(scheduleId, startTime, endTime, RESULT_LIST_LIMIT);
                }
                for (Map<String, Object> record : result) {
                    boolean isAllowed = !"running".equals(record.get("status")) && !"finished"
                            .equals(record.get("status")) && !"recovering".equals(record.get("status"));
                    record.put("is_allowed", isAllowed);
                }
                writeData(true, "query execute by time range success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(result), response);
            } catch (Exception e) {
                LOGGER.error("query execute by time range error.", e);
                writeData(false, "query execute by time range error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("check_allowed".equals(operate)) { //check if given instance is allowed to be patched
            String scheduleId = request.getParameter("schedule_id");
            String scheduleTimeStr = request.getParameter("schedule_time");
            if (StringUtils.isEmpty(scheduleId) || StringUtils.isEmpty(scheduleTimeStr)) {
                writeData(false, "[schedule_id][schedule_time] required. ", response);
                return;
            }
            try {
                Long scheduleTime = Long.parseLong(scheduleTimeStr);
                TaskStatus status = dao.getExecuteStatus(scheduleId, scheduleTime);
                boolean isAllowed = status != TaskStatus.running && status != TaskStatus.finished;
                Map<String, Object> result = new HashMap<>();
                result.put("status", status.name());
                result.put("is_allowed", isAllowed);
                writeData(true, "", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(result), response);
            } catch (Exception e) {
                LOGGER.error("check patch allowed error.", e);
                writeData(false, "check patch allowed error." + e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION,
                        response);
            }
        }
    }

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        LOGGER.info("trying to patch execute:" + JsonUtils.writeValueAsString(params));
        String operate = (String) params.get("operate");
        if ("run".equals(operate)) { //run patch instance
            String scheduleId = (String) params.get("processing_id");
            if (StringUtils.isEmpty(scheduleId)) {
                writeData(false, "Param must contain [processing_id].", HttpReturnCode.ERROR_PARAM_INVALID, response);
                return;
            }
            String rerunScheduleListStr = (String) params.get("rerun_processings");
            String rerunModel = (String) params.get("rerun_model");
            Long targetScheduleTime = (Long) params.get("target_schedule_time");
            Long sourceScheduleTime = (Long) params.get("source_schedule_time");
            boolean dispatchToStorage = params.get("dispatch_to_storage") != null && (Boolean) params
                    .get("dispatch_to_storage");
            try {
                Schedule schedule = ScheduleManager.getSchedule();
                ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
                List<TaskEvent> rerunExecutes = new ArrayList<>();
                if (scheduleInfo != null && !rerunScheduleListStr.isEmpty()) {
                    Period period = schedule.getScheduleInfo(scheduleId).getPeriod();
                    if (period != null) {
                        List<String> rerunScheduleList = Arrays.asList(rerunScheduleListStr.split(","));
                        TimeZone timeZone = period.getTimezone();
                        Long[] timeRange = period.getPeriodUnit().getPeriodTimestampRange(targetScheduleTime, timeZone);
                        generateRerunExecute(rerunScheduleList, rerunModel, timeRange[0], timeRange[1], rerunExecutes);
                        for (TaskEvent event : rerunExecutes) {
                            MetaDataManager.getJobDao().addExecute(event);
                            String rerunScheduleId = event.getContext().getTaskInfo().getScheduleId();
                            long dataTime = event.getContext().getTaskInfo().getDataTime();
                            ExecuteStatusCache.putStatus(rerunScheduleId, dataTime, TaskStatus.preparing);
                        }
                    } else {
                        LOGGER.warn("schedule [" + scheduleId + "] is not period schedule. skip rerun.");
                    }
                }
                Long executeId = runPatch(scheduleId, targetScheduleTime, sourceScheduleTime, dispatchToStorage);
                List<Map<String, Object>> result = new ArrayList<>();
                Map<String, Object> patchResult = new HashMap<>();
                patchResult.put("patch_exec_id", executeId);
                result.add(patchResult);
                for (TaskEvent event : rerunExecutes) {
                    sendStatusEvent(event);
                }
                for (TaskEvent event : rerunExecutes) {
                    Map<String, Object> context = event.getContext().toHttpJson();
                    result.add(context);
                }
                writeData(true, "patch is preparing, id is " + executeId, HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(result), response);
            } catch (Exception e) {
                LOGGER.error("patch execute error, detail: ", e);
                writeData(false, "patch execute error, detail: " + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        }
    }

    private Long runPatch(String scheduleId, Long targetScheduleTime, Long sourceScheduleTime,
            boolean dispatchToStorage) throws Exception {
        AbstractJobDao dao = MetaDataManager.getJobDao();
        ExecuteResult executeResult = dao.queryLatestExecuteResult(scheduleId, targetScheduleTime);
        TaskStatus status = (executeResult != null) ? executeResult.getStatus() : TaskStatus.none;
        if (status == TaskStatus.running || status == TaskStatus.finished || status == TaskStatus.recovering) {
            throw new NaviException("[" + scheduleId + "][" + targetScheduleTime + "][" + status.name()
                    + "] is not allowed to be patched");
        }
        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo info = schedule.getScheduleInfo(scheduleId);
        if (info == null) {
            throw new NaviException("Cannot find schedule [" + scheduleId + "]. ");
        }
        DefaultTaskEvent event = schedule.buildPreparingEvent(info, targetScheduleTime);
        if (executeResult == null || status != TaskStatus.preparing) {
            LOGGER.info("add new execute:[" + event.getContext().getExecuteInfo().getId() + "][" + scheduleId + "]");
            MetaDataManager.getJobDao().addExecute(event);
        } else {
            event.getContext().getExecuteInfo().setId(executeResult.getExecuteInfo().getId());
        }
        event.setChangeStatus(TaskStatus.running);
        event.setEventName("patching");
        String extraInfo = event.getContext().getTaskInfo().getExtraInfo();
        Map<String, Object> extraInfoMap = JsonUtils.readMap(extraInfo);
        extraInfoMap.put("makeup_rt_id", scheduleId);
        extraInfoMap.put("type", "data_makeup");
        extraInfoMap.put("source_schedule_time", sourceScheduleTime);
        extraInfoMap.put("dispatch_to_storage", dispatchToStorage);
        LOGGER.info("patch extra info for:" + "[" + scheduleId + "][" + targetScheduleTime + "] is " + JsonUtils
                .writeValueAsString(extraInfoMap));
        event.getContext().setEventInfo(JsonUtils.writeValueAsString(extraInfoMap));
        TaskStateManager.dispatchEvent(event);
        long executeId = event.getContext().getExecuteInfo().getId();
        LOGGER.info("patch execute id is " + executeId);
        return executeId;
    }

    private void sendStatusEvent(TaskEvent event) throws NaviException {
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        long executeId = event.getContext().getExecuteInfo().getId();
        long scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
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

    private void generateRerunExecute(List<String> scheduleIdList, String model, Long startTime, Long endTime,
            List<TaskEvent> executes) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        Set<String> visited = new HashSet<>(scheduleIdList);
        Queue<String> bfsQueue = new LinkedList<>(visited);
        while (!bfsQueue.isEmpty()) {
            String scheduleId = bfsQueue.poll();
            if (scheduleId == null) {
                continue;
            }
            ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
            if (scheduleInfo == null) {
                LOGGER.warn("Can not find schedule [" + scheduleId + "]");
                continue;
            }

            Period period = schedule.getScheduleInfo(scheduleId).getPeriod();
            if (period == null) {
                LOGGER.warn("schedule [" + scheduleId + "] is not period schedule. skip patch.");
                continue;
            }

            if (!schedule.getScheduleInfo(scheduleId).isActive()) {
                LOGGER.warn("schedule [" + scheduleId + "] is disabled. skip rerun.");
                continue;
            }

            Long nextScheduleTime = schedule.getScheduleTimes().get(scheduleId);
            if (nextScheduleTime != null && endTime > nextScheduleTime) {
                String prettyTime = CronUtil
                        .getPrettyTime(nextScheduleTime, "yyyy-MM-dd HH:mm:ss", period.getTimezone());
                LOGGER.warn("schedule [" + scheduleId + "] at schedule time " + prettyTime + " is not scheduled.");
                continue;
            }
            Long currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, startTime, false);
            while (currentScheduleTime <= endTime) {
                String currentPrettyTime = CronUtil
                        .getPrettyTime(currentScheduleTime, "yyyy-MM-dd HH:mm:ss", period.getTimezone());
                AbstractJobDao dao = MetaDataManager.getJobDao();
                TaskStatus status = dao.getExecuteStatus(scheduleId, currentScheduleTime);
                if (status == TaskStatus.preparing) {
                    LOGGER.info("schedule [" + scheduleId + "] rerun at time : " + currentScheduleTime
                            + " is already waiting");
                } else if (status == TaskStatus.running || status == TaskStatus.decommissioned
                        || status == TaskStatus.recovering) {
                    String errMsg = "schedule [" + scheduleId + "] downstream at schedule time " + currentPrettyTime
                            + " which is not finished is not allowed to be rerun";
                    LOGGER.error(errMsg);
                    throw new NaviException(errMsg);
                } else {
                    TaskEvent preparingEvent = schedule.buildPreparingEvent(scheduleInfo, currentScheduleTime);
                    if (!isExecuteContain(preparingEvent, executes)) {
                        LOGGER.info("add schedule [" + scheduleId + "] rerun at time : " + currentScheduleTime);
                        executes.add(preparingEvent);
                    }
                }
                currentScheduleTime = currentScheduleTime + 1;
                currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, currentScheduleTime, false);
            }
            if ("all_child".equals(model)) {
                AbstractJobDao dao = MetaDataManager.getJobDao();
                List<String> children = dao.getChildrenByParentId(scheduleId);
                // only re run active child
                for (String child : children) {
                    if (!visited.contains(child)
                            && null != schedule.getScheduleInfo(child)
                            && schedule.getScheduleInfo(child).isActive()) {
                        visited.add(child);
                        bfsQueue.add(child);
                    }
                }
            }
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
}

