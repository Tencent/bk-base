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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http.admin;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
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
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class ScheduleToolHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(ScheduleToolHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        if ("calculate_schedule_task_time".equals(operate)) {
            String rerunListStr = (String) params.get("rerun_processings");
            String rerunModel = (String) params.get("rerun_model");
            Long startTime = (Long) params.get("start_time");
            Long endTime = (Long) params.get("end_time");
            List<String> rerunList = Arrays.asList(rerunListStr.split(","));
            final Set<TaskStatus> excludeStatuses = new HashSet<>();
            String excludeStatusesStr = request.getParameter("exclude_statuses");
            if (excludeStatusesStr != null) {
                for (String excludeStatus : excludeStatusesStr.split(",")) {
                    excludeStatuses.add(TaskStatus.valueOf(excludeStatus));
                }
            }
            List<TaskInfo> taskInfoList = new ArrayList<>();
            try {
                generateTask(rerunList, rerunModel, startTime, endTime, taskInfoList, excludeStatuses);
            } catch (NaviException e) {
                writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
            //generated result message
            List<Map<String, Object>> result = new ArrayList<>();
            for (TaskInfo taskInfo : taskInfoList) {
                Map<String, Object> map = new HashMap<>();
                map.put("schedule_id", taskInfo.getScheduleId());
                map.put("schedule_time", taskInfo.getScheduleTime());
                result.add(map);
            }
            writeData(true, "rerun success.", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(result), response);
        }
    }

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        if ("query_failed_executes".equals(operate)) {
            try {
                long beginTime = Long.parseLong(request.getParameter("begin_time"));
                long endTime = Long.parseLong(request.getParameter("end_time"));
                String typeId = request.getParameter("type_id") != null ? request.getParameter("type_id") : "";
                List<ExecuteResult> executeResults = dao.queryFailedExecutes(beginTime, endTime, typeId);
                List<Map<String, Object>> results = new ArrayList<>();
                for (ExecuteResult executeResult : executeResults) {
                    results.add(executeResult.toHTTPJson());
                }
                writeData(true, "query failed execute success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(results), response);
            } catch (Exception e) {
                LOGGER.error("query failed execute error.", e);
                writeData(false, "query failed execute error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        }
    }

    private void generateTask(List<String> scheduleIdList, String model, Long startTime, Long endTime,
            List<TaskInfo> taskInfoList, Set<TaskStatus> excludeStatuses) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        //find heads
        List<String> heads = getHeads(scheduleIdList, endTime);
        //generate heads taskInfo
        for (String scheduleId : heads) {
            ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
            Period period = scheduleInfo.getPeriod();
            Long currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, startTime, false);
            while (currentScheduleTime <= endTime) {
                TaskInfo taskInfo = schedule.buildTaskInfo(scheduleInfo, currentScheduleTime);
                generateTaskInfo(taskInfo, period.getTimezone(), taskInfoList, excludeStatuses);
                currentScheduleTime = currentScheduleTime + 1;
                currentScheduleTime = schedule.calcNextScheduleTime(scheduleId, currentScheduleTime, false);
            }
        }
        //generate children taskInfo
        generateChildTaskInfoList(taskInfoList, scheduleIdList, model, excludeStatuses);
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

    private boolean generateTaskInfo(TaskInfo taskInfo, TimeZone timeZone,
        List<TaskInfo> generatedTaskInfoList, Set<TaskStatus> excludeStatuses) throws NaviException {
        String scheduleId = taskInfo.getScheduleId();
        long scheduleTime = taskInfo.getScheduleTime();
        String schedulePrettyTime = CronUtil.getPrettyTime(scheduleTime, "yyyy-MM-dd HH:mm:ss", timeZone);
        if (needRerun(scheduleId, scheduleTime, schedulePrettyTime, excludeStatuses)) {
            if (!isTaskContain(taskInfo, generatedTaskInfoList)) {
                LOGGER.info("add schedule [" + scheduleId + "] rerun at time : " + schedulePrettyTime);
                generatedTaskInfoList.add(taskInfo);
            }
            return true;
        }
        return false;
    }

    private void generateChildTaskInfoList(List<TaskInfo> generatedTaskInfoList, List<String> rerunIdList,
        String rerunModel, Set<TaskStatus> excludeStatuses) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        Queue<TaskInfo> bfsQueue = new LinkedList<>(generatedTaskInfoList);
        while (!bfsQueue.isEmpty()) {
            TaskInfo taskInfo = bfsQueue.poll();
            if (taskInfo == null) {
                continue;
            }
            String scheduleId = taskInfo.getScheduleId();
            long dataTime = taskInfo.getDataTime();
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
                    if (generateTaskInfo(childTaskInfo, period.getTimezone(), generatedTaskInfoList, excludeStatuses)) {
                        bfsQueue.add(childTaskInfo);
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

    private boolean isTaskContain(TaskInfo task, List<TaskInfo> taskInfoList) {
        for (TaskInfo taskInfo : taskInfoList) {
            if (taskInfo.getScheduleId().equals(task.getScheduleId()) && taskInfo.getScheduleTime() == task
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
}
