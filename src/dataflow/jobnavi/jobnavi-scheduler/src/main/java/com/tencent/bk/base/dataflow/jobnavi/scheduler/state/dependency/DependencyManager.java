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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyConstants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.AbstractDependencyParam;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyRule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodKeyword;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.EvalUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import javax.script.ScriptException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class DependencyManager {

    public static final Logger LOGGER = Logger.getLogger(DependencyManager.class);

    public static boolean isDependency(String scheduleId) {
        ScheduleInfo scheduleInfo = ScheduleManager.getSchedule().getScheduleInfo(scheduleId);
        return scheduleInfo != null && scheduleInfo.getParents().size() > 0;
    }

    /**
     * <p>generate children event by parent schedule scheduleId.</p>
     * <p>send children task running event if status is preparing</p>
     *
     * @param scheduleId schedule scheduleId
     * @return list of task running events
     * @throws NaviException generate event error
     */
    public static List<TaskEvent> generateChildrenEvents(String scheduleId) throws NaviException {
        List<TaskEvent> runningEvents = new ArrayList<>();
        AbstractJobDao dao = MetaDataManager.getJobDao();
        List<String> children = dao.getChildrenByParentId(scheduleId);
        for (String child : children) {
            ScheduleInfo scheduleInfo = ScheduleManager.getSchedule().getScheduleInfo(child);
            List<ExecuteResult> results = dao.queryPreparingExecute(child);
            for (ExecuteResult result : results) {
                //Child task status will be checked by RunningProcessor
                DefaultTaskEvent runningEvent = buildEvent(TaskStatus.running, result, scheduleInfo);
                runningEvents.add(runningEvent);
            }
        }
        return runningEvents;
    }

    /**
     * <p>generate children event by parent schedule scheduleId.</p>
     * <p>send children task running event if status is preparing</p>
     *
     * @param scheduleId schedule scheduleId
     * @return list of task running events
     * @throws NaviException generate event error
     */
    public static List<TaskEvent> generateChildrenEvents(String scheduleId, long dataTime) throws NaviException {
        if (dataTime == 0) {
            LOGGER.warn("Data time of schedule:" + scheduleId + " is not set");
            return generateChildrenEvents(scheduleId);
        }
        List<TaskEvent> runningEvents = new ArrayList<>();
        AbstractJobDao dao = MetaDataManager.getJobDao();
        List<String> children = dao.getChildrenByParentId(scheduleId);
        for (String child : children) {
            ScheduleInfo childInfo = ScheduleManager.getSchedule().getScheduleInfo(child);
            DependencyInfo dependencyInfo = null;
            if (childInfo.getParents() != null) {
                for (DependencyInfo parent : childInfo.getParents()) {
                    if (scheduleId.equals(parent.getParentId())) {
                        dependencyInfo = parent;
                    }
                }
            }
            if (dependencyInfo == null) {
                String errMsg = "Failed to generate children events. Dependence info of child:" + child
                        + " was changed.";
                LOGGER.error(errMsg);
                throw new NaviException(errMsg);
            }
            List<Long> times = getDependChildrenScheduleTimes(dataTime, dependencyInfo, childInfo);
            if (times != null) {
                Long[] timeRange = new Long[2];
                timeRange[0] = times.get(0);
                timeRange[1] = times.get(times.size() - 1);
                LOGGER.info(String.format("[%s][dataTime:%d] child:[%s] schedule time range:%s ~ %s",
                        scheduleId, dataTime, child, CronUtil.getPrettyTime(timeRange[0]),
                        CronUtil.getPrettyTime(timeRange[1])));
                List<ExecuteResult> results = dao.queryExecuteResultByTimeRange(child, timeRange);
                for (ExecuteResult result : results) {
                    //Child task status will be checked by RunningProcessor
                    if (result.getStatus() == TaskStatus.preparing || result.getStatus() == TaskStatus.recovering) {
                        DefaultTaskEvent runningEvent = buildEvent(TaskStatus.running, result, childInfo);
                        runningEvents.add(runningEvent);
                    }
                }
            }
        }
        return runningEvents;
    }

    /**
     * <p>generate children task info by parent schedule scheduleId.</p>
     *
     * @param scheduleId schedule scheduleId
     * @return list of children task info
     * @throws NaviException generate children task info error
     */
    public static List<TaskInfo> generateChildrenTaskInfo(String scheduleId, long dataTime) throws NaviException {
        if (dataTime == 0) {
            LOGGER.warn("Data time of schedule:" + scheduleId + " is not set");
            return null;
        }
        List<TaskInfo> taskInfoList = new ArrayList<>();
        AbstractJobDao dao = MetaDataManager.getJobDao();
        List<String> children = dao.getChildrenByParentId(scheduleId);
        for (String child : children) {
            ScheduleInfo childInfo = ScheduleManager.getSchedule().getScheduleInfo(child);
            if (!childInfo.isActive()) {
                continue;
            }
            DependencyInfo dependencyInfo = null;
            if (childInfo.getParents() != null) {
                for (DependencyInfo parent : childInfo.getParents()) {
                    if (scheduleId.equals(parent.getParentId())) {
                        dependencyInfo = parent;
                    }
                }
            }
            if (dependencyInfo == null) {
                String errMsg = "Failed to generate children events. Dependence info of child:" + child
                        + " was changed.";
                LOGGER.error(errMsg);
                throw new NaviException(errMsg);
            }
            List<Long> times = getDependChildrenScheduleTimes(dataTime, dependencyInfo, childInfo);
            if (times != null) {
                LOGGER.info(String.format("[%s][dataTime:%d] child:[%s] schedule time range:%s ~ %s",
                        scheduleId, dataTime, child, CronUtil.getPrettyTime(times.get(0)),
                        CronUtil.getPrettyTime(times.get(times.size() - 1))));
                for (Long scheduleTime : times) {
                    TaskInfo childTaskInfo = ScheduleManager.getSchedule().buildTaskInfo(childInfo, scheduleTime);
                    taskInfoList.add(childTaskInfo);
                }
            }
        }
        return taskInfoList;
    }

    /**
     * get dependency status of given schedule
     *
     * @param scheduleId
     * @param scheduleTime
     * @return dependency status
     * @throws NaviException
     */
    public static DependencyResult getDependencyStatus(String scheduleId, long scheduleTime) throws NaviException {
        Map<DependencyResult, Integer> statusPriority = new HashMap<>();
        statusPriority.put(DependencyResult.ready, 0);
        statusPriority.put(DependencyResult.waiting, 1);
        statusPriority.put(DependencyResult.running, 1);
        statusPriority.put(DependencyResult.not_scheduled, 2);
        statusPriority.put(DependencyResult.terminated, 3);
        statusPriority.put(DependencyResult.skipped, 4);
        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
        List<DependencyInfo> parents = scheduleInfo.getParents();
        DependencyResult overallDependencyStatus = DependencyResult.ready;
        //check dependence status for all parents
        for (DependencyInfo parent : parents) {
            DependencyResult parentStatus = getParentStatus(scheduleInfo, parent, scheduleTime);
            if (statusPriority.containsKey(overallDependencyStatus) && statusPriority.containsKey(parentStatus)
                    && statusPriority.get(parentStatus) >= statusPriority.get(overallDependencyStatus)) {
                overallDependencyStatus = parentStatus;
            }
        }
        return overallDependencyStatus;
    }

    /**
     * get parent dependence status
     *
     * @param scheduleInfo
     * @param parent
     * @param scheduleTime
     * @return parent dependence status
     * @throws NaviException
     */
    public static DependencyResult getParentStatus(ScheduleInfo scheduleInfo, DependencyInfo parent, long scheduleTime)
            throws NaviException {
        String scheduleId = scheduleInfo.getScheduleId();
        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo parentInfo = schedule.getScheduleInfo(parent.getParentId());
        if (parentInfo == null) {
            LOGGER.info("Cannot find parent scheduleInfo, skipped.");
            return DependencyResult.skipped;
        }
        DependencyResult parentStatus;
        if (parent.getRule() == DependencyRule.self_finished || parent.getRule() == DependencyRule.self_no_failed) {
            parentStatus = getSelfDependenceStatus(scheduleId, scheduleTime, parent.getRule());
        } else {
            Long[] range = getDependParentDataTimeRange(scheduleTime, parent, scheduleInfo.getPeriod().getTimezone());
            LOGGER.info(String.format("[%s][%d] parent:[%s] data time range:[%s, %s)",
                    scheduleId, scheduleTime, parent.getParentId(), CronUtil.getPrettyTime(range[0]),
                    CronUtil.getPrettyTime(range[1])));
            switch (parent.getRule()) {
                case all_finished:
                    parentStatus = getDependenceStatusAllFinished(scheduleInfo, parentInfo, range);
                    break;
                case at_least_one_finished:
                    parentStatus = getDependenceStatusOneFinished(scheduleInfo, parentInfo, range);
                    break;
                case all_failed:
                    parentStatus = getDependenceStatusAllFailed(scheduleInfo, parentInfo, range);
                    break;
                case no_failed:
                    parentStatus = getDependenceStatusNoFailed(scheduleInfo, parentInfo, range);
                    break;
                default:
                    throw new NaviException("unsupported dependency rule:" + parent.getRule());
            }
        }
        return parentStatus;
    }

    /**
     * get depend parent data time range
     *
     * @param scheduleTime
     * @param dependencyInfo
     * @param timeZone
     * @return
     */
    public static Long[] getDependParentDataTimeRange(long scheduleTime, DependencyInfo dependencyInfo,
            TimeZone timeZone) {
        Long[] parentDataTimesRange = new Long[2];
        AbstractDependencyParam param = getDependencyParam(scheduleTime, dependencyInfo, timeZone);
        long endTime = param.getDependenceEndTime();
        long startTime = param.getDependenceStartTime();

        ScheduleInfo parentScheduleInfo = ScheduleManager.getSchedule().getScheduleInfo(dependencyInfo.getParentId());
        if (parentScheduleInfo.getPeriod() != null && parentScheduleInfo.getPeriod().getFirstScheduleTime() != null) {
            long parentStartTime = parentScheduleInfo.getPeriod().getFirstScheduleTime().getTime();
            long parentFirstDataTime = ScheduleManager.getSchedule()
                    .calculateDataTime(parentScheduleInfo, parentStartTime);
            if (startTime < parentFirstDataTime && parentFirstDataTime < endTime) {
                startTime = parentFirstDataTime;
            }
        }

        parentDataTimesRange[0] = startTime;
        parentDataTimesRange[1] = endTime;

        return parentDataTimesRange;
    }

    /**
     * get depend parent data time range
     *
     * @param scheduleId
     * @param scheduleTime
     * @param parentId
     * @return
     */
    public static Long[] getDependParentDataTimeRange(String scheduleId, long scheduleTime, String parentId) {
        Long[] parentDataTimesRange = null;
        ScheduleInfo childScheduleInfo = ScheduleManager.getSchedule().getScheduleInfo(scheduleId);
        if (childScheduleInfo != null && childScheduleInfo.getPeriod() != null) {
            List<DependencyInfo> dependencyInfoList = childScheduleInfo.getParents();
            if (dependencyInfoList != null) {
                DependencyInfo dependencyInfo = null;
                for (DependencyInfo info : dependencyInfoList) {
                    if (parentId.equals(info.getParentId())) {
                        dependencyInfo = info;
                        break;
                    }
                }
                if (dependencyInfo != null) {
                    AbstractDependencyParam param = getDependencyParam(scheduleTime, dependencyInfo,
                                                                       childScheduleInfo.getPeriod().getTimezone());
                    long beginDataTime = param.getDependenceStartTime();
                    long endDataTime = param.getDependenceEndTime();
                    parentDataTimesRange = new Long[2];
                    parentDataTimesRange[0] = beginDataTime;
                    parentDataTimesRange[1] = endDataTime;
                }
            }
        }
        return parentDataTimesRange;
    }

    private static DependencyResult getDependenceStatusAllFinished(ScheduleInfo scheduleInfo,
        ScheduleInfo parentInfo, Long[] dataTimeRange) throws NaviException {
        Set<TaskStatus> terminatedStatusSet = new HashSet<>();
        //dependence is determined not ready if some of the parent executes are not finished
        terminatedStatusSet.add(TaskStatus.failed);
        terminatedStatusSet.add(TaskStatus.failed_succeeded);
        terminatedStatusSet.add(TaskStatus.killed);
        terminatedStatusSet.add(TaskStatus.disabled);
        terminatedStatusSet.add(TaskStatus.skipped);
        Map<DependencyResult, Integer> statusPriority = new HashMap<>();
        //wait until at least one of the parent executes failed/failed_succeeded or all parent executes end
        statusPriority.put(DependencyResult.ready, 0);
        statusPriority.put(DependencyResult.waiting, 1);
        statusPriority.put(DependencyResult.running, 1);
        statusPriority.put(DependencyResult.terminated, 2);
        statusPriority.put(DependencyResult.not_scheduled, 3);
        Set<TaskStatus> keyStatusSet = new HashSet<>(terminatedStatusSet);
        return getDependenceStatus(scheduleInfo, parentInfo, dataTimeRange, DependencyResult.ready,
                                   keyStatusSet, null, terminatedStatusSet, statusPriority);
    }

    private static DependencyResult getDependenceStatusOneFinished(ScheduleInfo scheduleInfo,
        ScheduleInfo parentInfo, Long[] dataTimeRange) throws NaviException {
        //dependence is determined ready if at least one of the parent executes is finished
        Set<TaskStatus> readyStatusSet = new HashSet<>();
        readyStatusSet.add(TaskStatus.finished);
        //wait until all parent executes end
        Map<DependencyResult, Integer> statusPriority = new HashMap<>();
        statusPriority.put(DependencyResult.terminated, 0);
        statusPriority.put(DependencyResult.ready, 1);
        statusPriority.put(DependencyResult.waiting, 2);
        statusPriority.put(DependencyResult.running, 2);
        statusPriority.put(DependencyResult.not_scheduled, 3);
        Set<TaskStatus> keyStatusSet = new HashSet<>();
        return getDependenceStatus(scheduleInfo, parentInfo, dataTimeRange, DependencyResult.terminated,
                                   keyStatusSet, readyStatusSet, null, statusPriority);
    }

    private static DependencyResult getDependenceStatusAllFailed(ScheduleInfo scheduleInfo,
        ScheduleInfo parentInfo, Long[] dataTimeRange) throws NaviException {
        Set<TaskStatus> terminatedStatusSet = new HashSet<>();
        //dependence is determined not ready if some of the parent executes
        // are finished/failed_succeeded/killed/disabled/skipped
        terminatedStatusSet.add(TaskStatus.finished);
        terminatedStatusSet.add(TaskStatus.failed_succeeded);
        terminatedStatusSet.add(TaskStatus.killed);
        terminatedStatusSet.add(TaskStatus.disabled);
        terminatedStatusSet.add(TaskStatus.skipped);
        Map<DependencyResult, Integer> statusPriority = new HashMap<>();
        //wait until at least one of the parent executes finished or all parent executes end
        statusPriority.put(DependencyResult.ready, 0);
        statusPriority.put(DependencyResult.waiting, 1);
        statusPriority.put(DependencyResult.running, 1);
        statusPriority.put(DependencyResult.terminated, 2);
        statusPriority.put(DependencyResult.not_scheduled, 3);
        Set<TaskStatus> keyStatusSet = new HashSet<>(terminatedStatusSet);
        return getDependenceStatus(scheduleInfo, parentInfo, dataTimeRange, DependencyResult.ready,
                                   keyStatusSet, null, terminatedStatusSet, statusPriority);
    }

    private static DependencyResult getDependenceStatusNoFailed(ScheduleInfo scheduleInfo,
        ScheduleInfo parentInfo, Long[] dataTimeRange) throws NaviException {
        Set<TaskStatus> terminatedStatusSet = new HashSet<>();
        //dependence is determined not ready if some of the parent executes
        // are not finished or failed_succeeded
        terminatedStatusSet.add(TaskStatus.failed);
        terminatedStatusSet.add(TaskStatus.killed);
        terminatedStatusSet.add(TaskStatus.disabled);
        terminatedStatusSet.add(TaskStatus.skipped);
        Map<DependencyResult, Integer> statusPriority = new HashMap<>();
        //wait until at least one of the parent executes failed or all parent executes end
        statusPriority.put(DependencyResult.ready, 0);
        statusPriority.put(DependencyResult.waiting, 1);
        statusPriority.put(DependencyResult.running, 1);
        statusPriority.put(DependencyResult.terminated, 2);
        statusPriority.put(DependencyResult.not_scheduled, 3);
        Set<TaskStatus> keyStatusSet = new HashSet<>(terminatedStatusSet);
        return getDependenceStatus(scheduleInfo, parentInfo, dataTimeRange, DependencyResult.ready,
                                   keyStatusSet, null, terminatedStatusSet, statusPriority);
    }

    /**
     * check parent task status in given data time range and determine current dependence status
     *
     * @param scheduleInfo current schedule info
     * @param parentInfo parent schedule info
     * @param dataTimeRange given data time range
     * @param initialStatus initial status of current dependence
     * @param keyStatusSet parent execute statuses that determine current dependence status immediately
     * @param readyStatusSet parent execute statuses that considered to be ready
     * @param terminatedStatusSet parent execute statuses that considered to be terminated
     * @param statusPriority priority between different status
     * @return current dependence status
     * @throws NaviException
     */
    private static DependencyResult getDependenceStatus(ScheduleInfo scheduleInfo,
            ScheduleInfo parentInfo,
            Long[] dataTimeRange,
            DependencyResult initialStatus,
            Set<TaskStatus> keyStatusSet,
            Set<TaskStatus> readyStatusSet,
            Set<TaskStatus> terminatedStatusSet,
            Map<DependencyResult, Integer> statusPriority) throws NaviException {
        //check all status of parent instances in given data time range
        String scheduleId = scheduleInfo.getScheduleId();
        Schedule schedule = ScheduleManager.getSchedule();
        AbstractJobDao dao = MetaDataManager.getJobDao();
        String parentId = parentInfo.getScheduleId();
        DependencyResult overallDependencyStatus = initialStatus;
        if (schedule.getScheduleInfo(parentId).getPeriod() != null) {
            //check status of period schedule
            Long parentDataTime = schedule.calcNextDataTime(parentInfo, dataTimeRange[0]);
            if (parentDataTime == null || parentDataTime >= dataTimeRange[1]) {
                // out of depend time range, set status skipped.
                LOGGER.info(scheduleId + " depend parent " + parentId + " range:[" + dataTimeRange[0] + ","
                        + dataTimeRange[1] + ")"
                        + ", But calc parentDataTime is " + parentDataTime + ", skipped.");
                return DependencyResult.skipped;
            }
            while (parentDataTime != null && parentDataTime < dataTimeRange[1]) {
                TaskStatus status = queryExecuteStatusByDataTime(parentId, parentDataTime);
                LOGGER.info("scheduleId [" + scheduleId + "] parent [" + parentId + "], dataTime ["
                        + parentDataTime + "] status is " + status);
                DependencyResult dependencyStatus = getDependenceStatus(status, readyStatusSet, terminatedStatusSet);
                //if current parent execute status is in the key status set, return the dependence status immediately
                //otherwise dependence status is determined when all parent execute status is checked
                if (keyStatusSet != null && keyStatusSet.contains(status)) {
                    return dependencyStatus;
                } else if (dependencyStatus != DependencyResult.none) {
                    if (statusPriority.containsKey(overallDependencyStatus) && statusPriority
                            .containsKey(dependencyStatus)
                            && statusPriority.get(dependencyStatus) >= statusPriority.get(overallDependencyStatus)) {
                        overallDependencyStatus = dependencyStatus;
                    }
                }
                parentDataTime = schedule.calcNextDataTime(parentInfo, parentDataTime + 1);
            }
        } else {
            List<ExecuteResult> results = dao.queryExecuteResultByDataTimeRange(parentId, dataTimeRange);
            if (results == null || results.size() == 0) {
                return DependencyResult.not_scheduled;
            }
            for (ExecuteResult result : results) {
                long parentDataTime = result.getDataTime();
                TaskStatus status = result.getStatus();
                LOGGER.info("scheduleId [" + scheduleId + "] parent [" + parentId + "], dataTime ["
                        + parentDataTime + "] status is " + status);
                DependencyResult dependencyStatus = getDependenceStatus(status, readyStatusSet, terminatedStatusSet);
                //if current parent execute status is in the key status set, return the dependence status immediately
                //otherwise dependence status is determined when all parent execute status is checked
                if (keyStatusSet != null && keyStatusSet.contains(status)) {
                    return dependencyStatus;
                } else if (dependencyStatus != DependencyResult.none) {
                    overallDependencyStatus = dependencyStatus;
                }
            }
        }
        return overallDependencyStatus;
    }

    /**
     * check parent task status of given execute and determine current dependence status
     *
     * @param parentStatus parent status
     * @param readyStatusSet return DependencyResult.ready when parent status is in the given set
     * @param terminatedStatusSet return DependencyResult.terminated when parent status is in the given set
     * @return current dependence status
     */
    private static DependencyResult getDependenceStatus(TaskStatus parentStatus,
            Set<TaskStatus> readyStatusSet,
            Set<TaskStatus> terminatedStatusSet) {
        if (readyStatusSet != null && readyStatusSet.contains(parentStatus)) {
            return DependencyResult.ready;
        } else if (terminatedStatusSet != null && terminatedStatusSet.contains(parentStatus)) {
            return DependencyResult.terminated;
        } else if (parentStatus == TaskStatus.none) {
            return DependencyResult.not_scheduled; //parent task not scheduled yet
        } else if (parentStatus == TaskStatus.preparing || parentStatus == TaskStatus.lost) {
            return DependencyResult.waiting; //parent task is waiting to start
        } else if (parentStatus == TaskStatus.running || parentStatus == TaskStatus.decommissioned
                || parentStatus == TaskStatus.recovering || parentStatus == TaskStatus.retried) {
            return DependencyResult.running; //parent task is running
        }
        return DependencyResult.none;
    }

    private static DependencyResult getSelfDependenceStatus(String scheduleId, long scheduleTime,
            DependencyRule dependencyRule) throws NaviException {
        AbstractJobDao dao = MetaDataManager.getJobDao();
        ExecuteResult result = dao.queryLastExecuteResult(scheduleId, scheduleTime);
        if (result == null) {
            LOGGER.info("Cannot find " + scheduleId + " + last schedule, running schedule at " + scheduleTime);
            return DependencyResult.ready;
        }
        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo scheduleInfo = schedule.getScheduleInfo(scheduleId);
        long firstScheduleTime = scheduleInfo.getPeriod().getFirstScheduleTime().getTime();
        if (scheduleTime == firstScheduleTime) {
            return DependencyResult.ready;
        }
        long lastScheduleTime = (firstScheduleTime < scheduleTime) ? Math
                .max(result.getScheduleTime(), firstScheduleTime) : result.getScheduleTime();
        LOGGER.info(scheduleId + " last schedule time is: " + lastScheduleTime);
        long nextScheduleTime = schedule.calcNextScheduleTime(scheduleId, lastScheduleTime + 1, false);

        if (nextScheduleTime >= scheduleTime) {
            //is last
            TaskStatus status = result.getStatus();
            LOGGER.info(scheduleId + " last schedule status is: " + status);
            Set<TaskStatus> terminatedStatusSet = new HashSet<>();
            switch (dependencyRule) {
                case self_finished:
                    terminatedStatusSet.add(TaskStatus.failed);
                    terminatedStatusSet.add(TaskStatus.failed_succeeded);
                    terminatedStatusSet.add(TaskStatus.killed);
                    terminatedStatusSet.add(TaskStatus.disabled);
                    terminatedStatusSet.add(TaskStatus.skipped);
                    return getDependenceStatus(status, null, terminatedStatusSet);
                case self_no_failed:
                    terminatedStatusSet.add(TaskStatus.failed);
                    terminatedStatusSet.add(TaskStatus.killed);
                    terminatedStatusSet.add(TaskStatus.disabled);
                    terminatedStatusSet.add(TaskStatus.skipped);
                    return getDependenceStatus(status, null, terminatedStatusSet);
                default:
                    throw new NaviException("unsupported self dependence rule:" + dependencyRule);
            }
        } else {
            return DependencyResult.not_scheduled;
        }
    }

    /**
     * get dependence children schedule times
     *
     * @param dataTime
     * @param dependencyInfo
     * @param childInfo
     * @return dependence children schedule times
     * @throws NaviException
     */
    public static List<Long> getDependChildrenScheduleTimes(long dataTime, DependencyInfo dependencyInfo,
            ScheduleInfo childInfo) throws NaviException {
        Period childPeriod = childInfo.getPeriod();
        if (childPeriod == null) {
            LOGGER.info("Child:" + childInfo.getScheduleId() + " is not a period schedule");
            return null;
        }
        String childScheduleId = childInfo.getScheduleId();
        long windowOffsetMills = parseWindowOffset(dataTime,
                                                   dependencyInfo.getWindowOffset(),
                                                   childPeriod.getTimezone(),
                                                   true);
        final long millsInAnHour = 3600 * 1000;
        //the first schedule time of child which depends on dataTime
        Long nextScheduleTime = ScheduleManager.getSchedule()
                .calcNextScheduleTime(childScheduleId, dataTime + windowOffsetMills + millsInAnHour, false);
        if (nextScheduleTime == null) {
            LOGGER.info("Child:" + childInfo.getScheduleId() + " is not a period schedule");
            return null;
        }
        if ((dependencyInfo.getRule() == DependencyRule.self_finished
            || dependencyInfo.getRule() == DependencyRule.self_no_failed)
            && StringUtils.isEmpty(dependencyInfo.getWindowOffset())) {
            dependencyInfo = dependencyInfo.clone();
            dependencyInfo.setWindowOffset(
                dependencyInfo.getValue()); //self dependence has default window offset of 1 cycle
        }
        long startTime = getDependencyParam(nextScheduleTime, dependencyInfo, childPeriod.getTimezone())
                .getDependenceStartTime();
        if (startTime > dataTime) {
            return null;
        }
        List<Long> childrenScheduleTimes = new ArrayList<>();
        while (startTime <= dataTime) {
            childrenScheduleTimes.add(nextScheduleTime);
            nextScheduleTime = ScheduleManager.getSchedule()
                    .calcNextScheduleTime(childScheduleId, nextScheduleTime + 1, false);
            startTime = getDependencyParam(nextScheduleTime, dependencyInfo, childPeriod.getTimezone())
                    .getDependenceStartTime();
        }
        return childrenScheduleTimes;
    }

    /**
     * replace param key word
     *
     * @param periodStr
     * @param scheduleTime
     * @param timeZone
     * @return
     */
    public static String replaceKeyWord(String periodStr, long scheduleTime, TimeZone timeZone) throws NaviException {
        if (periodStr.contains(DependencyConstants.DEPENDENCY_KEYWORD_SYMBOL)) {
            String formula = periodStr.substring(0, periodStr.length() - 1);
            for (PeriodKeyword keyword : PeriodKeyword.values()) {
                if (formula.contains(keyword.toString())) {
                    final char periodUnit = periodStr.charAt(periodStr.length() - 1);
                    PeriodUnit unit = PeriodUnit.value(periodUnit);
                    int periodInt = PeriodUtil.getPeriodIntByKeyword(keyword, unit, scheduleTime, timeZone);
                    formula = formula
                            .replaceAll("\\" + DependencyConstants.DEPENDENCY_KEYWORD_SYMBOL + keyword.toString(),
                                    Integer.toString(periodInt));
                    try {
                        return EvalUtils.eval(formula).toString() + "" + periodUnit;
                    } catch (ScriptException e) {
                        throw new NaviException(e);
                    }
                }
            }
        }
        return periodStr;
    }

    public static long parseWindowOffset(long baseTime, String windowOffsetExpr, TimeZone timeZone, boolean forward) {
        long windowOffsetMills = 0;
        if (windowOffsetExpr != null) {
            for (String windowOffset : windowOffsetExpr.split("\\+")) {
                windowOffsetMills += CronUtil
                    .parsePeriodStringToMills(windowOffset, baseTime, timeZone, forward);
            }
        }
        return windowOffsetMills;
    }

    public static long parseWindowOffset(String windowOffsetExpr) {
        long windowOffsetMills = 0;
        if (windowOffsetExpr != null) {
            for (String windowOffset : windowOffsetExpr.split("\\+")) {
                windowOffsetMills += CronUtil.parsePeriodStringToMills(windowOffset);
            }
        }
        return windowOffsetMills;
    }

    private static TaskStatus queryExecuteStatusByDataTime(String scheduleId, long dataTime) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        Long lastScheduleDataTime = schedule.getLastScheduleDataTime(scheduleId);
        if (lastScheduleDataTime == null || dataTime <= lastScheduleDataTime) {
            //query from execute reference cache first
            TaskStatus status = ExecuteStatusCache.getStatus(scheduleId, dataTime);
            if (status == TaskStatus.none) {
                AbstractJobDao dao = MetaDataManager.getJobDao();
                ExecuteResult result = dao.queryLatestExecuteResultByDataTime(scheduleId, dataTime);
                status = result != null ? result.getStatus() : TaskStatus.none;
            } else {
                LOGGER.info("get execute status from cache:" + scheduleId + "~" + dataTime + "~" + status);
            }
            return status;
        }
        return TaskStatus.none;
    }

    /**
     * build new task event
     *
     * @param status
     * @param result
     * @param scheduleInfo
     * @return
     */
    private static DefaultTaskEvent buildEvent(TaskStatus status, ExecuteResult result, ScheduleInfo scheduleInfo)
            throws NaviException {
        TaskInfo info = new TaskInfo();
        info.setScheduleId(result.getScheduleId());
        info.setScheduleTime(result.getScheduleTime());
        info.setDataTime(result.getDataTime());
        info.setExtraInfo(scheduleInfo.getExtraInfo());
        info.setDecommissionTimeout(scheduleInfo.getDecommissionTimeout());
        info.setNodeLabel(scheduleInfo.getNodeLabel());
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        TaskType type = jobDao
                .queryTaskType(scheduleInfo.getTypeId(), scheduleInfo.getTypeTag(), scheduleInfo.getNodeLabel());
        if (type == null) {
            throw new NaviException("Cannot find task type [" + info.getType() + "].");
        }
        info.setType(type);

        TaskRecoveryInfo recoveryInfo = new TaskRecoveryInfo();
        recoveryInfo.setRecoveryEnable(scheduleInfo.getRecoveryInfo().isEnable());
        recoveryInfo.setRecoveryTimes(0);
        recoveryInfo.setMaxRecoveryTimes(scheduleInfo.getRecoveryInfo().getRetryTimes());
        recoveryInfo.setIntervalTime(scheduleInfo.getRecoveryInfo().getIntervalTime());
        info.setRecoveryInfo(recoveryInfo);
        return DefaultTaskEventBuilder
                .buildEvent(status.toString(), result.getExecuteInfo().getId(), result.getExecuteInfo().getHost(), info,
                        result.getExecuteInfo().getRank());
    }

    private static AbstractDependencyParam getDependencyParam(long scheduleTime, DependencyInfo dependencyInfo,
            TimeZone timeZone) {
        long windowOffset = parseWindowOffset(scheduleTime, dependencyInfo.getWindowOffset(), timeZone, false);
        long baseTime = scheduleTime - windowOffset;
        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo parentInfo = schedule.getScheduleInfo(dependencyInfo.getParentId());
        Period parentPeriod = parentInfo.getPeriod();
        if (parentPeriod != null) {
            return dependencyInfo.getParam(baseTime, timeZone, parentPeriod);
        } else {
            return dependencyInfo.getParam(baseTime, timeZone);
        }
    }
}
