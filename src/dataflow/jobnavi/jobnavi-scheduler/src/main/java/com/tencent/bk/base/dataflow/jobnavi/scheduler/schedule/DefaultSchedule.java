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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class DefaultSchedule implements Schedule {

    private static final Logger LOGGER = Logger.getLogger(DefaultSchedule.class);

    private final ConcurrentSkipListMap<String, ScheduleInfo> scheduleInfos = new ConcurrentSkipListMap<>();
    private final Object scheduleInfoLock = new Object();
    private final ConcurrentSkipListMap<String, Long> scheduleTime = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> lastScheduleDataTime = new ConcurrentSkipListMap<>();

    private final ConcurrentSkipListSet<String> forceScheduleLock = new ConcurrentSkipListSet<>();

    private Thread scheduleThread;
    private Configuration conf;

    @Override
    public void init(Configuration conf) throws NaviException {
        this.conf = conf;
        DefaultTaskEventBuilder.setDefaultEventRank(conf.getDouble(
                Constants.JOBNAVI_SCHEDULER_TASK_EVENT_RANK_BASE,
                Constants.JOBNAVI_SCHEDULER_TASK_EVENT_RANK_BASE_DEFAULT));
    }

    @Override
    public void start() throws NaviException {
        LOGGER.info("Schedule info loading...");
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        List<ScheduleInfo> scheduleInfoList = jobDao.listScheduleInfo();
        LOGGER.info(scheduleInfoList.size() + " Schedules loaded from DB");
        for (ScheduleInfo info : scheduleInfoList) {
            LOGGER.debug("load Schedule info: " + info.getScheduleId());
            scheduleInfos.put(info.getScheduleId(), info);
        }
        LOGGER.info("Schedules loaded into memory");

        LOGGER.info("Recovering last schedule time of schedules from DB...");
        Map<String, Long> currentScheduleTimeMap = jobDao.listCurrentScheduleTime();
        for (Map.Entry<String, Long> scheduleTimeEntry : currentScheduleTimeMap.entrySet()) {
            String scheduleId = scheduleTimeEntry.getKey();
            ScheduleInfo scheduleInfo = scheduleInfos.get(scheduleId);
            if (scheduleInfo.getPeriod() != null) {
                Long currentTime = scheduleTimeEntry.getValue();
                if (currentTime == null) {
                    boolean isExecuteBefore = conf.getBoolean(Constants.JOBNAVI_SCHEDULER_EXECUTE_BEFORE_ON_START,
                            Constants.JOBNAVI_SCHEDULER_EXECUTE_BEFORE_ON_START_DEFAULT);
                    currentTime = calcScheduleBeforeTime(scheduleId, isExecuteBefore);
                } else {
                    long now = System.currentTimeMillis();
                    if (currentTime + CronUtil.parsePeriodStringToMills(scheduleInfo.getPeriod().getDelay()) > now) {
                        addForceScheduleLock(scheduleId);
                    }
                    //set last schedule data time
                    lastScheduleDataTime.put(scheduleId, calculateDataTime(scheduleInfo, currentTime));
                    //current Time is scheduled, calculate next schedule time by plus one mills.
                    currentTime = currentTime + 1;
                }

                Long nextScheduleTime = calcNextScheduleTime(scheduleId, currentTime);
                if (nextScheduleTime != null) {
                    LOGGER.info("add schedule [" + scheduleId + "] , next schedule time is " + CronUtil
                            .getPrettyTime(nextScheduleTime) + " timestamp:" + nextScheduleTime);
                    scheduleTime.put(scheduleId, nextScheduleTime);
                }
            }
        }
        scheduleThread = new Thread(new ScheduleThread(), "schedule-thread");
        scheduleThread.start();
    }

    @Override
    public void stop() throws NaviException {
        scheduleInfos.clear();
        scheduleTime.clear();
        if (scheduleThread != null) {
            scheduleThread.interrupt();
        }
    }

    @Override
    public Long addSchedule(ScheduleInfo info) throws NaviException {
        scheduleInfos.put(info.getScheduleId(), info);
        if (info.isActive()) {
            startScheduleJob(info.getScheduleId());
            if (info.isExecOnCreate()) {
                TaskEvent event = buildPreparingEvent(info, System.currentTimeMillis());
                TaskStateManager.dispatchEvent(event);
                return event.getContext().getExecuteInfo().getId();
            }
        }
        return null;
    }

    @Override
    public void stopScheduleJob(String scheduleId) throws NaviException {
        scheduleInfos.get(scheduleId).setActive(false);
        LOGGER.info("stop schedule [" + scheduleId + "] success");
    }

    @Override
    public void startScheduleJob(String scheduleId) throws NaviException {
        ScheduleInfo info = scheduleInfos.get(scheduleId);
        if (info.getPeriod() != null) {
            long startTime = calcScheduleBeforeTime(scheduleId);
            lastScheduleDataTime.put(scheduleId, calculateDataTime(info, startTime));
            Long nextTime = calcNextScheduleTime(scheduleId, startTime);
            if (nextTime != null) {
                if (!info.isExecuteBeforeNow()) {
                    updateScheduleStartTime(scheduleId, nextTime);
                }
                LOGGER.info("startTime is " + startTime + " nextTime is " + nextTime);
                LOGGER.info("start schedule [" + scheduleId + "] success, next schedule time is " + CronUtil
                        .getPrettyTime(nextTime) + " timestamp:" + nextTime);
                scheduleTime.put(scheduleId, nextTime);
            }
        }
        info.setActive(true);
    }

    @Override
    public void deleteSchedule(String scheduleId) throws NaviException {
        scheduleInfos.remove(scheduleId);
        scheduleTime.remove(scheduleId);
        LOGGER.info("delete schedule [" + scheduleId + "] success");
    }

    @Override
    public void updateSchedule(ScheduleInfo info) throws NaviException {
        if (scheduleInfos.get(info.getScheduleId()) == null) {
            throw new NaviException("Can not find schedule:" + info.getScheduleId());
        }
        if (info.isActive()) {
            info.setActive(scheduleInfos.get(info.getScheduleId()).isActive());
            scheduleInfos.put(info.getScheduleId(), info);
            LOGGER.info("update need start schedule [" + info.getScheduleId() + "].");
            startScheduleJob(info.getScheduleId());
        } else {
            scheduleInfos.put(info.getScheduleId(), info);
            stopScheduleJob(info.getScheduleId());
        }
        LOGGER.info("update schedule [" + info.getScheduleId() + "] success");
    }

    @Override
    public ScheduleInfo getScheduleInfo(String scheduleId) {
        return scheduleInfos.get(scheduleId);
    }

    /**
     * <p>calculate next scheduleTime after before timestamp.</p>
     * <p>if before timestamp is scheduleTime, return before.</p>
     *
     * @param scheduleId
     * @param before
     * @return next schedule time
     * @throws NaviException calc schedule time error.
     */
    @Override
    public Long calcNextScheduleTime(String scheduleId, Long before) throws NaviException {
        return calcNextScheduleTime(scheduleId, before, true);
    }

    @Override
    public Long calcNextScheduleTime(String scheduleId, Long before, boolean allowStartTime) throws NaviException {
        ScheduleInfo info = scheduleInfos.get(scheduleId);
        if (before == null) {
            before = System.currentTimeMillis();
        }
        Period period = info.getPeriod();
        if (period == null) {
            throw new NaviException(scheduleId + " is not period schedule.");
        }

        TimeZone timeZone = period.getTimezone();
        String cronExpression = period.getCronExpression();
        if (StringUtils.isNotEmpty(cronExpression)) {
            //maybe before time is schedule time,change to before -1
            Date nextDate = CronUtil.parseCronExpression(cronExpression, timeZone)
                    .getNextValidTimeAfter(new Date(before - 1));
            if (nextDate == null) {
                return null;
            }
            return nextDate.getTime();
        } else if (period.getPeriodUnit() != null && period.getPeriodUnit() != PeriodUnit.once) {
            int frequency = period.getFrequency();
            long offset;
            if (period.getPeriodUnit() == PeriodUnit.month) {
                //calculate the nearest schedule datetime
                DateTime scheduleDateTime = new DateTime(period.getFirstScheduleTime());
                if (scheduleDateTime.getMillis() < before) {
                    while (scheduleDateTime.plusMonths(frequency).getMillis() < before) {
                        scheduleDateTime = scheduleDateTime.plusMonths((frequency));
                    }
                } else {
                    while (scheduleDateTime.minusMonths(frequency).getMillis() > before) {
                        scheduleDateTime = scheduleDateTime.minusMonths(frequency);
                    }
                }
                offset = before - scheduleDateTime.getMillis();
            } else {
                offset = (before - period.getFirstScheduleTime().getTime()) % (frequency * PeriodUtil
                        .getTimeMills(period.getPeriodUnit()));
            }
            if (before < period.getFirstScheduleTime().getTime()) {
                if (allowStartTime) {
                    return period.getFirstScheduleTime().getTime();
                } else {
                    return before - offset;
                }
            } else if (offset == 0) {
                return before;
            } else if (period.getPeriodUnit() == PeriodUnit.month) {
                return new DateTime(before - offset, DateTimeZone.forTimeZone(timeZone)).plusMonths(frequency).toDate()
                        .getTime();
            } else {
                return (before - offset) + (frequency * PeriodUtil.getTimeMills(period.getPeriodUnit()));
            }
        }
        return null;
    }

    /**
     * calculate next schedule data time of given schedule
     *
     * @param scheduleInfo
     * @param before
     * @return next data time
     * @throws NaviException
     */
    public Long calcNextDataTime(ScheduleInfo scheduleInfo, Long before) throws NaviException {
        String scheduleId = scheduleInfo.getScheduleId();
        Period period = scheduleInfo.getPeriod();
        if (period == null) {
            throw new NaviException(scheduleId + " is not period schedule.");
        }
        if (before == null) {
            return null;
        }
        final long millsInAnHour = 3600 * 1000;
        TimeZone timeZone = period.getTimezone();
        String cronExpression = period.getCronExpression();
        if (StringUtils.isNotEmpty(cronExpression)) {
            long dataTimeOffset = calculateDateTimeOffset(scheduleInfo, before, true);
            CronExpression ce = CronUtil.parseCronExpression(cronExpression, timeZone); //schedule time cron expression
            //before + dataTimeOffset maybe equals to current schedule time, minus 1 to get current schedule time
            Date nextScheduleDate = new Date(before + dataTimeOffset - 1);
            long nextDataTime;
            do {
                nextScheduleDate = ce.getNextValidTimeAfter(nextScheduleDate);
                if (nextScheduleDate == null) {
                    return null;
                }
                dataTimeOffset = calculateDateTimeOffset(scheduleInfo, nextScheduleDate.getTime(), false);
                nextDataTime = nextScheduleDate.getTime() - dataTimeOffset;
                nextDataTime = nextDataTime - nextDataTime % millsInAnHour; //aligned with HH:00:00.000
            } while (nextDataTime < before);
            return nextDataTime;
        } else if (period.getPeriodUnit() != null && period.getPeriodUnit() != PeriodUnit.once) {
            int frequency = period.getFrequency();
            long firstScheduleTime = period.getFirstScheduleTime().getTime();
            long dataTimeOffset = calculateDateTimeOffset(scheduleInfo, firstScheduleTime, false);
            long dataTime = firstScheduleTime - dataTimeOffset;
            dataTime = dataTime - dataTime % millsInAnHour; //aligned with HH:00:00.000
            if (period.getPeriodUnit() == PeriodUnit.month) {
                //plus/minus frequency*N months to find the first time greater or equal than before
                DateTime dataDateTime = new DateTime(dataTime, DateTimeZone.forTimeZone(timeZone));
                if (dataDateTime.getMillis() < before) {
                    while (dataDateTime.getMillis() < before) {
                        dataDateTime = dataDateTime.plusMonths((frequency));
                    }
                } else {
                    while (dataDateTime.minusMonths(frequency).getMillis() >= before) {
                        dataDateTime = dataDateTime.minusMonths(frequency);
                    }
                }
                return dataDateTime.getMillis();
            } else {
                //plus/minus N cycles to find the first time greater or equal than before
                if (dataTime < before) {
                    while (dataTime < before) {
                        dataTime += frequency * PeriodUtil.getTimeMills(period.getPeriodUnit());
                    }
                } else {
                    while (dataTime - frequency * PeriodUtil.getTimeMills(period.getPeriodUnit()) >= before) {
                        dataTime -= frequency * PeriodUtil.getTimeMills(period.getPeriodUnit());
                    }
                }
                return dataTime;
            }
        }
        return null;
    }

    @Override
    public long calculateDateTimeOffset(ScheduleInfo scheduleInfo, long baseTimeMills, boolean forward) {
        long dataTimeOffset;
        if (scheduleInfo.getDataTimeOffset() != null && !scheduleInfo.getDataTimeOffset().isEmpty()
                && scheduleInfo.getPeriod() != null) {
            long dataTime = baseTimeMills;
            for (String offset : scheduleInfo.getDataTimeOffset().split("\\+")) {
                //Data time offset may be M months + N days/hours. Calculate base on baseTimeMills
                dataTime -= CronUtil.parsePeriodStringToMills(offset, dataTime, scheduleInfo.getPeriod().getTimezone(),
                        forward) * (forward ? -1 : 1);
            }
            dataTimeOffset = (baseTimeMills - dataTime) * (forward ? -1 : 1);
        } else if (scheduleInfo.getPeriod() != null && scheduleInfo.getPeriod().getPeriodUnit() != null
                && scheduleInfo.getPeriod().getPeriodUnit() != PeriodUnit.once) {
            //data time offset is one cycle by default for period schedule
            int frequency = scheduleInfo.getPeriod().getFrequency();
            PeriodUnit periodUnit = scheduleInfo.getPeriod().getPeriodUnit();
            dataTimeOffset = CronUtil.parsePeriodStringToMills(frequency, periodUnit, baseTimeMills,
                    scheduleInfo.getPeriod().getTimezone(), forward);
        } else {
            //data time offset is one hour by default for none period schedule
            dataTimeOffset = PeriodUtil.getTimeMills(PeriodUnit.hour);
        }
        return dataTimeOffset;
    }

    @Override
    public long calculateDataTime(ScheduleInfo scheduleInfo, long scheduleTime) {
        long dataTimeOffset = calculateDateTimeOffset(scheduleInfo, scheduleTime, false);
        long dataTime = scheduleTime - dataTimeOffset;
        final long millsInAnHour = 3600 * 1000;
        return dataTime - dataTime % millsInAnHour;
    }

    /**
     *
     * build task info from schedule info and schedule time
     *
     * @param scheduleInfo
     * @param scheduleTime
     * @return task info
     * @throws NaviException
     */
    public TaskInfo buildTaskInfo(ScheduleInfo scheduleInfo, long scheduleTime) throws NaviException {
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        TaskType type = jobDao
                .queryTaskType(scheduleInfo.getTypeId(), scheduleInfo.getTypeTag(), scheduleInfo.getNodeLabel());
        if (type == null) {
            throw new NaviException(
                    "Cannot find task type [" + scheduleInfo.getTypeId() + "], tag:[" + scheduleInfo.getTypeTag()
                            + "].");
        }
        TaskInfo taskinfo = new TaskInfo();
        taskinfo.setScheduleId(scheduleInfo.getScheduleId());
        taskinfo.setExtraInfo(scheduleInfo.getExtraInfo());
        taskinfo.setScheduleTime(scheduleTime);
        taskinfo.setDataTime(calculateDataTime(scheduleInfo, scheduleTime));
        taskinfo.setDecommissionTimeout(scheduleInfo.getDecommissionTimeout());
        taskinfo.setNodeLabel(scheduleInfo.getNodeLabel());
        taskinfo.setType(type);

        TaskRecoveryInfo recovery = new TaskRecoveryInfo();
        recovery.setRecoveryTimes(0);
        if (scheduleInfo.getRecoveryInfo() != null) {
            recovery.setMaxRecoveryTimes(scheduleInfo.getRecoveryInfo().getRetryTimes());
            recovery.setIntervalTime(scheduleInfo.getRecoveryInfo().getIntervalTime());
            recovery.setRecoveryEnable(scheduleInfo.getRecoveryInfo().isEnable());
            taskinfo.setRecoveryInfo(recovery);
        }
        return taskinfo;
    }

    @Override
    public Long getLastScheduleDataTime(String scheduleId) {
        return lastScheduleDataTime.get(scheduleId);
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time, double rank) throws NaviException {
        DefaultTaskEvent taskEvent = buildPreparingEvent(info, time);
        taskEvent.getContext().getExecuteInfo().setRank(rank);
        return taskEvent;
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time) throws NaviException {
        TaskInfo taskinfo = buildTaskInfo(info, time);

        long executeId = TaskStateManager.newExecuteId();
        return DefaultTaskEventBuilder.buildEvent(TaskStatus.preparing.toString(), executeId, null, taskinfo);
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(TaskInfo taskInfo, double rank) {
        long executeId = TaskStateManager.newExecuteId();
        return DefaultTaskEventBuilder.buildEvent(TaskStatus.preparing.toString(), executeId, null, taskInfo, rank);
    }

    @Override
    public Collection<ScheduleInfo> getAllScheduleInfo() throws NaviException {
        return scheduleInfos.values();
    }

    @Override
    public Map<String, Long> getScheduleTimes() throws NaviException {
        return new HashMap<>(scheduleTime);
    }

    @Override
    public Long forceScheduleTask(String scheduleId) throws NaviException {
        synchronized (scheduleInfoLock) {
            if (forceScheduleLock.contains(scheduleId)) {
                throw new NaviException("Cannot force schedule task. schedule task has already execute.");
            }
            Long time = scheduleTime.get(scheduleId);
            ScheduleInfo info = scheduleInfos.get(scheduleId);
            if (info.isActive() && time != null) {
                Long executeId = scheduleTask(info, time);
                addForceScheduleLock(scheduleId);
                return executeId;
            }
            throw new NaviException("Cannot force schedule task. Schedule may not active.");
        }
    }


    private long calcScheduleBeforeTime(String scheduleId) throws NaviException {
        ScheduleInfo info = scheduleInfos.get(scheduleId);
        boolean isExecuteBefore = info.isExecuteBeforeNow();
        return calcScheduleBeforeTime(scheduleId, isExecuteBefore);
    }


    private long calcScheduleBeforeTime(String scheduleId, boolean isExecuteBefore) throws NaviException {
        ScheduleInfo info = scheduleInfos.get(scheduleId);
        if (info == null) {
            throw new NaviException("[ERROR] Can not find schedule:[" + scheduleId + "]");
        }
        Date startTime = info.getPeriod().getFirstScheduleTime();
        String delay = info.getPeriod().getDelay();
        long currentScheduleTime = System.currentTimeMillis() - CronUtil.parsePeriodStringToMills(delay);

        if (isExecuteBefore) {
            Long lastScheduleTime = scheduleTime.get(scheduleId);
            if (lastScheduleTime != null) {
                return lastScheduleTime;
            } else if (startTime != null) {
                return startTime.getTime();
            } else {
                return currentScheduleTime;
            }
        } else {
            return currentScheduleTime;
        }
    }

    private void updateScheduleStartTime(String scheduleId, Long startTimeMills) throws NaviException {
        ScheduleInfo info = scheduleInfos.get(scheduleId);
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        jobDao.updateScheduleStartTime(scheduleId, startTimeMills);
        info.getPeriod().setFirstScheduleTime(startTimeMills != null ? new Date(startTimeMills) : null);
        LOGGER.info("update [" + scheduleId + "] start time:" + startTimeMills);
    }

    private Long scheduleTask(ScheduleInfo info, Long time) throws NaviException {
        long now = System.currentTimeMillis();
        LOGGER.debug("----- schedule now, cost time:" + (System.currentTimeMillis() - now));
        TaskEvent event = buildPreparingEvent(info, time);
        LOGGER.info(
                "build preparing event for schedule:[" + info.getScheduleId() + "], event detail:" + event.toJson());
        LOGGER.debug("----- build event, cost time:" + (System.currentTimeMillis() - now));
        TaskStateManager.dispatchEvent(event);
        LOGGER.debug("----- dispatchEvent, cost time:" + (System.currentTimeMillis() - now));
        lastScheduleDataTime.put(info.getScheduleId(), calculateDataTime(info, time));
        Long nextTime = calcNextScheduleTime(info.getScheduleId(), time + 1);
        LOGGER.debug("----- calc time cost, cost time:" + (System.currentTimeMillis() - now));
        if (nextTime != null) {
            LOGGER.info("schedule [" + info.getScheduleId() + "] next start time is " + nextTime + ": "
                    + CronUtil.getPrettyTime(nextTime));
            scheduleTime.put(info.getScheduleId(), nextTime);
            LOGGER.debug("----- put cost, cost time:" + (System.currentTimeMillis() - now));
        } else {
            LOGGER.warn("schedule [" + info.getScheduleId() + "] next start time is null. last start time is " + time);
        }
        return event.getContext().getExecuteInfo().getId();
    }

    private void removeForceScheduleLock(String scheduleId) {
        if (forceScheduleLock.contains(scheduleId)) {
            LOGGER.info("remove force schedule lock [" + scheduleId + "]");
            forceScheduleLock.remove(scheduleId);
        }
    }

    private void addForceScheduleLock(String scheduleId) {
        LOGGER.info("add force schedule lock [" + scheduleId + "]");
        forceScheduleLock.add(scheduleId);
    }


    class ScheduleThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    synchronized (scheduleInfoLock) {
                        long now = System.currentTimeMillis();
                        for (Map.Entry<String, Long> entry : scheduleTime.entrySet()) {
                            String scheduleId = entry.getKey();
                            Long time = entry.getValue();
                            ScheduleInfo info = scheduleInfos.get(scheduleId);
                            String delay = info.getPeriod().getDelay();
                            if (info.isActive() && time != null
                                    && time + CronUtil.parsePeriodStringToMills(delay) <= now) {
                                try {
                                    scheduleTask(info, time);
                                    removeForceScheduleLock(scheduleId);
                                } catch (Throwable e) {
                                    LOGGER.error("failed to schedule task:" + info.toJsonString(), e);
                                }
                            }
                        }
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.info("Schedule task thread is interrupted");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("Schedule task error", e);
                }
            }
        }
    }
}
