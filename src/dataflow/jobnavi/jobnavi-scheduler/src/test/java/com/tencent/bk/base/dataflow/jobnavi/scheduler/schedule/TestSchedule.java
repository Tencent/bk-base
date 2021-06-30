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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.log4j.Logger;

public class TestSchedule implements Schedule {

    private static final Logger LOGGER = Logger.getLogger(TestSchedule.class);

    private final ConcurrentSkipListMap<String, ScheduleInfo> scheduleInfos = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> scheduleTime = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> lastScheduleDataTime = new ConcurrentSkipListMap<>();

    private Configuration conf;

    @Override
    public void init(Configuration conf) throws NaviException {
        this.conf = conf;
    }

    @Override
    public void start() throws NaviException {
        LOGGER.info("Schedule info loading...");
    }

    @Override
    public void stop() throws NaviException {
        scheduleInfos.clear();
        scheduleTime.clear();
    }

    @Override
    public Long addSchedule(ScheduleInfo info) throws NaviException {
        scheduleInfos.put(info.getScheduleId(), info);
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
        return null;
    }

    @Override
    public Long calcNextScheduleTime(String scheduleId, Long before, boolean allowStartTime) throws NaviException {
        return null;
    }

    @Override
    public Long calcNextDataTime(ScheduleInfo scheduleInfo, Long before) throws NaviException {
        return null;
    }

    @Override
    public Long getLastScheduleDataTime(String scheduleId) {
        return lastScheduleDataTime.get(scheduleId);
    }

    @Override
    public long calculateDateTimeOffset(ScheduleInfo scheduleInfo, long baseTimeMills, boolean forward) {
        return 0;
    }

    @Override
    public long calculateDataTime(ScheduleInfo scheduleInfo, long scheduleTime) {
        return 0;
    }

    @Override
    public TaskInfo buildTaskInfo(ScheduleInfo scheduleInfo, long scheduleTime) throws NaviException {
        return null;
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time, double rank) throws NaviException {
        return null;
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time) throws NaviException {
        return null;
    }

    @Override
    public DefaultTaskEvent buildPreparingEvent(TaskInfo taskInfo, double rank) {
        return null;
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
        return null;
    }
}
