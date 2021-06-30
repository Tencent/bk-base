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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Collection;
import java.util.Map;

public interface Schedule {

    void init(Configuration conf) throws NaviException;

    void start() throws NaviException;

    void stop() throws NaviException;

    Long addSchedule(ScheduleInfo info) throws NaviException;

    void stopScheduleJob(String scheduleId) throws NaviException;

    void startScheduleJob(String scheduleId) throws NaviException;

    void deleteSchedule(String scheduleId) throws NaviException;

    void updateSchedule(ScheduleInfo info) throws NaviException;

    ScheduleInfo getScheduleInfo(String scheduleId);

    /**
     * calculate next schedule time
     * if before == scheduleTime,return before
     *
     * @param scheduleId
     * @param before
     * @return next schedule time
     * @throws NaviException
     */
    Long calcNextScheduleTime(String scheduleId, Long before) throws NaviException;

    Long calcNextScheduleTime(String scheduleId, Long before, boolean allowStartTime) throws NaviException;

    Long calcNextDataTime(ScheduleInfo scheduleInfo, Long before) throws NaviException;

    Long getLastScheduleDataTime(String scheduleId);

    long calculateDateTimeOffset(ScheduleInfo scheduleInfo, long baseTimeMills, boolean forward);

    long calculateDataTime(ScheduleInfo scheduleInfo, long scheduleTime);

    TaskInfo buildTaskInfo(ScheduleInfo scheduleInfo, long scheduleTime) throws NaviException;

    DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time, double rank) throws NaviException;

    DefaultTaskEvent buildPreparingEvent(ScheduleInfo info, long time) throws NaviException;

    DefaultTaskEvent buildPreparingEvent(TaskInfo taskInfo, double rank) throws NaviException;

    Collection<ScheduleInfo> getAllScheduleInfo() throws NaviException;

    Map<String, Long> getScheduleTimes() throws NaviException;

    Long forceScheduleTask(String scheduleId) throws NaviException;

}
