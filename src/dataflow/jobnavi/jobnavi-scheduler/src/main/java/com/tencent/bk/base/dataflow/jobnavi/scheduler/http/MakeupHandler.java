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
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import java.util.Collection;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class MakeupHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(MakeupHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        super.doGet(request, response);
        String scheduleId = request.getParameter("schedule_id");
        String startTimeStr = request.getParameter("start_time");
        String endTimeStr = request.getParameter("end_time");

        if (StringUtils.isEmpty(startTimeStr)) {
            writeData(false, "param need start_time.", HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }

        if (StringUtils.isEmpty(endTimeStr)) {
            writeData(false, "param need end_time.", HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }

        long startTime = Long.parseLong(startTimeStr);
        long endTime = Long.parseLong(endTimeStr);

        try {
            if (StringUtils.isEmpty(scheduleId)) {
                LOGGER.info("Makeup all schedule.");
                Collection<ScheduleInfo> scheduleInfos = ScheduleManager.getSchedule().getAllScheduleInfo();
                for (ScheduleInfo scheduleInfo : scheduleInfos) {
                    makeupExecute(scheduleInfo.getScheduleId(), startTime, endTime);
                }
            } else {
                makeupExecute(scheduleId, startTime, endTime);
            }

            writeData(true, "makeup success.", response);
        } catch (Throwable e) {
            LOGGER.error("make up error. ", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }

    private void makeupExecute(String scheduleId, long startTime, long endTime) throws NaviException {
        Schedule schedule = ScheduleManager.getSchedule();
        if (schedule.getScheduleInfo(scheduleId) == null) {
            LOGGER.error("makeup error. Can not find schedule [" + scheduleId + "]");
            return;
        }

        if (schedule.getScheduleInfo(scheduleId).getPeriod() == null) {
            LOGGER.error("schedule [" + scheduleId + "] is not period schedule. skip makeup.");
            return;
        }

        if (!schedule.getScheduleInfo(scheduleId).isActive()) {
            LOGGER.error("schedule [" + scheduleId + "] is disabled. skip makeup.");
            return;
        }

        Long scheduleTime = schedule.calcNextScheduleTime(scheduleId, startTime, false);
        LOGGER.info("makeup schedule time: " + scheduleTime);
        if (scheduleTime != null && scheduleTime < endTime
                && MetaDataManager.getJobDao().getExecuteStatus(scheduleId, scheduleTime) == TaskStatus.none) {
            ScheduleInfo info = schedule.getScheduleInfo(scheduleId);
            LOGGER.info("schedule [" + scheduleId + "] makeup at time : " + scheduleTime);
            TaskEvent preparingEvent = schedule.buildPreparingEvent(info, scheduleTime);
            TaskStateManager.dispatchEvent(preparingEvent);
        }

        if (scheduleTime != null && scheduleTime < endTime) {
            makeupExecute(scheduleId, scheduleTime + 1, endTime);
        }

    }
}
