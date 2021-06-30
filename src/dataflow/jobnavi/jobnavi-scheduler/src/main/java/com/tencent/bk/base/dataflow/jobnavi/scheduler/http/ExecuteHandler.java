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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;


public class ExecuteHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(ExecuteHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String scheduleId = request.getParameter("schedule_id");
        String scheduleTimeStr = request.getParameter("schedule_time");
        Boolean isForce = request.getParameter("force") != null && Boolean.parseBoolean(request.getParameter("force"));
        execute(scheduleId, scheduleTimeStr, isForce, null, response);
    }


    private void execute(String scheduleId, String scheduleTimeStr, boolean isForce, String extraInfo,
            Response response) throws Exception {
        Long scheduleTime;
        if (StringUtils.isEmpty(scheduleId)) {
            writeData(false, "Param must contain [schedule_id].", HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }

        if (scheduleTimeStr != null && scheduleTimeStr.length() > 0) {
            scheduleTime = Long.parseLong(scheduleTimeStr);
            if (scheduleTime > System.currentTimeMillis()) {
                writeData(false, "scheduleTime must before now.", HttpReturnCode.ERROR_PARAM_INVALID, response);
                return;
            }
        } else {
            scheduleTime = System.currentTimeMillis();
        }

        Schedule schedule = ScheduleManager.getSchedule();
        ScheduleInfo info = schedule.getScheduleInfo(scheduleId);
        if (info == null) {
            writeData(false, "Cannot find schedule [" + scheduleId + "]. ", HttpReturnCode.ERROR_PARAM_INVALID,
                    response);
            return;
        }

        if (!isForce) {
            AbstractJobDao dao = MetaDataManager.getJobDao();
            TaskStatus status = dao.getExecuteStatus(scheduleId, scheduleTime);
            if (status == TaskStatus.preparing || status == TaskStatus.running || status == TaskStatus.recovering) {
                writeData(false,
                        "[" + scheduleId + "] is under scheduling, Please try it later or set param force=true",
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                return;
            }
        }

        DefaultTaskEvent preparingEvent = schedule.buildPreparingEvent(info, scheduleTime);
        if (StringUtils.isNotEmpty(extraInfo)) {
            preparingEvent.getContext().getTaskInfo().setExtraInfo(extraInfo);
        }
        TaskStateManager.dispatchEvent(preparingEvent);
        long executeId = preparingEvent.getContext().getExecuteInfo().getId();
        LOGGER.info("receive execute, id is " + executeId);
        writeData(true, "Execute is preparing, id is " + executeId, HttpReturnCode.DEFAULT, String.valueOf(executeId),
                response);
    }
}

