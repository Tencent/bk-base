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
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.joda.time.DateTime;

public class DebugHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(SchedulerHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        if ("fetch_today_job_status".equals(operate)) {
            DateTime now = new DateTime();
            long startTime = now.minusDays(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate()
                    .getTime();
            long endTime = now.minusHours(1).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999)
                    .toDate().getTime();
            Map<String, Map<String, Integer>> amount = MetaDataManager.getJobDao()
                    .queryExecuteStatusAmount(startTime, endTime);
            String todayJobsStr = JsonUtils.writeValueAsString(amount);
            writeData(true, "", HttpReturnCode.DEFAULT, todayJobsStr, response);
        } else if ("query_processing_event_amount".equals(operate)) {
            int amount = TaskStateManager.queryNotProcessEventAmount();
            writeData(true, "", HttpReturnCode.DEFAULT, String.valueOf(amount), response);
        } else if ("query_next_schedule_time".equals(operate)) {
            Map<String, Long> scheduleTimes = ScheduleManager.getSchedule().getScheduleTimes();
            Map<String, String> scheduleTimePretty = new HashMap<>();
            for (Map.Entry<String, Long> scheduleTimeMap : scheduleTimes.entrySet()) {
                String scheduleId = scheduleTimeMap.getKey();
                Long scheduleTime = scheduleTimeMap.getValue();
                scheduleTimePretty.put(scheduleId, CronUtil.getPrettyTime(scheduleTime));
            }
            String times = JsonUtils.writeValueAsString(scheduleTimePretty);
            writeData(true, "", HttpReturnCode.DEFAULT, times, response);
        } else if ("fetch_last_hour_job_status".equals(operate)) {
            DateTime lastHour = new DateTime().minusHours(1);
            long startTime = lastHour.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate()
                    .getTime();
            long endTime = lastHour.withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999).toDate()
                    .getTime();
            Map<String, Map<String, Integer>> amount = MetaDataManager.getJobDao()
                    .queryExecuteStatusAmount(startTime, endTime);
            for (Map.Entry<String, Map<String, Integer>> entry : amount.entrySet()) {
                String todayJobsStr = JsonUtils.writeValueAsString(entry.getValue());
                writeData(true, "", HttpReturnCode.DEFAULT, todayJobsStr, response);
                return;
            }
        } else if ("fetch_job_status_by_create_time".equals(operate)) {
            DateTime now = new DateTime();
            long startTime = now.minusDays(1).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate()
                    .getTime();
            long endTime = now.minusHours(1).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999)
                    .toDate().getTime();
            Map<String, Map<String, Integer>> amount = MetaDataManager.getJobDao()
                    .queryExecuteStatusAmountByCreateAt(startTime, endTime);
            String todayJobsStr = JsonUtils.writeValueAsString(amount);
            writeData(true, "", HttpReturnCode.DEFAULT, todayJobsStr, response);
        } else {
            logger.error("debug operate " + operate + " not support.");
        }
    }
}
