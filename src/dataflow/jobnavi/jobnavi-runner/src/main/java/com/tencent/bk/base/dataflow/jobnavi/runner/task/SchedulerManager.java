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

package com.tencent.bk.base.dataflow.jobnavi.runner.task;

import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class SchedulerManager {

    private static final Logger LOGGER = Logger.getLogger(SchedulerManager.class);
    private static final int RETRY_TIMES = -1; //avoid event lost, -1 means infinite

    public static void sendEvent(TaskEvent event) throws Exception {
        HAProxy.sendPostRequest("/runner_event", event.toJson(), null, RETRY_TIMES);
    }

    public static void sendEventResult(long eventId, TaskEventResult result) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("operate", "process");
        params.put("event_id", eventId);
        params.put("task_event_result", result.toJson());
        HAProxy.sendPostRequest("/event_result", JsonUtils.writeValueAsString(params), null);
    }

    public static boolean recoveryTask(TaskInfo taskInfo) {
        try {
            String rtn = HAProxy.sendPostRequest("/sys/recovery_task", taskInfo.toJson(), null);
            LOGGER.info(String.format("submit task(%s) to schedule, response: %s", taskInfo.getScheduleId(), rtn));
        } catch (Exception e) {
            LOGGER.error(String.format("submit task(%s) to schedule failed", taskInfo.getScheduleId()), e);
            return false;
        }
        return true;
    }

}
