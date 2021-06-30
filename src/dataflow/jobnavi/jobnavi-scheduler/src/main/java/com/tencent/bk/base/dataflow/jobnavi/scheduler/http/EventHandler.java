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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.CustomTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class EventHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(EventHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        try {
            //external system event interface
            InputStream is = request.getInputStream();
            Map<String, Object> params = JsonUtils.readMap(is);
            if (params.get("event_name") == null) {
                writeData(false, "Event must contain event_name", HttpReturnCode.ERROR_PARAM_MISSING, response);
                return;
            }
            if (params.get("execute_id") == null) {
                writeData(false, "Event must contain execute_id", HttpReturnCode.ERROR_PARAM_MISSING, response);
                return;
            }
            long eventId = -1;
            String eventName = (String) params.get("event_name");
            TaskEvent event = TaskEventFactory.generateTaskEvent(eventName);
            if (event instanceof DefaultTaskEvent) {
                event.setEventName(eventName);

                Long executeId = Long.parseLong(params.get("execute_id").toString());
                EventContext context = TaskStateManager.generateEventContext(executeId);
                if (params.get("event_info") != null) {
                    context.setEventInfo((String) params.get("event_info"));
                }
                event.setContext(context);

                if (params.get("change_status") != null) {
                    event
                            .setChangeStatus(TaskStatus.valueOf((String) params.get("change_status")));
                }
                eventId = TaskStateManager.dispatchEvent(event);
            } else if (event instanceof CustomTaskEvent) {
                event.setEventName(eventName);
                Long executeId = Long.parseLong(params.get("execute_id").toString());
                ExecuteResult result = MetaDataManager.getJobDao().queryExecuteResult(executeId);
                if (result == null || TaskStatus.running != result.getStatus()) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("execute_id", executeId);
                    json.put("status", result != null ? result.getStatus().name() : null);
                    json.put("info", result != null ? result.getResultInfo() : null);
                    writeData(false, "execute is not running.",
                            HttpReturnCode.ERROR_RUNTIME_EXCEPTION, JsonUtils.writeValueAsString(json), response);
                    return;
                }
                EventContext context = TaskStateManager.generateEventContext(executeId);
                if (params.get("event_info") != null) {
                    context.setEventInfo((String) params.get("event_info"));
                }
                event.setContext(context);
                eventId = TaskStateManager.dispatchEventToRunner(event);
            } else {
                event.parseJson(params);
            }
            LOGGER.info("receive remote event: " + event.toJson());
            writeData(true, "Event received.", HttpReturnCode.DEFAULT, String.valueOf(eventId), response);
        } catch (Throwable e) {
            LOGGER.error("process event error.", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }
}
