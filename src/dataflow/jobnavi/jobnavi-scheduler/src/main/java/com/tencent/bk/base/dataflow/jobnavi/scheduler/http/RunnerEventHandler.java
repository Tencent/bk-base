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
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.InputStream;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class RunnerEventHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(RunnerEventHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        try {
            //internal event interface
            InputStream is = request.getInputStream();
            Map<String, Object> params = JsonUtils.readMap(is);
            if (params.get("eventName") == null) {
                writeData(false, "Event must contain eventName", HttpReturnCode.ERROR_PARAM_MISSING, response);
                return;
            }
            TaskEvent event;
            String eventName = (String) params.get("eventName");
            event = TaskEventFactory.generateTaskEvent(eventName);
            if (!event.parseJson(params) && event.getContext() != null && event.getContext().getExecuteInfo() != null) {
                long executeId = event.getContext().getExecuteInfo().getId();
                LOGGER.info("reload context for event: " + executeId + "~" + eventName);
                EventContext context = TaskStateManager.generateEventContext(executeId);
                event.setContext(context);
            }
            long eventId = TaskStateManager.dispatchEvent(event);
            LOGGER.info("receive remote event: " + event.toJson());
            writeData(true, "Event received.", HttpReturnCode.DEFAULT, String.valueOf(eventId), response);
        } catch (Throwable e) {
            LOGGER.error("process event error.", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }
}
