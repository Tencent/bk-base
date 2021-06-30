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
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class EventResultHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(EventResultHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        if ("query".equals(operate)) {
            String eventIdStr = request.getParameter("event_id");
            if (StringUtils.isEmpty(eventIdStr)) {
                writeData(false, "[event_id] required. ", response);
                return;
            }
            Long eventId = Long.parseLong(eventIdStr);

            Map<String, Object> result = new HashMap<>();
            try {
                TaskEventResult eventResult = MetaDataManager.getJobDao().queryEventResult(eventId);
                if (eventResult == null) {
                    result.put("is_processed", false);
                    result.put("process_success", "");
                    result.put("process_info", "");
                } else {
                    result.put("is_processed", true);
                    result.put("process_success", eventResult.isSuccess());
                    result.put("process_info", eventResult.getProcessInfo());
                }
                writeData(true, "query event_id " + eventId + " event result success.",
                        HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(result), response);
            } catch (Exception e) {
                logger.error("process event result error.", e);
                writeData(false, "query event_id " + eventId + " event result error. " + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else {
            writeData(false, "operate " + operate + " not support.", HttpReturnCode.ERROR_PARAM_INVALID, response);
        }
    }

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        if ("process".equals(operate)) {
            Object eventIdObj = params.get("event_id");
            String resultJson = (String) params.get("task_event_result");
            if (eventIdObj == null) {
                writeData(false, "[event_id] required. ", response);
                return;
            }

            if (StringUtils.isEmpty(resultJson)) {
                writeData(false, "[task_event_result] required. ", response);
                return;
            }

            logger.info("process event result: " + resultJson);

            Long eventId = Long.parseLong(eventIdObj.toString());
            TaskEventResult taskEventResult = new TaskEventResult();
            taskEventResult.parseJson(JsonUtils.readMap(resultJson));
            try {
                TaskStateManager.processEventResult(eventId, taskEventResult);
                writeData(true, "process event_id " + eventId + " event result success.", response);
            } catch (Exception e) {
                logger.error("process event result error.", e);
                writeData(false, "process event_id " + eventId + " event result error. " + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else {
            writeData(false, "operate " + operate + " not support.", HttpReturnCode.ERROR_PARAM_INVALID, response);
        }
    }
}
