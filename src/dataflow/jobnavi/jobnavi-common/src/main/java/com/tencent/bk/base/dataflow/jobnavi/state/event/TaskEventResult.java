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

package com.tencent.bk.base.dataflow.jobnavi.state.event;

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TaskEventResult {

    private boolean success = true; // task process result
    private boolean processed = true;
    private String processInfo;
    private List<TaskEvent> nextEvents = new ArrayList<>(); // do next event when current next has processed.

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public String getProcessInfo() {
        return processInfo;
    }

    public void setProcessInfo(String processInfo) {
        this.processInfo = processInfo;
    }

    public List<TaskEvent> getNextEvents() {
        return nextEvents;
    }

    public void addNextEvent(TaskEvent event) {
        nextEvents.add(event);
    }

    public void addNextEvents(List<TaskEvent> events) {
        nextEvents.addAll(events);
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    /**
     * parse task event result from json string
     *
     * @param params
     * @throws NaviException
     */
    public void parseJson(Map<String, Object> params) throws NaviException {
        success = (Boolean) params.get("success");
        processInfo = (String) params.get("processInfo");
        if (params.get("nextEvents") != null && params.get("nextEvents") instanceof List<?>) {
            List<?> nextEventList = (List<?>) params.get("nextEvents");
            nextEvents = new ArrayList<>();
            for (Object nextEventParam : nextEventList) {
                if (nextEventParam instanceof Map<?, ?>) {
                    TaskEvent taskEvent = TaskEventFactory.generateTaskEvent(
                            (String) ((Map<?, ?>) nextEventParam).get("eventName"));
                    taskEvent.parseJson((Map<?, ?>) nextEventParam);
                    nextEvents.add(taskEvent);
                }
            }
        }
    }
}
