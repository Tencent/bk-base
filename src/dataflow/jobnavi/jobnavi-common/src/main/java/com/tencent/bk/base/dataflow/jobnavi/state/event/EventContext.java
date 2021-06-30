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

import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.log4j.Logger;

public class EventContext implements Cloneable {

    private static final Logger LOGGER = Logger.getLogger(EventContext.class);

    private ExecuteInfo executeInfo;
    private TaskInfo taskInfo;
    private String eventInfo;

    public ExecuteInfo getExecuteInfo() {
        return executeInfo;
    }

    public void setExecuteInfo(ExecuteInfo executeInfo) {
        this.executeInfo = executeInfo;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    public String getEventInfo() {
        return eventInfo;
    }

    public void setEventInfo(String eventInfo) {
        this.eventInfo = eventInfo;
    }

    /**
     * parse event context json string
     *
     * @param params
     */
    public boolean parseJson(Map<?, ?> params) {
        boolean parseOK = true;
        if (params.get("executeInfo") != null && params.get("executeInfo") instanceof Map<?, ?>) {
            executeInfo = new ExecuteInfo();
            parseOK = executeInfo.parseJson((Map<?, ?>) params.get("executeInfo"));
        }
        if (params.get("taskInfo") != null && params.get("taskInfo") instanceof Map<?, ?>) {
            taskInfo = new TaskInfo();
            parseOK &= taskInfo.parseJson((Map<?, ?>) params.get("taskInfo"));
        }
        if (params.get("eventInfo") != null) {
            eventInfo = (String) params.get("eventInfo");
        }
        return parseOK;
    }

    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("executeInfo", executeInfo.toHttpJson());
        httpJson.put("taskInfo", taskInfo.toHttpJson());
        httpJson.put("eventInfo", eventInfo);
        return httpJson;
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventContext that = (EventContext) o;
        return executeInfo.equals(that.executeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executeInfo);
    }

    @Override
    public EventContext clone() {
        try {
            EventContext context = (EventContext) super.clone();
            context.setExecuteInfo(this.executeInfo != null ? this.executeInfo.clone() : null);
            context.setTaskInfo(this.taskInfo != null ? this.taskInfo.clone() : null);
            return context;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }
}
