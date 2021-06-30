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

package com.tencent.bk.base.dataflow.jobnavi.state;

import java.util.HashMap;
import java.util.Map;

public class ExecuteResult {

    private ExecuteInfo executeInfo;
    private String scheduleId;
    private long scheduleTime;
    private long dataTime;
    private TaskStatus status;
    private String resultInfo;
    private long createdAt;
    private long startedAt;
    private long updatedAt;

    public ExecuteInfo getExecuteInfo() {
        return executeInfo;
    }

    public void setExecuteInfo(ExecuteInfo executeInfo) {
        this.executeInfo = executeInfo;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public long getDataTime() {
        return dataTime;
    }

    public void setDataTime(long dataTime) {
        this.dataTime = dataTime;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public String getResultInfo() {
        return resultInfo;
    }

    public void setResultInfo(String resultInfo) {
        this.resultInfo = resultInfo;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJsonMap = new HashMap<>();
        httpJsonMap.put("execute_info", executeInfo);
        httpJsonMap.put("schedule_id", scheduleId);
        httpJsonMap.put("schedule_time", scheduleTime);
        httpJsonMap.put("data_time", dataTime);
        httpJsonMap.put("status", status.name());
        httpJsonMap.put("info", resultInfo);
        httpJsonMap.put("created_at", createdAt);
        httpJsonMap.put("started_at", startedAt);
        httpJsonMap.put("updated_at", updatedAt);
        return httpJsonMap;
    }

    /**
     * parse execute result json string
     *
     * @param maps
     */
    public void parseJson(Map<String, Object> maps) {
        if (maps.get("execute_info") != null) {
            if (executeInfo == null) {
                executeInfo = new ExecuteInfo();
            }
            executeInfo.parseJson((Map<String, Object>) maps.get("execute_info"));
        }

        if (maps.get("schedule_id") != null) {
            scheduleId = maps.get("schedule_id").toString();
        }

        if (maps.get("scheduleTime") != null) {
            scheduleTime = Long.parseLong(maps.get("scheduleTime").toString());
        }

        if (maps.get("dataTime") != null) {
            dataTime = Long.parseLong(maps.get("dataTime").toString());
        }

        if (maps.get("status") != null) {
            status = TaskStatus.values(maps.get("status").toString());
        }

        if (maps.get("info") != null) {
            resultInfo = maps.get("info").toString();
        }

        if (maps.get("created_at") != null) {
            createdAt = Long.parseLong(maps.get("created_at").toString());
        }

        if (maps.get("started_at") != null) {
            startedAt = Long.parseLong(maps.get("started_at").toString());
        }

        if (maps.get("updated_at") != null) {
            updatedAt = Long.parseLong(maps.get("updated_at").toString());
        }
    }
}
