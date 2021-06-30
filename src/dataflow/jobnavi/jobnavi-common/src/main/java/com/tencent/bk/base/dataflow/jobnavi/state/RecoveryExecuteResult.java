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

public class RecoveryExecuteResult {

    private long executeId;
    private String scheduleId;
    private long scheduleTime;
    private long retryTimes;
    private boolean recoveryStatus;
    private long createdAt;

    public long getExecuteId() {
        return executeId;
    }

    public void setExecuteId(long executeId) {
        this.executeId = executeId;
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

    public long getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(long retryTimes) {
        this.retryTimes = retryTimes;
    }

    public boolean isRecoveryStatus() {
        return recoveryStatus;
    }

    public void setRecoveryStatus(boolean recoveryStatus) {
        this.recoveryStatus = recoveryStatus;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJsonMap = new HashMap<>();
        httpJsonMap.put("execute_id", executeId);
        httpJsonMap.put("schedule_id", scheduleId);
        httpJsonMap.put("schedule_time", scheduleTime);
        httpJsonMap.put("retry_times", retryTimes);
        httpJsonMap.put("recovery_status", recoveryStatus);
        httpJsonMap.put("created_at", createdAt);
        return httpJsonMap;
    }

    /**
     * parse recovery execute result from json
     *
     * @param maps
     */
    public void parseJson(Map<String, Object> maps) {
        if (maps.get("execute_id") != null) {
            executeId = Long.parseLong(maps.get("execute_id").toString());
        }

        if (maps.get("schedule_id") != null) {
            scheduleId = maps.get("schedule_id").toString();
        }

        if (maps.get("schedule_time") != null) {
            scheduleTime = Long.parseLong(maps.get("schedule_time").toString());
        }

        if (maps.get("retryTimes") != null) {
            retryTimes = Long.parseLong(maps.get("retry_times").toString());
        }

        if (maps.get("recovery_status") != null) {
            recoveryStatus = Boolean.parseBoolean(maps.get("recovery_status").toString());
        }

        if (maps.get("created_at") != null) {
            createdAt = Long.parseLong(maps.get("created_at").toString());
        }
    }
}
