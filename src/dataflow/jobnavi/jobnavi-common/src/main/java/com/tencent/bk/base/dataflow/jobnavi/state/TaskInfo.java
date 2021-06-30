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

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.HashMap;
import java.util.Map;

public class TaskInfo implements Cloneable {

    private String scheduleId;
    private long scheduleTime;
    private long dataTime;
    private TaskType type;
    private String extraInfo;
    private TaskRecoveryInfo recoveryInfo;
    private String decommissionTimeout;
    private String nodeLabel;

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

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public TaskRecoveryInfo getRecoveryInfo() {
        return recoveryInfo;
    }

    public void setRecoveryInfo(TaskRecoveryInfo recoveryInfo) {
        this.recoveryInfo = recoveryInfo;
    }

    public TaskType getType() {
        return type;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public String getDecommissionTimeout() {
        return decommissionTimeout;
    }

    public void setDecommissionTimeout(String decommissionTimeout) {
        this.decommissionTimeout = decommissionTimeout;
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    public void setNodeLabel(String nodeLabel) {
        this.nodeLabel = nodeLabel;
    }

    /**
     * parse task info from json string
     *
     * @param maps
     */
    public boolean parseJson(Map<?, ?> maps) {
        boolean parseOK = true;
        if (maps.get("scheduleId") != null) {
            scheduleId = (String) maps.get("scheduleId");
        }
        if (maps.get("scheduleTime") != null) {
            scheduleTime = (Long) maps.get("scheduleTime");
        } else {
            parseOK = false;
        }
        if (maps.get("dataTime") != null) {
            dataTime = Long.parseLong(maps.get("dataTime").toString());
        } else {
            parseOK = false;
        }
        if (maps.get("type") != null && maps.get("type") instanceof Map<?, ?>) {
            type = new TaskType();
            parseOK &= type.parseJson((Map<?, ?>) maps.get("type"));
        }
        if (maps.get("extraInfo") != null) {
            extraInfo = (String) maps.get("extraInfo");
        }
        if (maps.get("recoveryInfo") != null && maps.get("recoveryInfo") instanceof Map<?, ?>) {
            recoveryInfo = new TaskRecoveryInfo();
            recoveryInfo.parseJson((Map<?, ?>) maps.get("recoveryInfo"));
        }
        if (maps.get("decommissionTimeout") != null) {
            decommissionTimeout = (String) maps.get("decommissionTimeout");
        }
        if (maps.get("nodeLabel") != null) {
            nodeLabel = (String) maps.get("nodeLabel");
        }
        return parseOK && scheduleId != null;
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("scheduleId", scheduleId);
        httpJson.put("scheduleTime", scheduleTime);
        httpJson.put("dataTime", dataTime);
        httpJson.put("type", type.toHttpJson());
        httpJson.put("extraInfo", extraInfo);
        httpJson.put("recoveryInfo", recoveryInfo.toHttpJson());
        httpJson.put("decommissionTimeout", decommissionTimeout);
        httpJson.put("nodeLabel", nodeLabel);
        return httpJson;
    }

    @Override
    public TaskInfo clone() {
        try {
            TaskInfo info = (TaskInfo) super.clone();
            info.setRecoveryInfo(this.recoveryInfo != null ? this.recoveryInfo.clone() : null);
            return info;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }
}
