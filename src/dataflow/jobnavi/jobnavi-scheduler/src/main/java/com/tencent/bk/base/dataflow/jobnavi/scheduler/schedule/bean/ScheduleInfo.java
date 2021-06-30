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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean;

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScheduleInfo implements Cloneable {

    private String scheduleId;
    private String description;
    private Period period;
    private String dataTimeOffset;
    private boolean execOnCreate;
    private String extraInfo;
    private List<DependencyInfo> parents;
    private String typeId;
    private String typeTag;
    private RecoveryInfo recoveryInfo;
    private boolean active = true;
    private boolean executeBeforeNow;
    private String createdBy;
    private String decommissionTimeout;
    private String nodeLabel;
    private int maxRunningTask = -1;

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public String getDataTimeOffset() {
        return dataTimeOffset;
    }

    public void setDataTimeOffset(String dataTimeOffset) {
        this.dataTimeOffset = dataTimeOffset;
    }

    public boolean isExecOnCreate() {
        return execOnCreate;
    }

    public void setExecOnCreate(boolean execOnCreate) {
        this.execOnCreate = execOnCreate;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public List<DependencyInfo> getParents() {
        return parents;
    }

    public void setParents(List<DependencyInfo> parents) {
        this.parents = parents;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public String getTypeTag() {
        return typeTag;
    }

    public void setTypeTag(String typeTag) {
        this.typeTag = typeTag;
    }

    public RecoveryInfo getRecoveryInfo() {
        return recoveryInfo;
    }

    public void setRecoveryInfo(RecoveryInfo recoveryInfo) {
        this.recoveryInfo = recoveryInfo;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isExecuteBeforeNow() {
        return executeBeforeNow;
    }

    public void setExecuteBeforeNow(boolean executeBeforeNow) {
        this.executeBeforeNow = executeBeforeNow;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
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

    public int getMaxRunningTask() {
        return maxRunningTask;
    }

    public void setMaxRunningTask(int maxRunningTask) {
        this.maxRunningTask = maxRunningTask;
    }

    /**
     * parse schedule info from json string
     *
     * @param jsonMap ScheduleInfo Json
     */
    public void parseJson(Map<String, Object> jsonMap) {
        scheduleId = (String) jsonMap.get("schedule_id");
        if (jsonMap.get("description") != null) {
            description = (String) jsonMap.get("description");
        }

        if (jsonMap.get("period") != null && jsonMap.get("period") instanceof Map<?, ?>) {
            if (period == null) {
                period = new Period();
            }
            period.parseJson((Map<?, ?>) jsonMap.get("period"));
        }

        if (jsonMap.get("data_time_offset") != null) {
            dataTimeOffset = (String) jsonMap.get("data_time_offset");
        }

        execOnCreate = jsonMap.get("exec_oncreate") != null && (boolean) jsonMap.get("exec_oncreate");
        if (jsonMap.get("extra_info") != null) {
            extraInfo = (String) jsonMap.get("extra_info");
        }

        if (parents == null) {
            parents = new ArrayList<>();
        }
        if (jsonMap.get("parents") != null && jsonMap.get("parents") instanceof List<?>) {
            parents = new ArrayList<>();
            List<?> parentList = (List<?>) jsonMap.get("parents");
            for (Object parent : parentList) {
                if (parent instanceof Map<?, ?>) {
                    DependencyInfo info = new DependencyInfo();
                    info.parseJson((Map<?, ?>) parent);
                    parents.add(info);
                }
            }
        }

        if (jsonMap.get("type_id") != null) {
            typeId = (String) jsonMap.get("type_id");
        }

        if (recoveryInfo == null) {
            recoveryInfo = new RecoveryInfo();
        }
        if (jsonMap.get("recovery") != null && jsonMap.get("recovery") instanceof Map<?, ?>) {
            recoveryInfo.parseJson((Map<?, ?>) jsonMap.get("recovery"));
        }
        if (jsonMap.get("active") != null) {
            active = (boolean) jsonMap.get("active");
        }
        if (jsonMap.get("execute_before_now") != null) {
            executeBeforeNow = (boolean) jsonMap.get("execute_before_now");
        }
        if (jsonMap.get("created_by") != null) {
            createdBy = (String) jsonMap.get("created_by");
        }
        if (jsonMap.get("decommission_timeout") != null) {
            decommissionTimeout = (String) jsonMap.get("decommission_timeout");
        }
        if (jsonMap.containsKey("node_label")) {
            nodeLabel = (String) jsonMap.get("node_label");
        }
        if (jsonMap.get("max_running_task") != null) {
            maxRunningTask = (Integer) jsonMap.get("max_running_task");
        }
    }

    public String toJsonString() {
        return JsonUtils.writeValueAsString(this);
    }

    /**
     * dump schedule info to HTTP json
     * @return HTTP json map
     */
    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJsonMap = new HashMap<>();
        httpJsonMap.put("schedule_id", scheduleId);
        httpJsonMap.put("description", description);
        if (period != null) {
            httpJsonMap.put("period", period.toHTTPJson());
        }
        if (dataTimeOffset != null) {
            httpJsonMap.put("data_time_offset", dataTimeOffset);
        }
        httpJsonMap.put("exec_oncreate", execOnCreate);
        httpJsonMap.put("extra_info", extraInfo);
        List<Map<String, Object>> parentList = new ArrayList<>();
        for (DependencyInfo parent : parents) {
            parentList.add(parent.toHTTPJson());
        }
        httpJsonMap.put("parents", parentList);
        httpJsonMap.put("type_id", typeId);
        if (recoveryInfo != null) {
            httpJsonMap.put("recovery_info", recoveryInfo.toHTTPJson());
        }
        httpJsonMap.put("active", active);
        httpJsonMap.put("execute_before_now", executeBeforeNow);
        httpJsonMap.put("created_by", createdBy);
        httpJsonMap.put("decommission_timeout", decommissionTimeout);
        httpJsonMap.put("node_label", nodeLabel);
        httpJsonMap.put("max_running_task", maxRunningTask);
        return httpJsonMap;
    }

    @Override
    public ScheduleInfo clone() {
        try {
            ScheduleInfo info = (ScheduleInfo) super.clone();
            info.recoveryInfo = recoveryInfo != null ? recoveryInfo.clone() : null;
            info.period = period != null ? period.clone() : null;
            if (parents != null) {
                info.parents = new ArrayList<>();
                for (DependencyInfo dependencyInfo : this.parents) {
                    info.parents.add(dependencyInfo.clone());
                }
            }
            return info;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }

}
