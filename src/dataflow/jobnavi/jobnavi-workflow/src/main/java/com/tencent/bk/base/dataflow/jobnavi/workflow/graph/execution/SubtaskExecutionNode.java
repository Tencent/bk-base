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

package com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution;

import java.util.Map;


public class SubtaskExecutionNode extends AbstractExecutionNode {

    private String scheduleId;
    private String typeId;
    private String subtaskInfo;
    private Integer retryQuota;
    private String retryInterval;
    private String nodeLabel;

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public String getSubtaskInfo() {
        return subtaskInfo;
    }

    public void setSubtaskInfo(String subtaskInfo) {
        this.subtaskInfo = subtaskInfo;
    }

    public Integer getRetryQuota() {
        return retryQuota;
    }

    public void setRetryQuota(Integer retryQuota) {
        this.retryQuota = retryQuota;
    }

    public String getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(String retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    public void setNodeLabel(String nodeLabel) {
        this.nodeLabel = nodeLabel;
    }

    @Override
    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = super.toHttpJson();
        if (!scheduleId.isEmpty()) {
            httpJson.put("scheduleId", scheduleId);
        }
        httpJson.put("typeId", typeId);
        if (!subtaskInfo.isEmpty()) {
            httpJson.put("subtaskInfo", subtaskInfo);
        }
        httpJson.put("retryQuota", retryQuota);
        if (!retryInterval.isEmpty()) {
            httpJson.put("retryInterval", retryInterval);
        }
        if (nodeLabel != null) {
            httpJson.put("nodeLabel", nodeLabel);
        }
        return httpJson;
    }
}
