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

public class TaskRecoveryInfo implements Cloneable {

    private boolean recoveryEnable;
    private int recoveryTimes = 0;
    private String intervalTime;
    private int maxRecoveryTimes;
    private double rank;
    private long createdAt;

    public int getMaxRecoveryTimes() {
        return maxRecoveryTimes;
    }

    public void setMaxRecoveryTimes(int maxRecoveryTimes) {
        this.maxRecoveryTimes = maxRecoveryTimes;
    }

    public boolean isRecoveryEnable() {
        return recoveryEnable;
    }

    public void setRecoveryEnable(boolean recoveryEnable) {
        this.recoveryEnable = recoveryEnable;
    }

    public int getRecoveryTimes() {
        return recoveryTimes;
    }

    public void setRecoveryTimes(int recoveryTimes) {
        this.recoveryTimes = recoveryTimes;
    }

    public String getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(String intervalTime) {
        this.intervalTime = intervalTime;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * parse task recovery info from json string
     *
     * @param map
     */
    public void parseJson(Map<?, ?> map) {
        if (map.get("recoveryEnable") != null) {
            recoveryEnable = (Boolean) map.get("recoveryEnable");
        }
        if (map.get("recoveryTimes") != null) {
            recoveryTimes = (Integer) map.get("recoveryTimes");
        }
        if (map.get("intervalTime") != null) {
            intervalTime = (String) map.get("intervalTime");
        }
        if (map.get("maxRecoveryTimes") != null) {
            maxRecoveryTimes = (Integer) map.get("maxRecoveryTimes");
        }
        if (map.get("rank") != null) {
            rank = (Double) map.get("rank");
        }
    }

    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("recoveryEnable", recoveryEnable);
        httpJson.put("recoveryTimes", recoveryTimes);
        httpJson.put("intervalTime", intervalTime);
        httpJson.put("maxRecoveryTimes", maxRecoveryTimes);
        httpJson.put("rank", rank);
        return httpJson;
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    @Override
    public TaskRecoveryInfo clone() {
        try {
            return (TaskRecoveryInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }
}
