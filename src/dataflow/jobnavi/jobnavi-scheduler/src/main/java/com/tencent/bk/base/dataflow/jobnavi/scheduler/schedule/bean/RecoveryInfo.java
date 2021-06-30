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

import java.util.HashMap;
import java.util.Map;

public class RecoveryInfo implements Cloneable {

    private boolean enable;
    private String intervalTime = "1h";
    private int retryTimes;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public String getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(String intervalTime) {
        this.intervalTime = intervalTime;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    /**
     * parse recovery info from json
     *
     * @param jsonMap
     */
    public void parseJson(Map<?, ?> jsonMap) {
        if (jsonMap != null) {
            enable = jsonMap.get("enable") != null && (Boolean) jsonMap.get("enable");
            if (jsonMap.get("interval_time") != null) {
                intervalTime = (String) jsonMap.get("interval_time");
            }
            if (jsonMap.get("retry_times") != null) {
                retryTimes = (Integer) jsonMap.get("retry_times");
            }
        } else {
            enable = false;
        }
    }

    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("enable", enable);
        httpJson.put("interval_time", intervalTime);
        httpJson.put("retry_times", retryTimes);
        return httpJson;
    }

    @Override
    public RecoveryInfo clone() {
        try {
            return (RecoveryInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }
}
