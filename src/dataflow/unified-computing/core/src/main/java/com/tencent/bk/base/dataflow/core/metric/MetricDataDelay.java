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

package com.tencent.bk.base.dataflow.core.metric;

import java.util.HashMap;
import java.util.Map;

public class MetricDataDelay {

    private int windowTime;
    private int waitingTime;

    private long minDelayOutputTime;
    private long minDelayDataTime;
    private long minDelayDelayTime;

    private long maxDelayOutputTime;
    private long maxDelayDataTime;
    private long maxDelayDelayTime;

    /**
     * 构造时间类型打点
     *
     * @return 时间类型打点的map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("window_time", windowTime);
        map.put("waiting_time", waitingTime);

        Map<String, Object> minDelay = new HashMap<>();
        minDelay.put("output_time", minDelayOutputTime);
        minDelay.put("data_time", minDelayDataTime);
        minDelay.put("delay_time", minDelayDelayTime);
        map.put("min_delay", minDelay);

        Map<String, Object> maxDelay = new HashMap<>();
        maxDelay.put("output_time", maxDelayOutputTime);
        maxDelay.put("data_time", maxDelayDataTime);
        maxDelay.put("delay_time", maxDelayDelayTime);
        map.put("max_delay", maxDelay);
        return map;
    }

    public int getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(int windowTime) {
        this.windowTime = windowTime;
    }

    public int getWaitingTime() {
        return waitingTime;
    }

    public void setWaitingTime(int waitingTime) {
        this.waitingTime = waitingTime;
    }

    public long getMinDelayOutputTime() {
        return minDelayOutputTime;
    }

    public void setMinDelayOutputTime(long minDelayOutputTime) {
        this.minDelayOutputTime = minDelayOutputTime;
    }

    public long getMinDelayDataTime() {
        return minDelayDataTime;
    }

    public void setMinDelayDataTime(long minDelayDataTime) {
        this.minDelayDataTime = minDelayDataTime;
    }

    public void setMinDelayDelayTime(long minDelayDelayTime) {
        this.minDelayDelayTime = minDelayDelayTime;
    }

    public long getMaxDelayOutputTime() {
        return maxDelayOutputTime;
    }

    public void setMaxDelayOutputTime(long maxDelayOutputTime) {
        this.maxDelayOutputTime = maxDelayOutputTime;
    }

    public long getMaxDelayDataTime() {
        return maxDelayDataTime;
    }

    public void setMaxDelayDataTime(long maxDelayDataTime) {
        this.maxDelayDataTime = maxDelayDataTime;
    }

    public void setMaxDelayDelayTime(long maxDelayDelayTime) {
        this.maxDelayDelayTime = maxDelayDelayTime;
    }
}
