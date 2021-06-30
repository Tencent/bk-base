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

package com.tencent.bk.base.dataflow.metric;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DelayMetricSample {

    private Long minDelayOutputTime = null;
    private Long minDelayDataTime = null;
    private Long minDelayDelayTime = null;

    private Long maxDelayOutputTime = null;
    private Long maxDelayDataTime = null;
    private Long maxDelayDelayTime = null;

    /**
     * 重置延迟指标
     */
    public void reset() {
        // 重置延迟指标
        minDelayOutputTime = null;
        minDelayDataTime = null;
        minDelayDelayTime = null;

        maxDelayOutputTime = null;
        maxDelayDataTime = null;
        maxDelayDelayTime = null;
    }

    /**
     * 设置延迟指标
     *
     * @param rowTime 数据时间
     * @throws ParseException 异常
     */
    public void setDelayTimeMetric(String rowTime) throws ParseException {
        SimpleDateFormat utfFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            {
                setTimeZone(TimeZone.getTimeZone("UTC"));
            }
        };
        long outputTime = System.currentTimeMillis() / 1000;
        Date date = utfFormat.parse(rowTime);
        long dataTime = date.getTime() / 1000;
        long delayTime = outputTime - dataTime;
        if (null == minDelayDelayTime || delayTime < minDelayDelayTime) {
            minDelayDelayTime = delayTime;
            minDelayDataTime = dataTime;
            minDelayOutputTime = outputTime;
        }

        if (null == maxDelayDelayTime || delayTime > maxDelayDelayTime) {
            maxDelayDelayTime = delayTime;
            maxDelayDataTime = dataTime;
            maxDelayOutputTime = outputTime;
        }
    }

    /**
     * 根据不同的task汇总延迟指标
     *
     * @param other 另外的采样信息
     */
    public void merge(DelayMetricSample other) {
        if (null == other || null == other.getMaxDelayDelayTime() || null == other.getMinDelayDelayTime()) {
            return;
        }
        if (null == minDelayDelayTime || other.getMinDelayDelayTime() < minDelayDelayTime) {
            minDelayDelayTime = other.getMinDelayDelayTime();
            minDelayDataTime = other.getMinDelayDataTime();
            minDelayOutputTime = other.getMinDelayOutputTime();
        }
        if (null == maxDelayDelayTime || other.getMaxDelayDelayTime() > maxDelayDelayTime) {
            maxDelayDelayTime = other.getMaxDelayDelayTime();
            maxDelayDataTime = other.getMaxDelayDataTime();
            maxDelayOutputTime = other.getMaxDelayOutputTime();
        }
    }

    public Long getMinDelayOutputTime() {
        return minDelayOutputTime;
    }

    public Long getMinDelayDataTime() {
        return this.minDelayDataTime;
    }

    public Long getMinDelayDelayTime() {
        return minDelayDelayTime;
    }

    public Long getMaxDelayOutputTime() {
        return maxDelayOutputTime;
    }

    public Long getMaxDelayDataTime() {
        return maxDelayDataTime;
    }

    public Long getMaxDelayDelayTime() {
        return maxDelayDelayTime;
    }
}
