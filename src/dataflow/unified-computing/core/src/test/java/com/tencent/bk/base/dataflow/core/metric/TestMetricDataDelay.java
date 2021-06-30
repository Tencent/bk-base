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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.junit.Test;

public class TestMetricDataDelay {

    @Test
    public void testToMap() {
        MetricDataDelay metricDataDelay = new MetricDataDelay();
        metricDataDelay.setWindowTime(60);
        metricDataDelay.setWaitingTime(10);

        metricDataDelay.setMinDelayOutputTime(1);
        metricDataDelay.setMinDelayDataTime(2);
        metricDataDelay.setMinDelayDelayTime(3);

        metricDataDelay.setMaxDelayOutputTime(10);
        metricDataDelay.setMaxDelayDataTime(11);
        metricDataDelay.setMaxDelayDelayTime(13);
        Map<String, Object> result = metricDataDelay.toMap();
        assertEquals(13L, ((Map) result.get("max_delay")).get("delay_time"));
        assertEquals(3L, ((Map) result.get("min_delay")).get("delay_time"));
        assertEquals(60, result.get("window_time"));
    }
}
