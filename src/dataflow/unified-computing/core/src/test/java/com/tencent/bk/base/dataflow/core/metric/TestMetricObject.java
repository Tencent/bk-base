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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Test;

public class TestMetricObject {

    @Test
    public void testCollectMetricMessage() {
        MetricObject metricObject = new MetricObject();
        metricObject.setModule("batch");
        metricObject.setComponent("spark");
        metricObject.setCluster("default");

        MetricTag metricTag = new MetricTag();
        metricTag.setDesc(ImmutableMap.of("test", "test"));
        metricTag.setTag("test tag");
        metricObject.setPhysicalTag(metricTag);
        metricObject.setLogicalTag(metricTag);

        metricObject.setCustomTags(ImmutableMap.of("custom_tag", "nothing"));

        metricObject.setStorage(ImmutableMap.of("kafka", "kafka info"));

        MetricStream metricStream = new MetricStream();
        metricStream.setModule("batch");
        metricStream.setComponent("spark");
        metricStream.setLogicalTag(Collections.singletonList(metricTag));
        metricObject.setDownStream(Collections.singletonList(metricStream));

        // data loss
        MetricDataStatics inputDataStatics = new MetricDataStatics();
        inputDataStatics.setTags(ImmutableMap.of("tag", 100L));
        inputDataStatics.setTotalCnt(200L);

        MetricDataStatics outputDataStatics = new MetricDataStatics();
        outputDataStatics.setTags(ImmutableMap.of("tag", 90L));
        outputDataStatics.setTotalCnt(190L);
        outputDataStatics.setCheckpointDroptags(ImmutableMap.of("checkpoint_drop", 10L));

        MetricDataLoss metricDataLoss = new MetricDataLoss();
        metricDataLoss.setInput(inputDataStatics);
        metricDataLoss.setOutput(outputDataStatics);
        metricDataLoss.setDataDrop(ImmutableMap.of("checkpoint_drop", 10L));

        metricObject.setDataLoss(metricDataLoss);

        // data delay
        MetricDataDelay metricDataDelay = new MetricDataDelay();
        metricDataDelay.setWindowTime(60);
        metricDataDelay.setWaitingTime(10);

        metricDataDelay.setMinDelayOutputTime(1);
        metricDataDelay.setMinDelayDataTime(2);
        metricDataDelay.setMinDelayDelayTime(3);

        metricDataDelay.setMaxDelayOutputTime(10);
        metricDataDelay.setMaxDelayDataTime(11);
        metricDataDelay.setMaxDelayDelayTime(13);
        metricObject.setDataDelay(metricDataDelay);

        metricObject.setResourceMonitor(ImmutableMap.of("resource_monitor", "nothing"));
        metricObject.setCustomMetrics(ImmutableMap.of("custom_metrics", "nothing"));
        String metric = metricObject.collectMetricMessage();

        assertTrue(metric.contains("\"total_cnt\":190"));
        assertTrue(metric.contains("\"tag\":90"));
        assertTrue(metric.contains("\"storage\":{\"kafka\":\"kafka info\"}"));
        assertTrue(metric.contains("\"window_time\":60"));
    }

}
