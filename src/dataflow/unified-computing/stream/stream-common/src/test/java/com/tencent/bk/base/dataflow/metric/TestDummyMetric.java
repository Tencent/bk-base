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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.kafka.producer.util.TestAbstractAvroKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.topology.TestStreamTopology;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class TestDummyMetric {

    private DummyMetric dummyMetric;

    @Before
    public void setUp() {
        dummyMetric = new DummyMetric(0, TestAbstractAvroKeyedSerializationSchema.getTestSinkNode(),
                TestStreamTopology.getTestStreamTopology(), "");
    }

    @Test
    public void testInit() {
        dummyMetric.init();
        assertEquals(0L, dummyMetric.getInputTotalCount().get());
    }

    @Test
    public void testGetMetricObjectInfo() {
        dummyMetric.recordCounter(Row.of(1622171682000L, "a"), "output tag");
        dummyMetric.recordDrop("checkpoint filter", null);
        dummyMetric.recordDataMalformed("ip format error", null);
        dummyMetric.updateMetricDataDelay();
        dummyMetric.createMetricObject();
        String metricObjectInfo = dummyMetric.getMetricObjectInfo();
        assertTrue(metricObjectInfo.contains("\"result_table_id\":\"1_test_node_id\""));
        assertTrue(metricObjectInfo.contains("\"module\":\"stream\""));
        assertTrue(metricObjectInfo.contains("\"component\":\"flink\""));
        assertTrue(metricObjectInfo.contains("\"total_cnt\":1"));
    }
}
