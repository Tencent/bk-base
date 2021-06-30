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

package com.tencent.bk.base.dataflow.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.topology.StreamTopology.AbstractStreamBuilder;
import java.util.Map;
import org.junit.Test;

public class TestStreamTopology {

    @Test
    public void testStreamTopology() {
        StreamTopology topology = getTestStreamTopology();
        assertEquals("Asia/Shanghai", topology.getTimeZone());
        assertEquals("stream", topology.getModule());
        assertEquals("flink", topology.getComponent());

        assertEquals("common metric kafka server", topology.getMetricConfig().getMetricKafkaServer());
        assertEquals("common metric kafka topic", topology.getMetricConfig().getMetricKafkaTopic());

        assertFalse(topology.isDisableChain());
        assertTrue(topology.isDebug());

        assertEquals("common debug_error_data_rest_api_url", topology.getDebugConfig().getDebugErrorDataApi());
        assertEquals("test_debug_id", topology.getDebugConfig().getDebugId());
        assertEquals("common debug_node_metric_rest_api_url", topology.getDebugConfig().getDebugNodeMetricApi());
        assertEquals("common debug_result_data_rest_api_url", topology.getDebugConfig().getDebugResultDataApi());
    }

    /**
     * 获取测试需要的 {@link StreamTopology} 实例
     *
     * @return {@link StreamTopology} 测试实例
     */
    public static StreamTopology getTestStreamTopology() {
        Map<String, Object> parameters = ImmutableMap.<String, Object>builder()
                .put("job_id", "test_common_job_id")
                .put("job_name", "test_common_job_name")
                .put("job_type", "flink")
                .put("run_mode", "debug")
                .put("time_zone", "Asia/Shanghai")
                .put("chain", "enable")
                .put("metric", ImmutableMap.<String, String>builder()
                        .put("metric_kafka_server", "common metric kafka server")
                        .put("metric_kafka_topic", "common metric kafka topic")
                        .build())
                .put("debug", ImmutableMap.<String, String>builder()
                        .put("debug_id", "test_debug_id")
                        .put("debug_error_data_rest_api_url", "common debug_error_data_rest_api_url")
                        .put("debug_node_metric_rest_api_url", "common debug_node_metric_rest_api_url")
                        .put("debug_result_data_rest_api_url", "common debug_result_data_rest_api_url")
                        .build())
                .build();

        return  (StreamTopology) new AbstractStreamBuilder(parameters) {
            @Override
            public Topology build() {
                return new StreamTopology<>(this);
            }
        }.build();
    }
}
