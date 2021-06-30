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

package com.tencent.bk.base.dataflow.flink.topology;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology.FlinkSDKBuilder;
import java.util.Map;
import org.junit.Test;

public class TestFlinkCodeTopology {

    /**
     * 生成测试需要使用的 code topology
     *
     * @return code topology
     */
    public static FlinkCodeTopology getTestFlinkCodeTopology() {
        Map<String, Object> parameters = ImmutableMap.<String, Object>builder()
                .put("job_id", "test_job_id")
                .put("job_name", "test_job_name")
                .put("job_type", "flink")
                .put("run_mode", "debug")
                .put("time_zone", "Asia/Shanghai")
                .put("chain", "enable")
                .put("metric", ImmutableMap.<String, String>builder()
                        .put("metric_kafka_server", "metric kafka server")
                        .put("metric_kafka_topic", "metric kafka topic")
                        .build())
                .put("debug", ImmutableMap.<String, String>builder()
                        .put("debug_id", "test_debug_id")
                        .put("debug_error_data_rest_api_url", "debug_error_data_rest_api_url")
                        .put("debug_node_metric_rest_api_url", "debug_node_metric_rest_api_url")
                        .put("debug_result_data_rest_api_url", "debug_result_data_rest_api_url")
                        .build())
                .put("user_args", ImmutableList.<String>builder()
                        .add("args0")
                        .add("args1")
                        .build())
                .put("user_main_class", "com.tencent.bk.base.dataflow.flink.pipeline.CodeTransformForTest")
                .put("savepoint", ImmutableMap.<String, Object>builder()
                        .put("start_position", "continue")
                        .put("hdfs_path", "hdfs path")
                        .build())
                .put("nodes", ImmutableMap.<String, Object>builder()
                        .put("sink", ImmutableMap.<String, Object>builder()
                                .put("1_test_code_transform", ImmutableMap.<String, Object>builder()
                                        .put("id", "1_test_code_transform")
                                        .put("name", "test_code_transform")
                                        .put("output", ImmutableMap.<String, String>builder()
                                                .put("cluster_port", "1")
                                                .put("type", "kafka")
                                                .put("cluster_domain", "cluster domain")
                                                .build())
                                        .put("fields", ImmutableList.<Map<String, Object>>builder()
                                                .add(ImmutableMap.of("field", "double_field",
                                                        "type", "double",
                                                        "origin", "",
                                                        "event_time", false))
                                                .add(ImmutableMap.of("field", "int_field",
                                                        "type", "int", "origin", "",
                                                        "event_time", false))
                                                .add(ImmutableMap.of("field", "string_field",
                                                        "type", "string",
                                                        "origin", "",
                                                        "event_time", false))
                                                .add(ImmutableMap.of("field", "long_field",
                                                        "type", "long",
                                                        "origin", "",
                                                        "event_time", true))
                                                .add(ImmutableMap.of("field", "float_field",
                                                        "type", "float",
                                                        "origin", "",
                                                        "event_time", false))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        return new FlinkSDKBuilder(parameters).build();
    }

    @Test
    public void testFlinkCodeTopology() {
        FlinkCodeTopology flinkCodeTopology = getTestFlinkCodeTopology();
        assertEquals("Asia/Shanghai", flinkCodeTopology.getTimeZone());
        assertEquals("test_job_id", flinkCodeTopology.getJobId());
        assertEquals("test_job_name", flinkCodeTopology.getJobName());
        assertEquals("flink", flinkCodeTopology.getJobType());
        assertEquals("debug", flinkCodeTopology.getRunMode());
    }
}
