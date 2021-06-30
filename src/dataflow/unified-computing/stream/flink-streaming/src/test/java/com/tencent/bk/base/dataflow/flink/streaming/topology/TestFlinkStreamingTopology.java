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

package com.tencent.bk.base.dataflow.flink.streaming.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology.FlinkStreamingBuilder;
import java.util.Map;
import org.junit.Test;

public class TestFlinkStreamingTopology {

    @Test
    public void testIsFileSystemState() {
        assertFalse(getFlinkStreamingTopology().isFileSystemState());
        assertEquals("Asia/Shanghai", getFlinkStreamingTopology().getTimeZone());
        assertNull(getFlinkStreamingTopology().getGeogAreaCode());
    }

    /**
     * 获取测试需要使用的 {@link FlinkStreamingTopology}
     *
     * @return {@link FlinkStreamingTopology}的测试实例
     */
    public static FlinkStreamingTopology getFlinkStreamingTopology() {
        Map<String, Object> parameters = ImmutableMap.<String, Object>builder()
                .put("job_id", "test_job_id")
                .put("job_name", "test_job_name")
                .put("job_type", "flink")
                .put("run_mode", "product")
                .put("time_zone", "Asia/Shanghai")
                .put("chain", "enable")
                .put("metric", ImmutableMap.<String, String>builder()
                        .put("metric_kafka_server", "metric kafka server")
                        .put("metric_kafka_topic", "metric kafka topic")
                        .build())
                .put("checkpoint", ImmutableMap.<String, Object>builder()
                        .put("checkpoint_redis_sentinel_port", 0)
                        .put("checkpoint_redis_port", 50000)
                        .put("state_backend", "rocksdb")
                        .put("checkpoint_redis_host", "redis host")
                        .put("manager", "redis")
                        .put("start_position", "continue")
                        .put("state_checkpoints_dir", "checkpoint dir")
                        .put("checkpoint_redis_password", "redis password")
                        .put("checkpoint_interval", 600000)
                        .build())
                .put("nodes", ImmutableMap.<String, Object>builder()
                        .put("source", generateSourceNodeInfo())
                        .put("transform", generateTransformNodeInfo())
                        .build())
                .build();
        return new FlinkStreamingBuilder(parameters).build();
    }

    private static Map<String, Object> generateTransformNodeInfo() {
        return ImmutableMap.<String, Object>builder()
                .put("1_test_join_transform", ImmutableMap.<String, Object>builder()
                        .put("id", "1_test_join_transform")
                        .put("name", "test_join_transform")
                        .put("processor", ImmutableMap.of("processor_type", "join_transform",
                                "processor_args", "{\"first\":\"1_first\",\"second\":\"1_second\","
                                        + "\"type\":\"right\",\"join_keys\":"
                                        + "[{\"first\":\"first_key\",\"second\":\"second_key\"}]}"))
                        .put("parents", ImmutableList.of("1_first", "1_second"))
                        .put("window", ImmutableMap.of("count_freq", 60,
                                "allowed_lateness", false, "type", "tumbling"))
                        .put("fields", ImmutableList.<Map<String, Object>>builder()
                                .add(ImmutableMap.of("field", "dtEventTime", "type", "string",
                                        "origin", "", "description", "event time"))
                                .add(ImmutableMap.of("field", "_startTime_", "type", "string",
                                        "origin", "", "description", "event time"))
                                .add(ImmutableMap.of("field", "_endTime_", "type", "string",
                                        "origin", "", "description", "event time"))
                                .add(ImmutableMap.of("field", "a", "type", "string",
                                        "origin", "1_first:a", "description", "a"))
                                .add(ImmutableMap.of("field", "b", "type", "string",
                                        "origin", "1_second:b", "description", "b"))
                                .build())
                        .build())
                .build();
    }

    private static Map<String, Object> generateSourceNodeInfo() {
        return ImmutableMap.<String, Object>builder()
                .put("1_first", ImmutableMap.<String, Object>builder()
                        .put("id", "1_first")
                        .put("name", "first")
                        .put("input", ImmutableMap.of("type", "kafka", "cluster_port", 9092,
                                "cluster_domain", "domain"))
                        .put("fields", ImmutableList.<Map<String, Object>>builder()
                                .add(ImmutableMap.of("field", "dtEventTime", "type", "string",
                                        "origin", "", "description", "event time"))
                                .add(ImmutableMap.of("field", "bkdata_par_offset", "type", "string",
                                        "origin", "", "description", "bkdata_par_offset"))
                                .add(ImmutableMap.of("field", "first_key", "type", "string",
                                        "origin", "", "description", "first key"))
                                .add(ImmutableMap.of("field", "a", "type", "string",
                                        "origin", "", "description", "a"))
                                .build())
                        .build())
                .put("1_second", ImmutableMap.<String, Object>builder()
                        .put("id", "1_second")
                        .put("name", "second")
                        .put("input", ImmutableMap.of("type", "kafka", "cluster_port", 9092,
                                "cluster_domain", "domain"))
                        .put("fields", ImmutableList.<Map<String, Object>>builder()
                                .add(ImmutableMap.of("field", "dtEventTime", "type", "string",
                                        "origin", "", "description", "event time"))
                                .add(ImmutableMap.of("field", "bkdata_par_offset", "type", "string",
                                        "origin", "", "description", "bkdata_par_offset"))
                                .add(ImmutableMap.of("field", "second_key", "type", "string",
                                        "origin", "", "description", "second key"))
                                .add(ImmutableMap.of("field", "b", "type", "string",
                                        "origin", "", "description", "b"))
                                .build())
                        .build())
                .build();
    }
}
