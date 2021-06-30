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

package com.tencent.bk.base.dataflow.core.topo;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.topo.Topology.AbstractBuilder;
import java.util.ArrayList;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class TestTopology {

    private Topology topology;

    @Before
    public void setUp() {
        AbstractBuilder builder = new AbstractBuilder(ImmutableMap.of("job_id", "test_job_id",
                "job_name", "test_job_name", "job_type", "flink", "run_mode", "product")) {
            @Override
            public Topology build() {
                return null;
            }
        };
        this.topology = new Topology(builder);
    }

    @Test
    public void testMapUdfs() {

        Map<String, Object> udfInfo = ImmutableMap.<String, Object>builder()
                .put("source_data", new ArrayList<>())
                .put("config", ImmutableList.<Map<String, String>>builder()
                        .add(ImmutableMap.<String, String>builder()
                                .put("hdfs_path", "hdfs path")
                                .put("version", "v1")
                                .put("language", "python")
                                .put("local_path", "local path")
                                .put("type", "udtf")
                                .put("name", "udf name")
                                .build())
                        .build())
                .build();
        topology.mapUdfs(udfInfo);
        UserDefinedFunctionConfig udfConfig = topology.getUserDefinedFunctions().getFunctions().get(0);
        assertEquals("python", udfConfig.getLanguage());
        assertEquals("hdfs path", udfConfig.getHdfsPath());
        assertEquals("udtf", udfConfig.getType());
    }

    @Test
    public void testMapNodes() {
        Map<String, Object> sourceInfo = ImmutableMap.<String, Object>builder()
                .put("1_test_source", ImmutableMap.<String, Object>builder()
                        .put("id", "1_test_source")
                        .put("name", "test_source")
                        .put("input", ImmutableMap.<String, String>builder()
                                .put("cluster_port", "1")
                                .put("type", "kafka")
                                .put("cluster_domain", "cluster domain")
                                .build())
                        .build())
                .build();

        Map<String, Object> transformInfo = ImmutableMap.<String, Object>builder()
                .put("1_test_transform", ImmutableMap.<String, Object>builder()
                        .put("id", "1_test_transform")
                        .put("name", "test_transform")
                        .put("parents", ImmutableList.of("1_test_source"))
                        .build())
                .build();

        Map<String, Object> sinkInfo = ImmutableMap.<String, Object>builder()
                .put("1_test_transform", ImmutableMap.<String, Object>builder()
                        .put("id", "1_test_transform")
                        .put("name", "test_transform")
                        .put("output", ImmutableMap.<String, String>builder()
                                .put("cluster_port", "1")
                                .put("type", "kafka")
                                .put("cluster_domain", "cluster domain")
                                .build())
                        .build())
                .build();


        Map<String, Map<String, Object>> typeTables = ImmutableMap.<String, Map<String, Object>>builder()
                .put("source", sourceInfo)
                .put("transform", transformInfo)
                .put("sink", sinkInfo)
                .build();
        this.topology.mapNodes(typeTables);
        assertEquals("1_test_source", this.topology.getSourceNodes().get("1_test_source").nodeId);
        assertEquals("[1_test_source]",
                this.topology.getTransformNodes().get("1_test_transform").parents.toString());
        assertEquals(this.topology.getSourceNodes().get("1_test_source"),
                this.topology.queryNode("1_test_source"));
        assertEquals(this.topology.getTransformNodes().get("1_test_transform"),
                this.topology.queryNode("1_test_transform"));
    }
}
