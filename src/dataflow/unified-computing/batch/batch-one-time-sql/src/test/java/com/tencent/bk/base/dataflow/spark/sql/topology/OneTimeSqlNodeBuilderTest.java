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

package com.tencent.bk.base.dataflow.spark.sql.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OneTimeSqlNodeBuilderTest {
    @Test
    public void testOneTimeSQLHDFSSourceNodeBuilder() {
        Map<String, Object> jobnaviParameters = new HashMap<>();
        jobnaviParameters.put("job_id", "topo_test");
        jobnaviParameters.put("job_name", "topo_test");
        jobnaviParameters.put("run_mode", "test");
        jobnaviParameters.put("job_type", "one_time_sql");
        jobnaviParameters.put("execute_id", "11111");

        String sourceNode1Id = "source_node_1";
        String transformNode1Id = "transform_node_1";
        String sinkNode1Id = "sink_node_1";
        String sql = String.format("SELECT key, value FROM %s", sourceNode1Id);
        Map<String, Object> testApiOneTimeSqlParam = createTopoInfo(
            sourceNode1Id,
            transformNode1Id,
            sinkNode1Id,
            sql
        );

        TestOneTimeSQLTopoBuilder builder = new TestOneTimeSQLTopoBuilder(jobnaviParameters);
        builder.setTestOneTimeSqlParam(testApiOneTimeSqlParam);
        BatchOneTimeSQLTopology topology = builder.build();
        assertTrue(topology.getSourceNodes().get(sourceNode1Id) instanceof BatchHDFSSourceNode);
        BatchHDFSSourceNode sourceNode1 = (BatchHDFSSourceNode)topology.getSourceNodes().get(sourceNode1Id);
        List<String> pathList = sourceNode1.getHdfsInput().getInputInfo();
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/03/31/00"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/03/31/10"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/01"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/02"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/03"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/04"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/05"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/06"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/07"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/08"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/09"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/10"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/11"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/12"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/13"));
        assertTrue(pathList.contains("hdfs://hdfs-test/data/source_node_1/2021/01/04/14"));

        assertTrue(topology.getTransformNodes().get(transformNode1Id) instanceof BatchSQLTransformNode);
        BatchSQLTransformNode transformNode1 =
            (BatchSQLTransformNode)topology.getTransformNodes().get(transformNode1Id);
        assertEquals(sql, transformNode1.getSql());

        assertTrue(topology.getSinkNodes().get(sinkNode1Id) instanceof BatchHDFSSinkNode);
        BatchHDFSSinkNode sinkNode1 = (BatchHDFSSinkNode)topology.getSinkNodes().get(sinkNode1Id);
        Assert.assertEquals(String.format("hdfs://hdfs-test/data/%s", sinkNode1Id),
                sinkNode1.getHdfsOutput().getOutputInfo());
    }

    private Map<String, Object> createTopoInfo(
            String sourceNodeId,
            String transformNodeId,
            String sinkNodeId,
            String sql) {
        Map<String, Object> sourceNode1Info = new HashMap<>();
        sourceNode1Info.put("id", sourceNodeId);
        sourceNode1Info.put("name", sourceNodeId);
        sourceNode1Info.put("description", sourceNodeId);
        sourceNode1Info.put("role", "clean");
        sourceNode1Info.put("input", this.createParquetHdfsConf(sourceNodeId));
        sourceNode1Info.put("partition", this.createTestPartition());
        sourceNode1Info.put("fields", this.createFieldsList(sourceNodeId));
        Map<String, Object> sourceNodesInfo = new HashMap<>();
        sourceNodesInfo.put(sourceNodeId, sourceNode1Info);
        Map<String, Object> nodes = new HashMap<>();
        nodes.put("source", sourceNodesInfo);

        Map<String, Object> transformNode1Info = new HashMap<>();
        transformNode1Info.put("id", transformNodeId);
        transformNode1Info.put("name", transformNodeId);
        transformNode1Info.put("description", transformNodeId);
        Map<String, Object> processorInfo = new HashMap<>();
        processorInfo.put("processor_args", sql);
        transformNode1Info.put("processor", processorInfo);
        Map<String, Object> transformNodesInfo = new HashMap<>();
        transformNodesInfo.put(transformNodeId, transformNode1Info);
        nodes.put("transform", transformNodesInfo);

        Map<String, Object> sinkNode1Info = new HashMap<>();
        sinkNode1Info.put("id", sinkNodeId);
        sinkNode1Info.put("name", sinkNodeId);
        sinkNode1Info.put("description", sinkNodeId);
        sinkNode1Info.put("role", "queryset");
        Map<String, Object> outputMap = this.createParquetHdfsConf(sinkNodeId);
        outputMap.put("mode", "overwrite");
        sinkNode1Info.put("output", outputMap);
        Map<String, Object> sinkNodesInfo = new HashMap<>();
        sinkNodesInfo.put(sinkNodeId, sinkNode1Info);
        nodes.put("sink", sinkNodesInfo);

        Map<String, Object> testApiOneTimeSqlParam = new HashMap<>();
        testApiOneTimeSqlParam.put("nodes", nodes);
        testApiOneTimeSqlParam.put("job_type", "one_time_sql");
        testApiOneTimeSqlParam.put("job_name", "one_time_sql_test");
        testApiOneTimeSqlParam.put("run_mode", "test");
        return testApiOneTimeSqlParam;
    }

    private Map<String, Object> createParquetHdfsConf(String resultTableId) {
        Map<String, Object> hdfsInfo = new HashMap<>();
        hdfsInfo.put("type", "hdfs");
        Map<String, Object> confInfo = new HashMap<>();
        confInfo.put("cluster_name", "hdfs-test");
        confInfo.put("name_service", "hdfs://hdfs-test");
        confInfo.put("cluster_group", "hdfs-test");
        confInfo.put("data_type", "parquet");
        confInfo.put("physical_table_name", String.format("/data/%s", resultTableId));
        hdfsInfo.put("conf", confInfo);
        return hdfsInfo;
    }

    private Map<String, Object> createTestPartition() {
        List<Map<String, Object>> range = new ArrayList<>();
        Map<String, Object> rangeItem1 = new HashMap<>();
        rangeItem1.put("start", "2021010401");
        rangeItem1.put("end", "2021010414");
        range.add(rangeItem1);
        List<String> list = new ArrayList<>();
        list.add("2021033100");
        list.add("2021033110");
        Map<String, Object> partitions = new HashMap<>();
        partitions.put("range", range);
        partitions.put("list", list);
        return partitions;
    }

    private List<Map<String, Object>> createFieldsList(String resultTableId) {
        Map<String, Object> field1 = new HashMap<>();
        field1.put("origin", resultTableId);
        field1.put("field", "key");
        field1.put("type", "int");
        field1.put("description", "key");
        List<Map<String, Object>> fieldsList = new ArrayList<>();
        fieldsList.add(field1);
        Map<String, Object> field2 = new HashMap<>();
        field2.put("origin", resultTableId);
        field2.put("field", "value");
        field2.put("type", "string");
        field2.put("description", "value");
        fieldsList.add(field2);
        return fieldsList;
    }
}
