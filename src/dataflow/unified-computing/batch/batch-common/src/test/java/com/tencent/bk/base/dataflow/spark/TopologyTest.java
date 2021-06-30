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

package com.tencent.bk.base.dataflow.spark;

import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.TopoSinkNodeTest.HdfsSinkOption;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopologyTest {
    @Test
    public void testHdfsSinkNode() {
        Map<String, Object> info = new HashMap<>();
        info.put("job_id", "topo_test");
        info.put("job_name", "topo_test");
        info.put("job_type", "batch_sql");
        info.put("run_mode", "test");
        info.put("execute_id", "11111");
        String fakeHdfsRoot = "hdfs://fakeHdfs/api/flow";
        Map<String, Object> sourceNodeConf = new HashMap<>();
        sourceNodeConf.put("result_table_id", "8_hdfs_source_node");
        sourceNodeConf.put("root_path", fakeHdfsRoot);
        sourceNodeConf.put("start_time", "2021052000");
        sourceNodeConf.put("end_time", "2021052005");
        Map<String, Object> sourceNodeMap = new HashMap<>();
        sourceNodeMap.put("8_hdfs_source_node", sourceNodeConf);
        info.put("source_node_info", sourceNodeMap);

        Map<String, Object> transformNodeMap = new HashMap<>();
        Map<String, Object> transformNodeConf = new HashMap<>();
        transformNodeConf.put("result_table_id", "8_hdfs_sink_node");
        transformNodeConf.put("sql", "select id, test_time_str from hdfs_source_node_8");
        transformNodeMap.put("8_hdfs_sink_node", transformNodeConf);
        info.put("transform_node_info", transformNodeMap);

        Map<String, Object> sinkNodeConf = new HashMap<>();
        sinkNodeConf.put("result_table_id", "8_hdfs_sink_node");
        sinkNodeConf.put("root_path", fakeHdfsRoot);
        sinkNodeConf.put("dt_event_timestamp", 1621440000000L);
        sinkNodeConf.put("schedule_timestamp", 1621526400000L);
        Map<String, Object> sinkNodeMap = new HashMap<>();
        sinkNodeMap.put("8_hdfs_sink_node", sinkNodeConf);
        info.put("sink_node_info", sinkNodeMap);

        TestBatchTopologyBuilder builder = new TestBatchTopologyBuilder(info);
        BatchTopology batchTopology = builder.build();
        assertTrue(batchTopology.getSourceNodes().get("8_hdfs_source_node") instanceof BatchHDFSSourceNode);
        assertTrue(batchTopology.getTransformNodes().get("8_hdfs_sink_node") instanceof BatchSQLTransformNode);
        assertTrue(batchTopology.getSinkNodes().get("8_hdfs_sink_node") instanceof BatchHDFSSinkNode);
    }


    public static class TestBatchTopologyBuilder extends AbstractBatchTopologyBuilder {

        private Map<String, Object> sourceNodeInfo;
        private Map<String, Object> transformNodeInfo;
        private Map<String, Object> sinkNodeInfo;

        /**
         * 初始化BatchTopologyBuilder
         *
         * @param parameters batch拓扑参数
         */
        public TestBatchTopologyBuilder(Map<String, Object> parameters) {
            super(parameters);
            this.sourceNodeInfo = (Map<String, Object>)parameters.get("source_node_info");
            this.transformNodeInfo = (Map<String, Object>)parameters.get("transform_node_info");
            this.sinkNodeInfo = (Map<String, Object>)parameters.get("sink_node_info");
        }

        private void buildNodes() throws ParseException {
            List<Node> sourceNodesList = new LinkedList<>();
            List<Node> transformNodesList = new LinkedList<>();
            for (Entry<String, Object> entry : this.sourceNodeInfo.entrySet()) {
                Object nodeConf = entry.getValue();
                SourceNode sourceNode = this.createSourceNode((Map<String, Object>) nodeConf);
                sourceNodesList.add(sourceNode);
            }

            for (Entry<String, Object> entry : this.transformNodeInfo.entrySet()) {
                Object nodeConf = entry.getValue();
                TransformNode transformNode = this.createTransformNode((Map<String, Object>) nodeConf);
                transformNodesList.add(transformNode);
                transformNode.setParents(sourceNodesList);
            }

            for (Entry<String, Object> entry : this.sinkNodeInfo.entrySet()) {
                Object nodeConf = entry.getValue();
                SinkNode sinkNode = this.createSinkNode((Map<String, Object>) nodeConf);
                sinkNode.setParents(transformNodesList);
                this.sinkNodes.put(sinkNode.getNodeId(), sinkNode);
            }
        }

        private SourceNode createSourceNode(Map<String, Object> nodeInfo) throws ParseException {
            BatchHDFSSourceNode hdfsSourceNode = TopoSourceNodeTest.createSimpleHdfsSourceNode(
                nodeInfo.get("result_table_id").toString(),
                nodeInfo.get("root_path").toString(),
                nodeInfo.get("start_time").toString(),
                nodeInfo.get("end_time").toString()
            );
            return hdfsSourceNode;
        }

        private TransformNode createTransformNode(Map<String, Object> nodeInfo) {
            BatchSQLTransformNode sqlTransformNode = TopoTransformNodeTest.createSimpleSqlTransformNode(
                nodeInfo.get("result_table_id").toString(),
                nodeInfo.get("sql").toString()
            );
            return sqlTransformNode;
        }

        private SinkNode createSinkNode(Map<String, Object> nodeInfo) {
            String expectedFormat = "parquet";
            String expectedOutputMode = "overwrite";
            long expectedDtEventTime = (long)nodeInfo.get("dt_event_timestamp");
            long expectedScheduleTime = (long)nodeInfo.get("schedule_timestamp");
            HdfsSinkOption options = new HdfsSinkOption();
            options.setEnableCallMaintainInterface(false);
            options.setEnableCallShipperInterface(false);
            options.setForceUpdateReservedSchema(false);
            options.setReservedFieldEnabled(true);
            options.setDtEventTimeStamp(expectedDtEventTime);
            options.setScheduleTime(expectedScheduleTime);
            BatchHDFSSinkNode hdfsSinkNode = TopoSinkNodeTest.createSimpleHdfsSinkNode(
                nodeInfo.get("result_table_id").toString(),
                nodeInfo.get("root_path").toString(),
                expectedFormat,
                expectedOutputMode,
                options
            );
            return hdfsSinkNode;
        }

        @Override
        public BatchTopology build() {
            try {
                this.buildNodes();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            this.buildUDF(null);
            this.fillNodesFromSinkParents();
            return new BatchTopology(this);
        }
    }
}
