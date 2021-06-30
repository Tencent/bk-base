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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.topology.HDFSStorageConstructor;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.LinkedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SQLDebugTopoBuilder extends AbstractBatchTopologyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLDebugTopoBuilder.class);
    private List<String> heads;
    private List<String> tails;

    private LinkedList<String> staticTable;
    private Map<String, TransformNode> tmpTransformNodesMap = new LinkedMap();

    private List<Map<String, Object>> udfList;

    public SQLDebugTopoBuilder(Map<String, Object> parameters) {
        super(parameters);
        this.staticTable = new LinkedList<>();
        this.tmpTransformNodesMap = new LinkedMap();
        this.udfList = new LinkedList<>();

        this.enableMetrics = false;
        this.heads = Arrays.asList(parameters.get("heads").toString().split(","));
        this.tails = Arrays.asList(parameters.get("tails").toString().split(","));
        buildNodes(parameters);
        Map<String, Object> udfMap = new HashMap<>();
        udfMap.put("config", this.udfList);
        this.buildUDF(udfMap);
    }

    private void buildNodes(Map<String, Object> parameters) {
        for (String tail : tails) {
            traverseBuildNodes(tail);
        }
    }

    private TransformNode traverseBuildNodes(String resultTableId) {
        Map<String, Object> batchProcessing = APIUtil$.MODULE$.getBatchProcessingAsJava(resultTableId);
        Map<String, Object> tranformNodeConfMap = new HashMap<>();

        String processorType = batchProcessing.get("processor_type").toString();

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> submitArgsMap;
        try {
            submitArgsMap = objectMapper.readValue(batchProcessing.get("submit_args").toString(), HashMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, Object> parentsIdMap;
        List<String> staticDataList;
        String sql;
        if (processorType.toLowerCase().equals("batch_sql_v2")) {
            Map<String, Object> processorLogic;
            try {
                processorLogic =
                        objectMapper.readValue(batchProcessing.get("processor_logic").toString(), HashMap.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            sql = processorLogic.get("sql").toString();
            parentsIdMap = new HashMap<>();
            staticDataList = new LinkedList<>();
            List<Map<String, Object>> inputTableList =
                    (List<Map<String, Object>>)submitArgsMap.get("input_result_tables");
            for (Map<String, Object> item : inputTableList) {
                if ((boolean)item.get("is_static")) {
                    staticDataList.add(item.get("result_table_id").toString());
                } else {
                    parentsIdMap.put(item.get("result_table_id").toString(), item);
                }
            }


        } else {
            sql = batchProcessing.get("processor_logic").toString();
            parentsIdMap = (Map<String, Object>)submitArgsMap.get("result_tables");
            staticDataList = (List<String>)submitArgsMap.get("static_data");
        }

        tranformNodeConfMap.put("id", resultTableId);
        tranformNodeConfMap.put("name", resultTableId);
        tranformNodeConfMap.put("sql", sql);
        BatchSQLTransformNode transformNode =
                (new BatchSQLTransformNode.BatchSQLTransformNodeBuilder(tranformNodeConfMap)).build();
        transformNode.setParents(new LinkedList<>());
        this.tmpTransformNodesMap.put(resultTableId, transformNode);

        this.handleStaticData(staticDataList, transformNode);
        this.handleSelfDependency(transformNode, submitArgsMap);
        this.buildDebugUdf(resultTableId);

        this.buildParentTransformAndSourceNode(transformNode, parentsIdMap);
        this.buildSinkNode(resultTableId, transformNode);

        return transformNode;
    }

    private void buildSinkNode(String resultTableId, TransformNode transformNode) {
        Map<String, Object> debugHttpSinkConf = new HashMap<>();
        debugHttpSinkConf.put("id", resultTableId);
        debugHttpSinkConf.put("name", resultTableId);
        debugHttpSinkConf.put("debug_id", this.getJobId());
        debugHttpSinkConf.put("execute_id", this.executeId);
        debugHttpSinkConf.put("enable_throw_exception", false);
        BatchDebugHTTPSinkNode debugHTTPSinkNode =
                new BatchDebugHTTPSinkNode.BatchDebugHTTPSinkNodeBuilder(debugHttpSinkConf).build();
        List<Node> sinkNodeParentsList = new LinkedList<>();
        sinkNodeParentsList.add(transformNode);
        debugHTTPSinkNode.setParents(sinkNodeParentsList);
        this.sinkNodes.put(resultTableId, debugHTTPSinkNode);
    }

    private void buildParentTransformAndSourceNode(TransformNode transformNode, Map<String, Object> parentsIdMap) {
        //Here decide how to set parent and child connection
        for (Map.Entry<String, Object> entry : parentsIdMap.entrySet()) {
            String tmpNodeId = entry.getKey();
            //heads shouldn't have self dependency id in it
            if (heads.contains(tmpNodeId)
                    && this.sourceNodes.containsKey(tmpNodeId)) {
                transformNode.getParents().add(this.sourceNodes.get(tmpNodeId));
            } else if (heads.contains(tmpNodeId)
                    && !this.sourceNodes.containsKey(tmpNodeId)
                    && !this.staticTable.contains(tmpNodeId)) {
                Map<String, Object> sourceNodeConfMap = new HashMap<>();
                sourceNodeConfMap.put("id", tmpNodeId);
                sourceNodeConfMap.put("name", tmpNodeId);
                sourceNodeConfMap.put("schedule_time", this.scheduleTime);
                SourceNode sourceNode = createHdfsSourceNode(tmpNodeId, sourceNodeConfMap);
                transformNode.getParents().add(sourceNode);
                this.sourceNodes.put(tmpNodeId, sourceNode);
                LOGGER.info(String.format("Added source node %s", tmpNodeId));
            } else if (!heads.contains(tmpNodeId)
                    && !this.tmpTransformNodesMap.containsKey(tmpNodeId)
                    && !this.staticTable.contains(tmpNodeId)) {
                transformNode.getParents().add(this.traverseBuildNodes(tmpNodeId));
            } else if (!heads.contains(tmpNodeId)
                    && this.tmpTransformNodesMap.containsKey(tmpNodeId)
                    && !this.staticTable.contains(tmpNodeId)) {
                transformNode.getParents().add(this.tmpTransformNodesMap.get(tmpNodeId));
            }

        }
    }

    private SourceNode createHdfsSourceNode(String resultTableId, Map<String, Object> sourceNodeConfMap) {
        HDFSStorageConstructor sourceStorageConstructor = new HDFSStorageConstructor(resultTableId);
        SourceNode sourceNode;
        if (!sourceStorageConstructor.getDataType().equals("iceberg")) {
            //为了兼容已有的hdfs parquet存储
            sourceNode = (new SQLDebugHDFSSourceNodeBuilder(sourceNodeConfMap)).build();
        } else {
            sourceNodeConfMap.put("iceberg_table_name", sourceStorageConstructor.getPhysicalName());
            sourceNodeConfMap.put("iceberg_conf", sourceStorageConstructor.getIcebergConfAsJava());
            sourceNode = (new SQLDebugIcebergNodeBuilder(sourceNodeConfMap)).build();
        }
        return sourceNode;
    }

    // this is for ignite data
    private void handleStaticData(List<String> staticData, BatchSQLTransformNode transformNode) {
        for (String igniteTableId : staticData) {
            if (!this.sourceNodes.containsKey(igniteTableId)) {
                Map<String, Object> igniteNodeConfMap = new HashMap<>();
                igniteNodeConfMap.put("id", igniteTableId);
                igniteNodeConfMap.put("name", igniteTableId);
                BatchIgniteSourceNode igniteNode =
                        (new BatchIgniteSourceNode.BatchIgniteSourceNodeBuilder(igniteNodeConfMap))
                            .build();
                this.staticTable.add(igniteTableId);
                transformNode.getParents().add(igniteNode);
                this.sourceNodes.put(igniteTableId, igniteNode);
                LOGGER.info(String.format("Added ignite source node %s", igniteTableId));
            } else {
                transformNode.getParents().add(this.sourceNodes.get(igniteTableId));
            }
        }
    }

    private void handleSelfDependency(TransformNode transformNode, Map<String, Object> submitArgsMap) {
        String resultTableId = transformNode.getNodeId();
        if (null != submitArgsMap.get("advanced")) {
            Map<String, Object> advancedMap = (Map<String, Object>)submitArgsMap.get("advanced");
            if (null != advancedMap.get("self_dependency")
                    && Boolean.parseBoolean(advancedMap.get("self_dependency").toString())) {
                Map<String, Object> sourceNodeConfMap = new HashMap<>();
                sourceNodeConfMap.put("id", resultTableId);
                sourceNodeConfMap.put("name", resultTableId);
                sourceNodeConfMap.put("schedule_time", this.scheduleTime);
                SourceNode sourceNode = createHdfsSourceNode(resultTableId, sourceNodeConfMap);
                transformNode.getParents().add(sourceNode);
                this.sourceNodes.put(resultTableId, sourceNode);
                LOGGER.info(String.format("Added self depend source node %s", resultTableId));
            }
        }
    }

    private void buildDebugUdf(String resultTableId) {
        List<Map<String, Object>> tmpUdfList = getUdfList(resultTableId);
        for (Map<String, Object> item1 : tmpUdfList) {
            boolean isIncluded = false;
            for (Map<String, Object> item2 : this.udfList) {
                if (item2.get("name").toString().equals(item1.get("name").toString())) {
                    isIncluded = true;
                    break;
                }
            }
            if (!isIncluded) {
                this.udfList.add(item1);
            }
        }
    }

    @Override
    public BatchSQLTopology build() {
        this.fillNodesFromSinkParents();
        BatchSQLTopology batchSQLTopology = new BatchSQLTopology(this);
        batchSQLTopology.setBatchType(BatchSQLType.SQL_DEBUG);
        return batchSQLTopology;
    }
}
