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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter$;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.AbstractBatchTransformNode;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PeriodicSQLv2TopoBuilder extends AbstractBatchTopologyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLv2TopoBuilder.class);

    private Map<String, Object> runtimeParams;
    private Map<String, Object> nodesInfo;

    private long startTimeStamp;
    private long endTimeStamp;

    private List<Node> sourceNodesList;
    private List<Node> transformNodesList;

    public PeriodicSQLv2TopoBuilder(Map<String, Object> parameters) {
        super(parameters);
        this.startTimeStamp = -1;
        this.endTimeStamp = -1;
        this.sourceNodesList = new LinkedList<>();
        this.transformNodesList = new LinkedList<>();
        this.runtimeParams = APIUtil$.MODULE$.fetchBKSQLFullParamsAsJava(this.getJobId());
        this.nodesInfo = (Map<String, Object> )this.runtimeParams.get("nodes");
        this.enableMetrics = true;
        this.setHadoopClusterInfo();
        this.buildNodes();

        this.buildUDF(this.getUDF());
    }

    private void buildNodes() {
        this.createSourceNodeList();
        this.createTransformNodeList();
        this.createSinkNodeList();
    }

    private void createSourceNodeList() {
        Map<String, Object> sourceNodesInfo = this.getSourceNodes();
        for (Map.Entry<String, Object> sourceNodeEntry : sourceNodesInfo.entrySet()) {
            LOGGER.info(String.format("Start to build source node %s", sourceNodeEntry.getKey()));
            Map<String, Object> sourceNodeInfo = (Map<String, Object>) sourceNodeEntry.getValue();
            Map<String, Object> sourceNodeConfMap = new HashMap<>(sourceNodeInfo);
            sourceNodeConfMap.put("schedule_time", this.scheduleTime);

            String storageType =
                    sourceNodeInfo.getOrDefault("storage_type", "hdfs").toString().toLowerCase();
            String selfDependencyMode = sourceNodeInfo.getOrDefault("self_dependency_mode", "")
                    .toString().toLowerCase();

            long currentStartTimeStamp = -1;
            long currentEndTimeStamp = -1;
            AbstractBatchSourceNode sourceNode;
            if ("hdfs".equals(storageType)) {
                Map<String, Object> storageConfMap = (Map<String, Object>) sourceNodeInfo.get("storage_conf");
                String dataType = storageConfMap.getOrDefault("data_type", "parquet").toString().toLowerCase();
                if ("upstream".equals(selfDependencyMode) || "current".equals(selfDependencyMode)) {
                    if ("iceberg".equals(dataType)) {
                        PeriodicSQLv2IcebergCopySourceNodeBuilder builder =
                                new PeriodicSQLv2IcebergCopySourceNodeBuilder(sourceNodeConfMap);
                        sourceNode = builder.build();
                        currentStartTimeStamp = builder.getWindowAnalyzer().getStartTimeInMilliseconds();
                        currentEndTimeStamp = builder.getWindowAnalyzer().getEndTimeInMilliseconds();
                    } else {
                        PeriodicSQLv2HDFSCopySourceNodeBuilder builder =
                                new PeriodicSQLv2HDFSCopySourceNodeBuilder(sourceNodeConfMap);
                        sourceNode = builder.build();
                        currentStartTimeStamp = builder.getWindowAnalyzer().getStartTimeInMilliseconds();
                        currentEndTimeStamp = builder.getWindowAnalyzer().getEndTimeInMilliseconds();
                    }
                } else {
                    if ("iceberg".equals(dataType)) {
                        PeriodicSQLv2IcebergSourceNodeBuilder builder =
                                new PeriodicSQLv2IcebergSourceNodeBuilder(sourceNodeConfMap);
                        sourceNode = builder.build();
                        currentStartTimeStamp = builder.getWindowAnalyzer().getStartTimeInMilliseconds();
                        currentEndTimeStamp = builder.getWindowAnalyzer().getEndTimeInMilliseconds();
                    } else {
                        PeriodicSQLv2HDFSSourceNodeBuilder builder =
                                new PeriodicSQLv2HDFSSourceNodeBuilder(sourceNodeConfMap);
                        sourceNode = builder.build();
                        currentStartTimeStamp = builder.getWindowAnalyzer().getStartTimeInMilliseconds();
                        currentEndTimeStamp = builder.getWindowAnalyzer().getEndTimeInMilliseconds();
                    }
                }

            } else if ("ignite".equals(storageType)) {
                sourceNode = (new PeriodicSQLv2IgniteSourceNodeBuilder(sourceNodeConfMap)).build();
            } else {
                throw new RuntimeException(String.format("Can't find valid source node type for %s",
                        sourceNodeEntry.getKey()));
            }

            if (this.startTimeStamp == -1 || currentStartTimeStamp < this.startTimeStamp) {
                this.startTimeStamp = currentStartTimeStamp;
            }

            if (this.endTimeStamp == -1 || this.endTimeStamp < currentEndTimeStamp) {
                this.endTimeStamp = currentEndTimeStamp;
            }
            this.sourceNodesList.add(sourceNode);
        }
    }

    private void createTransformNodeList() {
        Map<String, Object> transformNodesInfo = this.getTransformNodes();
        for (Map.Entry<String, Object> transformNodeEntry : transformNodesInfo.entrySet()) {
            LOGGER.info(String.format("Start to build transform node %s", transformNodeEntry.getKey()));
            Map<String, Object> transformNodeInfo = (Map<String, Object>) transformNodeEntry.getValue();
            AbstractBatchTransformNode transformNode =
                    (new PeriodicSQLv2TransformNodeBuilder(transformNodeInfo)).build();
            transformNode.setParents(sourceNodesList);
            this.transformNodesList.add(transformNode);
        }
    }

    private void createSinkNodeList() {
        Map<String, Object> sinkNodesInfo = this.getSinkNodes();
        for (Map.Entry<String, Object> sinkNodeEntry : sinkNodesInfo.entrySet()) {
            LOGGER.info(String.format("Start to build sink node %s", sinkNodeEntry.getKey()));
            Map<String, Object> sinkNodeInfo = (Map<String, Object>) sinkNodeEntry.getValue();
            Map<String, Object> storageConfMap = (Map<String, Object>) sinkNodeInfo.get("storage_conf");
            String dataType = storageConfMap.getOrDefault("data_type", "parquet")
                    .toString().toLowerCase();

            Map<String, Object> sinkNodeConfMap = new HashMap<>(sinkNodeInfo);
            sinkNodeConfMap.put("schedule_time", this.scheduleTime);
            AbstractBatchSinkNode sinkNode;
            if ("iceberg".equals(dataType)) {
                sinkNode = (new PeriodicSQLv2IcebergSinkNodeBuilder(sinkNodeConfMap)).build();
            } else {
                sinkNode = (new PeriodicSQLv2HDFSSinkNodeBuilder(sinkNodeConfMap)).build();
            }
            sinkNode.setParents(this.transformNodesList);

            if (this.startTimeStamp != -1) {
                String startTimeStr = TimeFormatConverter$.MODULE$.sdf2().format(this.startTimeStamp);
                sinkNode.setStartTimeString(startTimeStr);
            }

            if (this.endTimeStamp != -1) {
                String endTimeStr = TimeFormatConverter$.MODULE$.sdf2().format(this.endTimeStamp);
                sinkNode.setEndTimeString(endTimeStr);
            }
            this.sinkNodes.put(sinkNode.getNodeId(), sinkNode);

        }
    }

    private Map<String, Object> getUDF() {
        List<Map<String, Object>> udfList =
                (List<Map<String, Object>>)CommonUtil$.MODULE$.collectionToJava(this.runtimeParams.get("udf"));
        List<Map<String, Object>> udfInfoList = new ArrayList<>();
        for (Map<String, Object> udf : udfList) {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> udfInfoMap = null;
            try {
                udfInfoMap = objectMapper.readValue(udf.get("udf_info").toString(), HashMap.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            udfInfoList.add(udfInfoMap);
        }
        Map<String, Object> udfMap = new HashMap<>();
        udfMap.put("config", udfInfoList);
        return udfMap;
    }

    private Map<String, Object> getSourceNodes() {
        return (Map<String, Object> )this.nodesInfo.get("source");
    }

    private Map<String, Object> getTransformNodes() {
        return (Map<String, Object> )this.nodesInfo.get("transform");
    }

    private Map<String, Object> getSinkNodes() {
        return (Map<String, Object> )this.nodesInfo.get("sink");
    }

    private void setHadoopClusterInfo() {
        Map<String, Object> sinkNodesInfo = this.getSinkNodes();
        Map<String, Object> info = (Map<String, Object>)sinkNodesInfo.get(this.getJobId());
        Map<String, Object> storageMap = (Map<String, Object>)info.get("storage_conf");
        this.storageClusterGroup = storageMap.get("cluster_group").toString();
        HadoopUtil.setClusterGroup(this.storageClusterGroup);
        HadoopUtil.setClusterName(storageMap.get("cluster_name").toString());
    }

    @Override
    public BatchTopology build() {
        this.fillNodesFromSinkParents();
        BatchSQLTopology batchSQLTopology = new BatchSQLTopology(this);
        batchSQLTopology.setBatchType(BatchSQLType.BATCH_SQL);
        return batchSQLTopology;
    }
}