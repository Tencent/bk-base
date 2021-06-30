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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic;

import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.spark.sql.parser.SQLNodeJsonParam4JavaParser;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.topology.HDFSStorageConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode.BatchIgniteSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;

import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class PeriodicSQLTopoBuilder extends AbstractBatchTopologyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLTopoBuilder.class);

    SQLNodeJsonParam4JavaParser parser;

    private List<Node> sourceNodesList;
    private List<Node> transformNodesList;

    public PeriodicSQLTopoBuilder(Map<String, Object> parameters) {
        super(parameters);
        this.sourceNodesList = new LinkedList<>();
        this.transformNodesList = new LinkedList<>();
        this.parser = new SQLNodeJsonParam4JavaParser(this.getJobId());
        Tuple2<String, String> clusterTuple2 = APIUtil$.MODULE$.getRTCluster(this.getJobId());
        HadoopUtil.setClusterGroup(clusterTuple2._1());
        HadoopUtil.setClusterName(clusterTuple2._2());
        this.storageClusterGroup = clusterTuple2._1();
        // set this to true to enable pipeline metrics
        this.enableMetrics = true;
        buildNodes();
        this.buildUDF(parser.getUDFMapAsJava());
    }

    @Override
    public BatchSQLTopology build() {
        this.fillNodesFromSinkParents();
        BatchSQLTopology batchSQLTopology = new BatchSQLTopology(this);
        batchSQLTopology.setBatchType(BatchSQLType.BATCH_SQL);
        return batchSQLTopology;
    }

    private void buildNodes() {
        this.createSourceNodesList();
        this.createTransformNodesList();
        this.createSinkNodesMap();
    }

    private void createSourceNodesList() {
        // build source nodes
        this.parser.getParentsIDsAsJava().forEach(
                (id, nodeConf) -> {
                    LOGGER.info(String.format("Start to build source node %s", id));
                    String storageType = "hdfs";
                    String selfDependencyMode = "";
                    Map<String, Object> sourceNodeConfMap = new HashMap<>();
                    sourceNodeConfMap.put("id", id);
                    sourceNodeConfMap.put("name", id);
                    sourceNodeConfMap.put("job_type", this.getJobType());
                    sourceNodeConfMap.put("schedule_time", this.scheduleTime);
                    sourceNodeConfMap.put("accumulate", parser.isAccumulate());
                    sourceNodeConfMap.put("data_start", parser.getDataStart());
                    sourceNodeConfMap.put("data_end", parser.getDataEnd());

                    for (Map.Entry<String, Object> entry : nodeConf.entrySet()) {
                        sourceNodeConfMap.put(entry.getKey(), entry.getValue());
                        if ("storage_type".equals(entry.getKey().toLowerCase())) {
                            storageType = entry.getValue().toString().toLowerCase();
                        }
                        if (entry.getKey().toLowerCase().equals("self_dependency_mode")) {
                            selfDependencyMode = entry.getValue().toString().toLowerCase();
                        }
                    }

                    SourceNode sourceNode;
                    if ("hdfs".equals(storageType) && "".equals(selfDependencyMode)) {
                        HDFSStorageConstructor sourceStorageConstructor = new HDFSStorageConstructor(id);
                        if (!"iceberg".equals(sourceStorageConstructor.getDataType())) {
                            //为了兼容已有的hdfs parquet存储
                            sourceNode = (new PeriodicSQLHDFSSourceNodeBuilder(sourceNodeConfMap)).build();
                        } else {
                            sourceNodeConfMap.put("iceberg_table_name", sourceStorageConstructor.getPhysicalName());
                            sourceNodeConfMap.put("iceberg_conf", sourceStorageConstructor.getIcebergConfAsJava());
                            sourceNode = (new PeriodicSQLIcebergSourceBuilder(sourceNodeConfMap)).build();
                        }

                    } else if ("hdfs".equals(storageType)
                            && ("upstream".equals(selfDependencyMode)
                            || "current".equals(selfDependencyMode))) {
                        HDFSStorageConstructor sourceStorageConstructor = new HDFSStorageConstructor(id);
                        if (!"iceberg".equals(sourceStorageConstructor.getDataType())) {
                            sourceNode = (new PeriodicSQLHDFSCopySourceBuilder(sourceNodeConfMap)).build();
                        } else {
                            sourceNodeConfMap.put("iceberg_table_name", sourceStorageConstructor.getPhysicalName());
                            sourceNodeConfMap.put("iceberg_conf", sourceStorageConstructor.getIcebergConfAsJava());
                            sourceNode = (new PeriodicSQLIcebergCopySourceBuilder(sourceNodeConfMap)).build();
                        }
                    } else if ("ignite".equals(storageType)) {
                        sourceNode =
                                (new BatchIgniteSourceNodeBuilder(sourceNodeConfMap)).build();
                    } else {
                        throw new RuntimeException(String.format("Can't find valid source node type for %s", id));
                    }
                    this.sourceNodesList.add(sourceNode);
                }
        );
    }

    private void createTransformNodesList() {
        // build transform nodes
        LOGGER.info(String.format("Start to build transform node %s", this.getJobId()));
        Map<String, Object> transformNodeConfMap = new HashMap<>();
        transformNodeConfMap.put("id", this.getJobId());
        transformNodeConfMap.put("name", this.getJobId());
        transformNodeConfMap.put("sql", this.parser.getSQL());
        transformNodeConfMap.put("period_value", this.parser.getCountFreq());
        transformNodeConfMap.put("period_unit", this.parser.getSchedulePeriodStr());
        BatchSQLTransformNode transformNode = (new PeriodicSQLTransformNodeBuilder(transformNodeConfMap)).build();

        transformNode.setParents(this.sourceNodesList);
        this.transformNodesList.add(transformNode);
    }

    private void createSinkNodesMap() {
        // build sink nodes
        LOGGER.info(String.format("Start to build sink node %s", this.getJobId()));
        Map<String, Object> sinkNodeConfMap = new HashMap<>();
        sinkNodeConfMap.put("id", this.getJobId());
        sinkNodeConfMap.put("name", this.getJobId());
        sinkNodeConfMap.put("job_type", this.getJobType());
        sinkNodeConfMap.put("storages", this.parser.getStoragesMapAsJava());
        sinkNodeConfMap.put("schedule_time", this.scheduleTime);

        Tuple2 minWindow = this.parser.getMinWindowSize();

        sinkNodeConfMap.put("min_window_size", (Integer) minWindow._1());
        sinkNodeConfMap.put("min_window_unit", minWindow._2());

        HDFSStorageConstructor sinkStorageConstructor = new HDFSStorageConstructor(this.getJobId());

        AbstractBatchSinkNode sinkNode;
        if (!"iceberg".equals(sinkStorageConstructor.getDataType())) {
            sinkNode = (new PeriodicSQLHDFSSinkNodeBuilder(sinkNodeConfMap)).build();
        } else {
            sinkNodeConfMap.put("iceberg_table_name", sinkStorageConstructor.getPhysicalName());
            sinkNodeConfMap.put("iceberg_conf", sinkStorageConstructor.getIcebergConfAsJava());
            sinkNode = (new PeriodicSQLIcebergSinkBuilder(sinkNodeConfMap)).build();
        }

        sinkNode.setParents(this.transformNodesList);
        this.sinkNodes.put(sinkNode.getNodeId(), sinkNode);
    }
}
