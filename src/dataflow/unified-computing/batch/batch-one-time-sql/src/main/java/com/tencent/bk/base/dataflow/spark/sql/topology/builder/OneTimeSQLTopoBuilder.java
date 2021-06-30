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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder;

import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchOneTimeSQLTopology;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OneTimeSQLTopoBuilder extends AbstractBatchTopologyBuilder {

    private Map<String, Object> oneTimeSqlParam;

    public OneTimeSQLTopoBuilder(Map<String, Object> parameters) {
        super(parameters);
    }

    private void buildNodes() {
        Map<String, Object> nodes = (Map<String, Object>) this.oneTimeSqlParam.get("nodes");

        List<Node> sourceNodesList = new LinkedList<>();

        Map<String, Object> sourceNodesConf = (Map<String, Object>) nodes.get("source");
        for (Map.Entry<String, Object> item : sourceNodesConf.entrySet()) {
            Map<String, Object> input = (Map<String, Object>) ((Map<String, Object>) item.getValue()).get("input");
            String storageType = input.get("type").toString().toLowerCase();
            Map<String, Object> inputConf = (Map<String, Object>) input.get("conf");
            String inputDataType = inputConf.get("data_type") == null ? "" : inputConf.get("data_type").toString();
            AbstractBatchSourceNode sourceNode;
            if ("ignite".equals(storageType)) {
                sourceNode = (new BatchIgniteSourceNode.BatchIgniteSourceNodeBuilder(
                        (Map<String, Object>) item.getValue())).build();
            } else if ("iceberg".equals(inputDataType)) {
                sourceNode = (new OneTimeSQLIcebergSourceNodeBuilder((Map<String, Object>) item.getValue())).build();
            } else {
                sourceNode = (new OneTimeSQLHDFSSourceNodeBuilder((Map<String, Object>) item.getValue())).build();
            }
            sourceNodesList.add(sourceNode);
        }

        Map<String, Object> transformNodesConf = (Map<String, Object>) nodes.get("transform");
        Map<String, Object> transformConf = (Map<String, Object>) transformNodesConf.entrySet().stream().findFirst()
                .get().getValue();
        String sql = ((Map<String, Object>) transformConf.get("processor")).get("processor_args").toString();
        Map<String, Object> transformInfo = new HashMap<>();
        transformInfo.put("sql", sql);
        transformInfo.put("id", transformConf.get("id").toString());
        transformInfo.put("name", transformConf.get("name").toString());
        BatchSQLTransformNode transformNode = (new BatchSQLTransformNode.BatchSQLTransformNodeBuilder(transformInfo))
                .build();
        transformNode.setParents(sourceNodesList);
        List<Node> transformNodesList = new LinkedList<>();
        transformNodesList.add(transformNode);

        Map<String, Object> sinkNodesConf = (Map<String, Object>) nodes.get("sink");
        Map<String, Object> sinkConf = (Map<String, Object>) sinkNodesConf.entrySet().stream().findFirst().get()
                .getValue();

        Map<String, Object> output = (Map<String, Object>) sinkConf.get("output");
        Map<String, Object> outputConf = (Map<String, Object>) output.get("conf");
        String outputDataType = outputConf.get("data_type") == null ? null : outputConf.get("data_type").toString();
        AbstractBatchSinkNode sinkNode;
        if (null != outputDataType && "iceberg".equals(outputDataType)) {
            sinkNode = (new OneTimeSQLIcebergSinkNodeBuilder(sinkConf)).build();
        } else {
            sinkNode = (new OneTimeSQLHDFSSinkNodeBuilder(sinkConf)).build();
        }
        sinkNode.setParents(transformNodesList);
        this.sinkNodes.put(sinkNode.getNodeId(), sinkNode);
    }

    private String parseStorageClusterGroup() {
        Map<String, Object> nodes = (Map<String, Object>) this.oneTimeSqlParam.get("nodes");
        Map<String, Object> sinkNodesConf = (Map<String, Object>) nodes.get("sink");
        Map<String, Object> sinkConf = (Map<String, Object>) sinkNodesConf.entrySet().stream().findFirst().get()
                .getValue();
        Map<String, Object> outputMap = (Map<String, Object>) sinkConf.get("output");
        Map<String, String> storageMap = (Map<String, String>) outputMap.get("conf");
        String clusterGroup = storageMap.get("cluster_group");
        return clusterGroup;
    }

    protected Map<String, Object> getOneTimeSqlParam() {
        return APIUtil$.MODULE$.fetchOneTimeSqlFullParamsAsJava(this.getJobId());
    }

    @Override
    public BatchOneTimeSQLTopology build() {
        this.oneTimeSqlParam = this.getOneTimeSqlParam();
        this.storageClusterGroup = this.parseStorageClusterGroup();
        buildNodes();
        this.buildUDF((Map<String, Object>) this.oneTimeSqlParam.get("udf"));
        this.fillNodesFromSinkParents();
        return new BatchOneTimeSQLTopology(this);
    }
}
