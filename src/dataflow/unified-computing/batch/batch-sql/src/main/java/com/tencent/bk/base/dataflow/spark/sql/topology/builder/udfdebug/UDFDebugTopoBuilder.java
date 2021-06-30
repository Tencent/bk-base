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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.udfdebug;

import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugTopoBuilder;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchJsonSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class UDFDebugTopoBuilder extends AbstractBatchTopologyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLDebugTopoBuilder.class);

    public UDFDebugTopoBuilder(Map<String, Object> parameters) {
        super(parameters);
        this.enableMetrics = false;
        buildNodes(parameters);
        this.fillNodesFromSinkParents();
        Map<String, Object> udfMap = (Map<String, Object>)parameters.get("udf");
        this.buildUDF(udfMap);
    }

    private void buildNodes(Map<String, Object> parameters) {
        Map<String, Object> udfMap = (Map<String, Object>)parameters.get("udf");
        Map<String, Object> nodesConfMap = (Map<String, Object>)parameters.get("nodes");

        Map<String, Object> sourceNodesConfMap = (Map<String, Object>)nodesConfMap.get("source");
        List<Node> sourceNodeList = new LinkedList<>();

        Map<String, Object> sourceConf =
                (Map<String, Object>)sourceNodesConfMap.entrySet().stream().findFirst().get().getValue();
        List<Map<String, Object>> sourceData = (List<Map<String, Object>>)udfMap.get("source_data");
        sourceConf.put("source_data", sourceData);
        BatchJsonSourceNode jsonSourceNode =
                (new BatchJsonSourceNode.BatchJsonSourceNodeBuilder(sourceConf)).build();
        sourceNodeList.add(jsonSourceNode);


        Map<String, Object> transformNodesConfMap = (Map<String, Object>)nodesConfMap.get("transform");
        Map<String, Object> transformParameters =
                (Map<String, Object>)transformNodesConfMap.entrySet().stream().findFirst().get().getValue();
        Map<String, Object> transformConf = new HashMap<>();
        String transformNodeId = transformParameters.get("id").toString();
        transformConf.put("id", transformNodeId);
        transformConf.put("name", transformParameters.get("name").toString());
        Map<String, Object> processorConf = (Map<String, Object>)transformParameters.get("processor");
        transformConf.put("sql", processorConf.get("processor_args").toString());
        BatchSQLTransformNode transformNode =
                (new BatchSQLTransformNode.BatchSQLTransformNodeBuilder(transformConf)).build();
        transformNode.setParents(sourceNodeList);

        List<Node> transformNodeList = new LinkedList<>();
        transformNodeList.add(transformNode);

        Map<String, Object> debugHttpSinkConf = new HashMap<>();
        debugHttpSinkConf.put("id", transformNodeId);
        debugHttpSinkConf.put("name", transformNodeId);
        debugHttpSinkConf.put("debug_id", this.getJobId());
        debugHttpSinkConf.put("execute_id", this.executeId);
        debugHttpSinkConf.put("enable_throw_exception", true);
        BatchDebugHTTPSinkNode debugHTTPSinkNode =
                new BatchDebugHTTPSinkNode.BatchDebugHTTPSinkNodeBuilder(debugHttpSinkConf).build();
        debugHTTPSinkNode.setParents(transformNodeList);
        this.sinkNodes.put(transformNodeId, debugHTTPSinkNode);
    }

    @Override
    public BatchSQLTopology build() {
        this.fillNodesFromSinkParents();
        BatchSQLTopology batchSQLTopology = new BatchSQLTopology(this);
        batchSQLTopology.setBatchType(BatchSQLType.UDF_DEBUG);
        return batchSQLTopology;
    }
}
