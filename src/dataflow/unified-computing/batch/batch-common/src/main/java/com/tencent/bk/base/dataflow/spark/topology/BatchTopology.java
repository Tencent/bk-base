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

package com.tencent.bk.base.dataflow.spark.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctionConfig;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BatchTopology extends Topology {

    protected long scheduleTime;
    protected String storageClusterGroup;
    protected String executeId;
    protected String geogAreaCode;
    protected boolean enableMetrics;

    protected boolean isDebug;
    //这个makeup应该表示补算
    protected boolean isMakeup;

    public BatchTopology(AbstractBatchTopologyBuilder builder) {
        super(builder);
        this.scheduleTime = builder.scheduleTime;
        this.storageClusterGroup = builder.storageClusterGroup;
        this.executeId = builder.executeId;
        this.geogAreaCode = builder.geogAreaCode;
        this.enableMetrics = builder.enableMetrics;
        this.isDebug = builder.isDebug;
        this.isMakeup = builder.isMakeup;
        this.setSourceNodes(builder.sourceNodes);
        this.setTransformNodes(builder.transformNodes);
        this.setSinkNodes(builder.sinkNodes);
        this.setUserDefinedFunctions(builder.userDefinedFunctions);
    }

    public String getExecuteId() {
        return this.executeId;
    }

    public Boolean enableMetrics() {
        return enableMetrics;
    }

    public String getStorageClusterGroup() {
        return this.storageClusterGroup;
    }

    public String getGeogAreaCode() {
        return geogAreaCode;
    }

    public Boolean isDebug() {
        return this.isDebug;
    }

    public boolean isMakeup() {
        return isMakeup;
    }

    public abstract static class AbstractBatchTopologyBuilder extends AbstractBuilder {
        private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchTopologyBuilder.class);

        protected long scheduleTime;
        protected String executeId;
        protected String storageClusterGroup = "default";
        protected UserDefinedFunctions userDefinedFunctions;
        protected String geogAreaCode;
        protected boolean enableMetrics = false;

        protected boolean isDebug;
        protected boolean isMakeup;

        protected Map<String, SourceNode> sourceNodes = new LinkedHashMap<>();
        protected Map<String, TransformNode> transformNodes = new LinkedHashMap<>();
        protected Map<String, SinkNode> sinkNodes = new LinkedHashMap<>();

        /**
         * 初始化BatchTopologyBuilder
         * @param parameters batch拓扑参数
         */
        public AbstractBatchTopologyBuilder(Map<String, Object> parameters) {
            super(parameters);
            this.executeId = parameters.get("execute_id").toString();
            if (null != parameters.get("schedule_time")) {
                this.scheduleTime = (long) parameters.get("schedule_time");
            }

            if (null != parameters.get("geog_area_code")) {
                this.geogAreaCode = parameters.get("geog_area_code").toString();
            }
            if (null != parameters.get("makeup")) {
                this.isMakeup = Boolean.parseBoolean(parameters.get("makeup").toString());
            } else {
                this.isMakeup = false;
            }

            if (null != parameters.get("debug")) {
                this.isDebug = Boolean.parseBoolean(parameters.get("debug").toString());
            } else {
                this.isDebug = false;
            }
        }

        @Override
        public abstract BatchTopology build();

        protected void fillNodesFromSinkParents() {
            Stack<Node> nodeStack = new Stack<>();
            for (Map.Entry<String, SinkNode> entry : this.sinkNodes.entrySet()) {
                LOGGER.info(String.format("Found sink node in topology: %s -> %s", entry.getKey(),
                        entry.getValue().toString()));
                traverseNode(entry.getValue(), nodeStack);
            }

            while (!nodeStack.empty()) {
                Node node = nodeStack.pop();
                if (node instanceof SourceNode && !this.sourceNodes.containsKey(node.getNodeId())) {
                    LOGGER.info(String.format("Topology add source node: %s -> %s", node.getNodeId(), node.toString()));
                    this.sourceNodes.put(node.getNodeId(), (SourceNode) node);
                } else if (node instanceof TransformNode && !this.transformNodes.containsKey(node.getNodeId())) {
                    LOGGER.info(String.format("Topology add transform node: %s -> %s", node.getNodeId(),
                            node.toString()));
                    this.transformNodes.put(node.getNodeId(), (TransformNode) node);
                }
            }
        }

        protected void traverseNode(Node node, Stack<Node> nodeStack) {
            for (Node tmpNode : node.getParents()) {
                nodeStack.push(tmpNode);
                traverseNode(tmpNode, nodeStack);
            }
        }

        protected void buildUDF(Map<String, Object> udf) {
            UserDefinedFunctions.Builder builder = UserDefinedFunctions.builder();
            if (udf != null) {
                udf.forEach((key, value) -> {
                    if (key.equalsIgnoreCase("source_data")) {
                        builder.setDebugSourceData((List<Map<String, Object>>) value);
                    } else if (key.equalsIgnoreCase("config")) {
                        ((List<Map<String, String>>) value)
                                .forEach(item -> builder.addFunction(
                                        new UserDefinedFunctionConfig(item.get("language"), item.get("name"),
                                                item.get("type"), item.get("hdfs_path"), item.get("local_path"))));
                    }
                });
            }
            this.userDefinedFunctions = builder.create();
        }

        protected List<Map<String, Object>> getUdfList(String processingId) {
            List<Map<String, Object>> udfProcessingList = APIUtil$.MODULE$.getProcessingUdfAsJava(processingId);
            ObjectMapper objectMapper = new ObjectMapper();

            List<Map<String, Object>> resultList = new LinkedList<>();
            for (Map<String, Object> value : udfProcessingList) {
                Map<String, Object> udfMap;
                try {
                    udfMap = objectMapper.readValue(value.get("udf_info").toString(), HashMap.class);
                    resultList.add(udfMap);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return resultList;
        }
    }
}

