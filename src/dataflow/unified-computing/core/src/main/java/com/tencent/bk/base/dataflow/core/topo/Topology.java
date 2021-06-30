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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TopoNodeType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topology implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);

    private String jobId;
    private String jobName;
    private String jobType;
    private String runMode;
    /**
     * source node list 用于存放读取数据源的node
     */
    @JsonIgnore
    private Map<String, SourceNode> sourceNodes = new LinkedHashMap<>();
    /**
     * transform node list 存放各种transform node
     */
    @JsonIgnore
    private Map<String, TransformNode> transformNodes = new LinkedHashMap<>();
    @JsonIgnore
    private Map<String, SinkNode> sinkNodes = new LinkedHashMap<>();
    /**
     * udf 信息
     */
    private UserDefinedFunctions userDefinedFunctions;

    public Topology(AbstractBuilder builder) {
        this.jobId = builder.getJobId();
        this.jobName = builder.getJobName();
        this.jobType = builder.getJobType();
        this.runMode = builder.getRunMode();
    }

    /**
     * 解析udf信息
     *
     * @param udf udf参数
     */
    public void mapUdfs(Map<String, Object> udf) {
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
        this.setUserDefinedFunctions(builder.create());
    }

    /**
     * 映射node
     *
     * @param typeTables node的参数
     */
    public void mapNodes(Map<String, Map<String, Object>> typeTables) {
        String jobTypeStr = this.getJobType();
        Component jobType = ConstantVar.Component.valueOf(jobTypeStr);
        Map<String, SourceNode> sourceNodes = new HashMap<>();
        Map<String, TransformNode> transformNode = new HashMap<>();
        Map<String, SinkNode> sinkNodes = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : typeTables.entrySet()) {
            for (Object serialzedNodeObj : entry.getValue().values()) {
                Node node;
                TopoNodeType nodeType = TopoNodeType.valueOf(entry.getKey());
                switch (nodeType) {
                    case source:
                        node = new SourceNode();
                        parseNode(node, serialzedNodeObj, jobType);
                        sourceNodes.put(node.getNodeId(), (SourceNode) node);
                        break;
                    case transform:
                        node = new TransformNode();
                        parseNode(node, serialzedNodeObj, jobType);
                        transformNode.put(node.getNodeId(), (TransformNode) node);
                        break;
                    case sink:
                        node = new SinkNode();
                        parseNode(node, serialzedNodeObj, jobType);
                        sinkNodes.put(node.getNodeId(), (SinkNode) node);
                        break;
                    default:
                        throw new IllegalArgumentException("node type " + entry.getKey() + " is not support.");
                }
                LOGGER.info("Generate result table: " + node.getNodeId());
            }
        }

        this.setSourceNodes(sourceNodes);
        this.setTransformNodes(transformNode);
        this.setSinkNodes(sinkNodes);

        describeNodesRelationshipI(typeTables);
    }

    /**
     * 根据任务配置构建节点的上下游关系
     * todo 针对离线自依赖场景，需增加两个逻辑:
     * todo 1. 当 transform node id 的 parent 中包含自身相同id时（即自依赖节点)，那么它的上游节点为source 自依赖节点
     * todo 2. 当 transform node id 的 parent 中包含自依赖节点id时，那么它的上游节点为 transform自依赖节点
     *
     * @param typeTables 结构为(node type, (node id, node info))
     */
    protected void describeNodesRelationshipI(Map<String, Map<String, Object>> typeTables) {
        //描述父子node之间的关系
        for (Map.Entry<String, Map<String, Object>> entry : typeTables.entrySet()) {
            for (Object serialzedNodeObj : entry.getValue().values()) {
                Map<String, Object> serialzedNode = (Map<String, Object>) serialzedNodeObj;
                if (serialzedNode.get("parents") != null) {
                    String id = serialzedNode.get("id").toString();
                    Node node = this.queryNode(id);
                    List<String> serializedParents = (List<String>) serialzedNode.get("parents");
                    for (final String parentId : serializedParents) {
                        Node parentNode = this.queryNode(parentId);
                        if (parentNode != null) {
                            node.getParents().add(parentNode);
                            parentNode.getChildren().add(node);
                        }
                    }
                }
            }
        }
    }

    private void parseNode(Node node, Object serialzedNodeObj, ConstantVar.Component jobType) {
        Map<String, Object> serialzedNode = (Map<String, Object>) serialzedNodeObj;
        node.map(serialzedNode, jobType);
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobType() {
        return jobType;
    }

    public String getRunMode() {
        return runMode;
    }

    public Map<String, SourceNode> getSourceNodes() {
        return sourceNodes;
    }

    public void setSourceNodes(Map<String, SourceNode> sourceNodes) {
        this.sourceNodes = sourceNodes;
    }

    public Map<String, TransformNode> getTransformNodes() {
        return transformNodes;
    }

    public void setTransformNodes(Map<String, TransformNode> transformNodes) {
        this.transformNodes = transformNodes;
    }

    public void setTransformNodes(List<TransformNode> transformNodes) {
        Map<String, TransformNode> maps = new LinkedHashMap<>();
        for (TransformNode transformNode : transformNodes) {
            maps.put(transformNode.getNodeId(), transformNode);
        }
        this.transformNodes = maps;
    }

    public Map<String, SinkNode> getSinkNodes() {
        return sinkNodes;
    }

    public void setSinkNodes(Map<String, SinkNode> sinkNodes) {
        this.sinkNodes = sinkNodes;
    }

    public Node queryNode(String nodeId) {
        if (sourceNodes.get(nodeId) != null) {
            return sourceNodes.get(nodeId);
        } else if (transformNodes.get(nodeId) != null) {
            return transformNodes.get(nodeId);
        } else {
            throw new RuntimeException("Not found the node " + nodeId);
        }
    }

    /**
     * topology的root和body顺序展示
     *
     * @return
     */
    public Map<String, Object> serialize() {
        return new HashMap<String, Object>() {
            {
                put("source", sourceNodes);
                put("transform", transformNodes);
                put("sink", sinkNodes);
            }
        };
    }

    public UserDefinedFunctions getUserDefinedFunctions() {
        return userDefinedFunctions;
    }

    public void setUserDefinedFunctions(UserDefinedFunctions userDefinedFunctions) {
        this.userDefinedFunctions = userDefinedFunctions;
    }

    public abstract static class AbstractBuilder {

        private String jobId;
        private String jobName;
        private String jobType;
        private String runMode;

        public AbstractBuilder(Map<String, Object> parameters) {
            this.jobId = parameters.get("job_id").toString();
            this.jobName = parameters.get("job_name").toString();
            this.jobType = parameters.get("job_type").toString();
            this.runMode = parameters.get("run_mode").toString();
        }

        public String getJobId() {
            return this.jobId;
        }

        public String getJobName() {
            return this.jobName;
        }

        public String getJobType() {
            return this.jobType;
        }

        public String getRunMode() {
            return this.runMode;
        }

        public abstract Topology build();

    }
}
