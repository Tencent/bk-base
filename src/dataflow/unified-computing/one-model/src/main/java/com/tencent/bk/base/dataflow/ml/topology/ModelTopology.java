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

package com.tencent.bk.base.dataflow.ml.topology;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions;
import com.tencent.bk.base.dataflow.ml.metric.OneModelDebugConfig;
import com.tencent.bk.base.dataflow.ml.metric.OneModelMetricConfig;
import com.tencent.bk.base.dataflow.ml.node.ModelSourceBuilderFactory;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformerBuilderFactory;
import com.tencent.bk.base.dataflow.ml.node.AbstractModelBuilderFactory;
import com.tencent.bk.base.dataflow.ml.node.ModelSinkBuilderFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelTopology extends Topology {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelTopology.class);
    private String timeZone;
    private String queueName;
    private String clusterGroup;
    private boolean isDebug;
    private OneModelDebugConfig debugConfig;
    private OneModelMetricConfig metricConfig;

    public ModelTopology(ModelTopologyBuilder builder) {
        super(builder);
        this.timeZone = builder.timeZone;
        this.queueName = builder.resource.get("queue_name");
        this.clusterGroup = builder.resource.get("cluster_group");
        this.isDebug = builder.isDebug;
        this.debugConfig = builder.debugConfig;
        this.metricConfig = builder.metricConfig;
        builder.buildNodes();
        this.setSourceNodes(builder.sourceNodes);
        this.setTransformNodes(builder.transformNodes);
        this.setSinkNodes(builder.sinkNodes);
        super.describeNodesRelationshipI(builder.nodes);
        this.setUserDefinedFunctions(builder.userDefinedFunctions);
    }

    public String getQueueName() {
        return queueName;
    }

    public String getClusterGroup() {
        return clusterGroup;
    }

    public Boolean isDebug() {
        return this.isDebug;
    }

    public OneModelDebugConfig getDebugConfig() {
        return this.debugConfig;
    }

    public void setDebugConfig(OneModelDebugConfig config) {
        this.debugConfig = config;
    }

    public OneModelMetricConfig getMetricConfig() {
        return this.metricConfig;
    }

    public static class ModelTopologyBuilder extends AbstractBuilder {

        protected long scheduleTime;
        protected String dataFlowUrl;
        private String timeZone;
        private Map<String, Map<String, Object>> nodes;
        private Map<String, String> resource;
        private boolean isDebug;
        private OneModelDebugConfig debugConfig;
        private OneModelMetricConfig metricConfig;
        private UserDefinedFunctions userDefinedFunctions;
        /**
         * source node list
         * 用于存放读取数据源的node
         */
        private Map<String, SourceNode> sourceNodes = new LinkedHashMap<>();

        /**
         * transform node list
         * 存放各种transform node
         */
        private Map<String, TransformNode> transformNodes = new LinkedHashMap<>();

        private Map<String, SinkNode> sinkNodes = new LinkedHashMap<>();

        /**
         * 构建topology的builder
         *
         * @param parameters 通过接口得到的Job信息
         */
        public ModelTopologyBuilder(Map<String, Object> parameters) {
            super(parameters);
            this.timeZone = parameters.get("time_zone").toString();
            this.nodes = (Map<String, Map<String, Object>>) parameters.get("nodes");
            this.resource = (Map<String, String>) parameters.get("resource");
            this.isDebug = Objects.equals(ConstantVar.RunMode.debug.toString(), this.getRunMode());
            this.dataFlowUrl = parameters.getOrDefault("data_flow_url", "").toString();
            if (null != parameters.get("schedule_time")) {
                this.scheduleTime = (long) parameters.get("schedule_time");
                LOGGER.info("schedule time:" + this.scheduleTime);
            }
            if (this.isDebug) {
                Map<String, String> debugInfo = (Map<String, String>) parameters.get("debug");
                this.debugConfig = new OneModelDebugConfig();
                this.debugConfig.setDebugId(debugInfo.get("debug_id").toString());
                this.debugConfig.setDebugUrl(debugInfo.get("debug_rest_api_url"));
                this.debugConfig.setDebugExecId(Long.parseLong(debugInfo.get("debug_exec_id")));
            } else {
                // metric 信息
                this.metricConfig = new OneModelMetricConfig();
                Map<String, String> metricInfo = (Map<String, String>) parameters.get("metric");
                if (metricInfo != null) {
                    this.metricConfig.setMetricUrl(metricInfo.get("metric_rest_api_url"));
                }
            }
        }


        @Override
        public ModelTopology build() {
            return new ModelTopology(this);
        }

        public OneModelDebugConfig getDebugConfig() {
            return this.debugConfig;
        }

        public OneModelMetricConfig getMetricConfig() {
            return this.metricConfig;
        }

        public boolean isDebug() {
            return isDebug;
        }

        public long getScheduleTime() {
            return scheduleTime;
        }

        public String getDataFlowUrl() {
            return dataFlowUrl;
        }

        private void buildNodes() {
            Map<String, SourceNode> sourceNodes = new HashMap<>();
            Map<String, TransformNode> transformNodes = new HashMap<>();
            Map<String, SinkNode> sinkNodes = new HashMap<>();
            AbstractModelBuilderFactory sourceFactory = new ModelSourceBuilderFactory(this.scheduleTime,
                    this.getJobType(),
                    this.isDebug,
                    this.dataFlowUrl);
            AbstractModelBuilderFactory sinkFactory = new ModelSinkBuilderFactory(this.getJobType(), this.scheduleTime,
                    this.nodes.get("source"));
            AbstractModelBuilderFactory transformerFactory = new ModelTransformerBuilderFactory(this.getJobType(),
                    this.scheduleTime, this.nodes.get("source"));

            this.nodes.forEach((nodeType, node) -> {
                node.forEach((id, info) -> {
                    switch (nodeType) {
                        case "source":
                            SourceNode sourceNode = (SourceNode) sourceFactory
                                    .getBuilder((Map<String, Object>) info).build();
                            sourceNodes.put(sourceNode.getNodeId(), sourceNode);
                            break;
                        case "transform":
                            TransformNode transformNode = (TransformNode) transformerFactory
                                    .getBuilder((Map<String, Object>) info).build();
                            if (transformNode != null) {
                                transformNodes.put(transformNode.getNodeId(), transformNode);
                            }

                            break;
                        case "sink":
                            SinkNode sinkNode = (SinkNode) sinkFactory.getBuilder((Map<String, Object>) info).build();
                            sinkNodes.put(sinkNode.getNodeId(), sinkNode);
                            break;
                        default:
                            throw new IllegalArgumentException("node type " + nodeType + " is not support.");
                    }
                });
            });
            this.sourceNodes = sourceNodes;
            this.transformNodes = transformNodes;
            this.sinkNodes = sinkNodes;
            this.userDefinedFunctions = transformerFactory.getUserDefinedFunctions();
        }
    }
}
