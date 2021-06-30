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

package com.tencent.bk.base.dataflow.ml.spark.sink;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.sink.AbstractSink;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.ml.metric.AbstractNodeMetric;
import com.tencent.bk.base.dataflow.ml.metric.DebugConstant;
import com.tencent.bk.base.dataflow.ml.metric.DebugResultData;
import com.tencent.bk.base.dataflow.ml.metric.ModelMetricFactory;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSinkNode;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSinkNode;
import com.tencent.bk.base.dataflow.ml.util.ModelNodeUtil;
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchHDFSSinkOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchIcebergSinkOperator;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSinkStage implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MLSinkStage.class);
    private static ModelMetricFactory metricFactory = new ModelMetricFactory();
    private Map<String, Model<?>> models;
    private Map<String, DataFrameWrapper> dataSets;
    private SparkSession env;

    public MLSinkStage(
            Map<String, Model<?>> models,
            Map<String, DataFrameWrapper> dataSets,
            SparkSession env) {
        this.models = models;
        this.dataSets = dataSets;
        this.env = env;
    }

    /**
     * 创建sink节点
     */
    public AbstractSink createSink(SinkNode node,
            @Nullable Model<?> model, @Nullable DataFrameWrapper dataSet) {
        if (node instanceof ModelDefaultSinkNode) {
            switch (ConstantVar.DataNodeType.valueOf(node.getType())) {
                case model:
                    return new MLDefaultSink((ModelDefaultSinkNode) node, model);
                case data:
                    return new MLQuerySetSink((ModelDefaultSinkNode) node, dataSet.getDataFrame());
                default:
                    throw new RuntimeException("In sink stage, not support the node type " + node.getType());
            }
        } else if (node instanceof ModelIceBergQuerySetSinkNode) {
            return new MLIcebergQuerySetSink((ModelIceBergQuerySetSinkNode) node, dataSet);

        } else if (node instanceof BatchHDFSSinkNode) {
            return new BatchHDFSSinkOperator((BatchHDFSSinkNode) node, this.env,
                    new HashMap<String, DataFrameWrapper>(),
                    this.dataSets,
                    this.dataSets);
        } else if (node instanceof BatchIcebergSinkNode) {
            return new BatchIcebergSinkOperator((BatchIcebergSinkNode) node, this.env,
                    new HashMap<String, DataFrameWrapper>(),
                    this.dataSets,
                    this.dataSets);
        } else {
            throw new RuntimeException("In sink stage, not support the node type " + node.getType());
        }
    }

    /**
     * 执行模型或数据落地操作，同时上报统计信息
     *
     * @param node 已构建完成的sink节点
     */
    public void sink(SinkNode node) {
        LOGGER.info("Model sink stage: " + node.getNodeId());
        LOGGER.info("Run Mode: " + DebugConstant.getRunMode());
        if (ConstantVar.RunMode.debug.toString().equalsIgnoreCase(DebugConstant.getRunMode())) {
            if (!"model".equalsIgnoreCase(node.getType())) {
                DebugResultData result = new DebugResultData(node, DebugConstant.getTopology());
                dataSets.get(node.getNodeId()).getDataFrame().show();
                result.reportResult(dataSets.get(node.getNodeId()).getDataFrame());
            }
        } else {
            createSink(node, models.get(node.getNodeId()), dataSets.get(node.getNodeId())).createNode();
        }
        // sink是没有parents的，要根据名称，找到对应的transform，这个transform的输入是本次sink的input
        AbstractNodeMetric metric = metricFactory.getMetric(DebugConstant.getTopology());
        TransformNode parentNode = DebugConstant.getTopology().getTransformNodes().get(node.getNodeId());
        metric.setMetricInfo(parentNode,
                dataSets.get(ModelNodeUtil.getSingleParentDataNode(parentNode).getNodeId()).getDataFrame(),
                dataSets.get(node.getNodeId()) == null ? null : dataSets.get(node.getNodeId()).getDataFrame());
        metric.save();
    }
}
