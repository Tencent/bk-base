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

package com.tencent.bk.base.dataflow.ml.spark.source;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchHDFSSourceOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchIcebergSourceOperator;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSourceNode;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode;
import java.io.Serializable;
import java.util.Map;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSourceStage implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(MLSourceStage.class);

    private Map<String, Model<?>> models;
    private Map<String, DataFrameWrapper> dataSets;
    private SparkSession env;

    public MLSourceStage(
            Map<String, Model<?>> models,
            Map<String, DataFrameWrapper> dataSets,
            SparkSession env) {
        this.models = models;
        this.dataSets = dataSets;
        this.env = env;
    }

    /**
     * 创建source节点
     */
    public AbstractSource<?> createSource(SourceNode node) {
        if (node instanceof ModelDefaultSourceNode) {
            //hdfs上的parquet(表或是模型)
            switch (ConstantVar.DataNodeType.valueOf(node.getType())) {
                case model:
                    return new MLDefaultSource((ModelDefaultSourceNode) node, this.models);
                case data:
                    return new MLQuerySetSource((ModelDefaultSourceNode) node, env, this.dataSets);
                default:
                    throw new RuntimeException("In source stage, not support the node type " + node.getType());
            }
        } else if (node instanceof ModelIceBergQuerySetSourceNode) {
            //HDFS上的iceberg queryset
            return new MLIcebergQuerySetSource((ModelIceBergQuerySetSourceNode) node, this.env, this.dataSets);
        } else if (node instanceof BatchHDFSSourceNode) {
            //HDFS上的parquet表
            return new BatchHDFSSourceOperator((BatchHDFSSourceNode) node, this.env, this.dataSets);
        } else if (node instanceof BatchIcebergSourceNode) {
            //HDFS上的iceberg表
            return new BatchIcebergSourceOperator((BatchIcebergSourceNode) node, this.env, this.dataSets);
        } else {
            throw new RuntimeException("In source stage, not support the node type " + node.getType());
        }
    }

    /**
     * 根据source node，构建出对应的source，分为加载模型和加载数据
     * 将加载数据存放在dataSets中，将模型存放在models中
     *
     * @param node {@link SourceNode} source node
     */
    public void source(SourceNode node) {
        LOGGER.info("Model source stage: " + node.getNodeId());
        AbstractSource<?> source = createSource(node);
        source.createNode();
    }
}
