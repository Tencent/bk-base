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

package com.tencent.bk.base.dataflow.ml.spark.transform;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.SparkMLlibRunMode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.spark.pipeline.transform.BatchSQLTransformOperator;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.io.Serializable;
import java.util.Map;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLTransformStage implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MLTransformStage.class);
    private Map<String, Model<?>> models;
    private Map<String, DataFrameWrapper> dataSets;
    private SparkSession sparkSession;

    public MLTransformStage(Map<String, Model<?>> models,
            Map<String, DataFrameWrapper> dataSets,
            SparkSession sparkSession) {
        this.models = models;
        this.dataSets = dataSets;
        this.sparkSession = sparkSession;
    }

    /**
     * 创建transform节点
     */
    public AbstractTransform createTransform(TransformNode node) {
        if (node instanceof ModelTransformNode) {
            String runType = ((ModelTransformNode) node).getProcessor().getType();
            SparkMLlibRunMode runMode = SparkMLlibRunMode.parse(runType);
            ModelTransformNode modelTransformNode = (ModelTransformNode) node;
            switch (runMode) {
                case train:
                    return new SparkMLTrainTransform(modelTransformNode, this.models, this.dataSets, this.sparkSession);
                case trained_run:
                    return new SparkMLTrainedRunTransform(modelTransformNode, this.models, this.dataSets,
                            this.sparkSession);
                case untrained_run:
                    return new SparkMLUntrainedRunTransform(modelTransformNode, this.models, this.dataSets,
                            this.sparkSession);
                default:
                    throw new RuntimeException("Unsupported run type:" + runType);
            }
        } else if (node instanceof BatchSQLTransformNode) {
            return new BatchSQLTransformOperator((BatchSQLTransformNode) node, this.sparkSession, this.dataSets,
                    this.dataSets);
        } else {
            throw new RuntimeException("In transform stage, not support the node type " + node.getType());
        }
    }

    public void transform(TransformNode node) {
        LOGGER.info("Model transform stage: " + node.getNodeId());
        AbstractTransform transform = createTransform(node);
        transform.createNode();
    }
}
