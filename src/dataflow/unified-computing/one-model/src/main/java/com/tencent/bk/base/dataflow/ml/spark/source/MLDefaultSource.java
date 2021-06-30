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

import static com.tencent.bk.base.dataflow.ml.spark.algorithm.SparkAlgorithmSummary.SPARK_ALGORITHMS;

import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.HDFSInput;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSourceNode;
import java.text.MessageFormat;
import java.util.Map;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLDefaultSource extends AbstractSource<Model<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MLDefaultSource.class);

    private ModelDefaultSourceNode node;
    private Map<String, Model<?>> models;

    public MLDefaultSource(ModelDefaultSourceNode node, Map<String, Model<?>> models) {
        this.node = node;
        this.models = models;
    }

    @Override
    public Model<?> createNode() {
        String modelName = ((HDFSInput) this.node.getInput()).getFormat();
        String path = this.node.getInput().getInputInfo();
        LOGGER.info(MessageFormat.format("Get model {0} from {1}", modelName, path));
        if ("lda".equalsIgnoreCase(modelName)) {
            try {
                Model model = org.apache.spark.ml.clustering.LocalLDAModel.load(path);
                this.models.put(node.getNodeId(), model);
                return model;
            } catch (Exception e) {
                Model model = org.apache.spark.ml.clustering.DistributedLDAModel.load(path);
                this.models.put(node.getNodeId(), model);
                return model;
            }
        }

        for (MLReader<?> mlReader : SPARK_ALGORITHMS.get(modelName).getAlgorithmReader()) {
            try {
                Model model = (Model<?>) mlReader.load(path);
                this.models.put(node.getNodeId(), model);
                return model;
            } catch (IllegalArgumentException e1) {
                LOGGER.warn("Failed to read model " + modelName + " for " + this.node.getNodeId());
            }
        }
        throw new RuntimeException("Failed to source model node " + this.node.getNodeId());
    }
}
