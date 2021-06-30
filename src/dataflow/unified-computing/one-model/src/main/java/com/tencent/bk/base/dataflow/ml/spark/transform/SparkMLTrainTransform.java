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

import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode;
import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithm;
import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithmFunction;
import com.tencent.bk.base.dataflow.ml.transform.AbstractMLTransform;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import java.text.MessageFormat;
import java.util.Map;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMLTrainTransform extends AbstractMLTransform {

    public SparkMLTrainTransform(ModelTransformNode node,
            Map<String, Model<?>> models,
            Map<String, DataFrameWrapper> dataSets,
            SparkSession sparkSession) {
        super(node, models, dataSets, sparkSession);
    }

    @Override
    public void createNode() {
        Dataset<Row> interpreterData = this.preparingData();
        AbstractSparkAlgorithm trainAlgorithm = SparkAlgorithmTransformers.getTrainAlgorithm(
                node.getProcessor().getName(), node.getProcessor().getArgs());
        if (!(trainAlgorithm instanceof AbstractSparkAlgorithmFunction)) {
            throw new RuntimeException(
                    MessageFormat.format(
                            "Not support the spark algorithm {0} and type is {1} in node {2}",
                            node.getProcessor().getName(),
                            node.getProcessor().getType(),
                            node.getNodeId()));
        }
        models.put(node.getNodeId(), ((AbstractSparkAlgorithmFunction) trainAlgorithm)
                .train(interpreterData));
    }
}
