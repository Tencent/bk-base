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

import com.tencent.bk.base.dataflow.ml.exceptions.AlgorithmRunModelException;
import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithm;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class SparkAlgorithmTransformersTest {

    @Test
    @PrepareForTest({DecisionTreeRegressionModel.class, AbstractSparkAlgorithm.class})
    public void testGetTrainedRunAlgorithm() {
        //获取预测算法信息
        DecisionTreeRegressionModel model = PowerMockito.mock(DecisionTreeRegressionModel.class);
        PowerMockito.when(model.params()).thenReturn(new Param[0]);
        Map<String, Object> params = new HashMap<>();
        String algorithmName = "decision_tree_classifier";
        AbstractSparkAlgorithm algorithm = SparkAlgorithmTransformers
                .getTrainedRunAlgorithm(algorithmName, model, params);
        System.out.println("algorithm:" + algorithm.getClass().getName());
        assert algorithm.getClass().getName()
                .equalsIgnoreCase("SparkDecisionTreeClassifier");
        algorithmName = "decision_tree_classifier_1";
        boolean exception = false;
        try {
            SparkAlgorithmTransformers
                    .getTrainedRunAlgorithm(algorithmName, model, params);
        } catch (AlgorithmRunModelException e) {
            exception = true;
        }
        assert exception;
    }

    @Test
    public void testGetUntrainedRunAlgorithm() {
        //获取特征转换算法信息
        Map<String, Object> params = new HashMap<>();
        String algorithmName = "tokenizer";
        AbstractSparkAlgorithm algorithm = SparkAlgorithmTransformers
                .getUntrainedRunAlgorithm(algorithmName, params);
        assert algorithm.getClass().getName()
                .equalsIgnoreCase("SparkTokenizer");
        algorithmName = "tokenizer_1";
        boolean exception = false;
        try {
            SparkAlgorithmTransformers
                    .getUntrainedRunAlgorithm(algorithmName, params);
        } catch (AlgorithmRunModelException e) {
            exception = true;
        }
        assert exception;
    }

    @Test
    public void testGetTrainAlgorithm() {
        //获取训练算法信息
        //获取预测算法信息
        Map<String, Object> params = new HashMap<>();
        String algorithmName = "logistic_regression";
        AbstractSparkAlgorithm algorithm = SparkAlgorithmTransformers
                .getTrainAlgorithm(algorithmName, params);
        System.out.println("algorithm:" + algorithm.getClass().getName());
        assert algorithm.getClass().getName()
                .equalsIgnoreCase("SparkLogisticRegression");
        algorithmName = "logistic_regression_1";
        boolean exception = false;
        try {
            SparkAlgorithmTransformers
                    .getTrainAlgorithm(algorithmName, params);
        } catch (AlgorithmRunModelException e) {
            exception = true;
        }
        assert exception;
    }
}
