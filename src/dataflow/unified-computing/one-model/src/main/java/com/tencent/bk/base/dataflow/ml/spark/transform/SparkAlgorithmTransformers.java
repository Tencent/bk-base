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

import com.tencent.bk.base.dataflow.ml.exceptions.AlgorithmGenerateModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.AlgorithmParamGenerateModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.AlgorithmRunModelException;
import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithm;
import com.tencent.bk.base.dataflow.ml.spark.algorithm.SparkAlgorithmSummary;
import com.tencent.bk.base.dataflow.ml.spark.codegen.Compiler;
import com.tencent.bk.base.dataflow.ml.spark.codegen.GeneratedAlgorithm;
import com.tencent.bk.base.dataflow.ml.spark.codegen.feature.FeatureTrainedAlgorithmCodeGenerator;
import com.tencent.bk.base.dataflow.ml.spark.codegen.feature.FeatureUntrainedTransformCodeGenerator;
import java.util.Map;
import org.apache.spark.ml.Model;

public class SparkAlgorithmTransformers {

    private static FeatureUntrainedTransformCodeGenerator featureUntrainedTransformCodeGenerator =
            new FeatureUntrainedTransformCodeGenerator();
    private static FeatureTrainedAlgorithmCodeGenerator featureTrainedAlgorithmCodeGenerator =
            new FeatureTrainedAlgorithmCodeGenerator();

    /**
     * 获取需要模型应用的算法实例
     *
     * @param algorithmName 算法名称
     * @param model 需要应用的模型
     * @return 算法实例
     */
    public static AbstractSparkAlgorithm getTrainedRunAlgorithm(String algorithmName, Model<?> model,
            Map<String, Object> params) {
        if (SparkAlgorithmSummary.SPARK_ALGORITHMS.containsKey(algorithmName)) {
            return compileTrainedRunAlgorithm(algorithmName, model, params);
        }
        //throw new RuntimeException(MessageFormat.format("The algorithm {0} don't support run.", algorithmName));
        throw new AlgorithmRunModelException(algorithmName, "run");
    }

    /**
     * 获取不需要训练就能直接应用的算法
     *
     * @param algorithmName 算法名称
     * @param params 算法应用需要的参数
     * @return 算法实例
     */
    public static AbstractSparkAlgorithm getUntrainedRunAlgorithm(String algorithmName, Map<String, Object> params) {
        if (SparkAlgorithmSummary.SPARK_UNTRAINED_ALGORITHMS.containsKey(algorithmName)) {
            return compileUntrainedRunAlgorithm(algorithmName, params);
        }
        //throw new RuntimeException(MessageFormat.format("The algorithm {0} don't support run.", algorithmName));
        throw new AlgorithmRunModelException(algorithmName, "run");
    }

    /**
     * 算法训练时，实例化对应算法，并返回
     *
     * @param algorithmName 算法名称
     * @return 算法对象
     */
    public static AbstractSparkAlgorithm getTrainAlgorithm(String algorithmName, Map<String, Object> params) {
        if (SparkAlgorithmSummary.SPARK_ALGORITHMS.containsKey(algorithmName)) {
            return compileTrainAlgorithm(algorithmName, params);
        }
        //throw new RuntimeException(MessageFormat.format("The algorithm {0} don't support train.", algorithmName));
        throw new AlgorithmRunModelException(algorithmName, "train");
    }

    private static AbstractSparkAlgorithm compileUntrainedRunAlgorithm(String algorithmName,
            Map<String, Object> params) {
        GeneratedAlgorithm generatedAlgorithm =
                featureUntrainedTransformCodeGenerator.generateAlgorithm(
                        SparkAlgorithmSummary.SPARK_UNTRAINED_ALGORITHMS.get(algorithmName));
        try {
            return (AbstractSparkAlgorithm) Compiler.compile(generatedAlgorithm.name(), generatedAlgorithm.code())
                    .getConstructor(Map.class)
                    .newInstance(params);
        } catch (Exception e) {
            /*throw new RuntimeException(MessageFormat.format("Failed to generate algorithm {0} for params {1}",
                algorithmName, params.toString()), e);*/
            throw new AlgorithmParamGenerateModelException(algorithmName, params.toString(), e);
        }
    }

    private static AbstractSparkAlgorithm compileTrainAlgorithm(String algorithmName, Map<String, Object> params) {
        GeneratedAlgorithm generatedAlgorithm =
                featureTrainedAlgorithmCodeGenerator
                        .generateAlgorithm(SparkAlgorithmSummary.SPARK_ALGORITHMS
                                .get(algorithmName).getAlgorithmClass());
        try {
            return (AbstractSparkAlgorithm) Compiler.compile(generatedAlgorithm.name(), generatedAlgorithm.code())
                    .getConstructor(Map.class)
                    .newInstance(params);
        } catch (Exception e) {
            /*throw new RuntimeException(MessageFormat.format("Failed to generate algorithm {0} for params {1}",
                algorithmName, params.toString()));*/
            throw new AlgorithmParamGenerateModelException(algorithmName, params.toString(), e);
        }
    }

    private static AbstractSparkAlgorithm compileTrainedRunAlgorithm(String algorithmName, Model<?> model,
            Map<String, Object> params) {
        GeneratedAlgorithm generatedAlgorithm =
                featureTrainedAlgorithmCodeGenerator
                        .generateAlgorithm(SparkAlgorithmSummary.SPARK_ALGORITHMS
                                .get(algorithmName).getAlgorithmClass());
        try {
            return (AbstractSparkAlgorithm) Compiler.compile(generatedAlgorithm.name(), generatedAlgorithm.code())
                    .getConstructor(Model.class, Map.class)
                    .newInstance(model, params);
        } catch (Exception e) {
            /*throw new RuntimeException(MessageFormat.format("Failed to generate algorithm {0}",
                algorithmName), e);*/
            throw new AlgorithmGenerateModelException(algorithmName, e);
        }
    }
}
