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

package com.tencent.bk.base.dataflow.ml.spark.algorithm;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.ml.feature.PCAModel;

public class SparkAlgorithmSummary {

    /**
     * spark 只有应用，没有训练的算法，key 是 MLSQL 规范名称，value 为对应的 spark 中算法的名称
     */
    public static final Map<String, String> SPARK_UNTRAINED_ALGORITHMS =
            new HashMap<String, String>() {
                {
                    put("tokenizer", "org.apache.spark.ml.feature.Tokenizer");
                    put("regex_tokenizer", "org.apache.spark.ml.feature.RegexTokenizer");
                    put("stop_words_remover", "org.apache.spark.ml.feature.StopWordsRemover");
                    put("n_gram", "org.apache.spark.ml.feature.NGram");
                    put("binarizer", "org.apache.spark.ml.feature.Binarizer");
                    put("polynomial_expansion", "org.apache.spark.ml.feature.PolynomialExpansion");
                    put("dct", "org.apache.spark.ml.feature.DCT");
                    put("index_to_string", "org.apache.spark.ml.feature.IndexToString");
                    put("vector_assembler", "org.apache.spark.ml.feature.VectorAssembler");
                    put("interaction", "org.apache.spark.ml.feature.Interaction");
                    put("normalizer", "org.apache.spark.ml.feature.Normalizer");
                    put("bucketizer", "org.apache.spark.ml.feature.Bucketizer");
                    put("vector_size_hint", "org.apache.spark.ml.feature.VectorSizeHint");
                    put("elementwise_product", "org.apache.spark.ml.feature.ElementwiseProduct");
                }
            };

    /**
     * spark 既有训练，又有应用的算法，key 为 MLSQL 规范名称，value 为对应的 spark 中算法的名称
     */
    public static final Map<String, SparkMLlibTrainedAlgorithm> SPARK_ALGORITHMS =
            new HashMap<String, SparkMLlibTrainedAlgorithm>() {
                {
                    // feature transformers
                    put("pca", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.PCA",
                            Collections.singletonList(PCAModel.read())));
                    put("string_indexer", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.StringIndexer",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.StringIndexerModel.read())));
                    put("one_hot_encoder_estimator", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.OneHotEncoderEstimator",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.OneHotEncoderModel.read())));
                    put("vector_indexer", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.VectorIndexer",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.VectorIndexerModel.read())));
                    put("standard_scaler", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.StandardScaler",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.StandardScalerModel.read())));
                    put("min_max_scaler", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.MinMaxScaler",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.MinMaxScalerModel.read())));
                    put("max_abs_scaler", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.MaxAbsScaler",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.MaxAbsScalerModel.read())));
                    put("quantile_discretizer", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.QuantileDiscretizer",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.Bucketizer.read())));
                    put("imputer", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.feature.Imputer",
                            Collections.singletonList(
                                    org.apache.spark.ml.feature.ImputerModel.read())));

                    // classification and regression
                    put("decision_tree_classifier", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.DecisionTreeClassifier",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.DecisionTreeClassificationModel.read())));
                    put("logistic_regression", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.LogisticRegression",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.LogisticRegressionModel.read())));
                    put("random_forest_classifier", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.RandomForestClassifier",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.RandomForestClassificationModel.read())));
                    put("gbt_classifier", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.GBTClassifier",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.GBTClassificationModel.read())));
                    put("multilayer_perceptron_classifier", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.MultilayerPerceptronClassifier",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
                                            .read())));
                    put("linear_svc", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.LinearSVC",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.LinearSVCModel.read())));
                    put("naive_bayes", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.classification.NaiveBayes",
                            Collections.singletonList(
                                    org.apache.spark.ml.classification.NaiveBayesModel.read())));
                    put("linear_regression", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.LinearRegression",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.LinearRegressionModel.read())));
                    put("generalized_linear_regression", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.GeneralizedLinearRegression",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.GeneralizedLinearRegressionModel.read())));
                    put("decision_tree_regressor", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.DecisionTreeRegressor",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.DecisionTreeRegressionModel.read())));
                    put("random_forest_regressor", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.RandomForestRegressor",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.RandomForestRegressionModel.read())));
                    put("gbt_regressor", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.GBTRegressor",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.GBTRegressionModel.read())));
                    put("aft_survival_regression", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.AFTSurvivalRegression",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.AFTSurvivalRegressionModel.read())));
                    put("isotonic_regression", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.regression.IsotonicRegression",
                            Collections.singletonList(
                                    org.apache.spark.ml.regression.IsotonicRegressionModel.read())));

                    // clustering
                    put("k_means", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.clustering.KMeans",
                            Collections.singletonList(
                                    org.apache.spark.ml.clustering.KMeansModel.read())));
                    put("lda", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.clustering.LDA",
                            Arrays.asList(
                                    org.apache.spark.ml.clustering.LocalLDAModel.read(),
                                    org.apache.spark.ml.clustering.DistributedLDAModel.read())));
                    put("bisecting_k_means", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.clustering.BisectingKMeans",
                            Collections.singletonList(
                                    org.apache.spark.ml.clustering.BisectingKMeansModel.read())));
                    put("gaussian_mixture", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.clustering.GaussianMixture",
                            Collections.singletonList(
                                    org.apache.spark.ml.clustering.GaussianMixtureModel.read())));

                    // collaborative filtering
                    put("als", new SparkMLlibTrainedAlgorithm(
                            "org.apache.spark.ml.recommendation.ALS",
                            Collections.singletonList(
                                    org.apache.spark.ml.recommendation.ALSModel.read())));
                }
            };
}
