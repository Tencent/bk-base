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

package com.tencent.bk.base.dataflow.ml.spark.codegen.feature;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkUntrainedAlgorithmFunction;
import com.tencent.bk.base.dataflow.ml.spark.codegen.Compiler;
import com.tencent.bk.base.dataflow.ml.spark.codegen.GeneratedAlgorithm;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

public class TestTokenizer {

    /**
     * 测试Tokenzier算法
     */
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaTokenizerExample")
                .master("local")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Map<String, String> maps = new HashMap<String, String>() {
            {
                put("inputCol", "sentence");
                put("outputCol", "words");
            }
        };

        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

        FeatureUntrainedTransformCodeGenerator codeGenerator = new FeatureUntrainedTransformCodeGenerator();
        GeneratedAlgorithm tokenizer = codeGenerator.generateAlgorithm("org.apache.spark.ml.feature.Tokenizer");

        Class<?> compile = Compiler.compile(tokenizer.name(), tokenizer.code());
        AbstractSparkUntrainedAlgorithmFunction
                sparkAlgorithm = (AbstractSparkUntrainedAlgorithmFunction) compile.getConstructor(Map.class)
                .newInstance(maps);

        spark.udf().register(
                "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);
        Dataset<Row> run = sparkAlgorithm.run(sentenceDataFrame);
        run.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);

        Map<String, String> regexMaps = new HashMap<String, String>() {
            {
                put("inputCol", "sentence");
                put("outputCol", "words");
                put("pattern", "\\W");
            }
        };

        FeatureUntrainedTransformCodeGenerator codeGenerator1 = new FeatureUntrainedTransformCodeGenerator();
        GeneratedAlgorithm tokenize1 = codeGenerator1.generateAlgorithm("org.apache.spark.ml.feature.RegexTokenizer");

        Class<?> compile1 = Compiler.compile(tokenize1.name(), tokenize1.code());
        AbstractSparkUntrainedAlgorithmFunction
                sparkAlgorithm1 = (AbstractSparkUntrainedAlgorithmFunction) compile1.getConstructor(Map.class)
                .newInstance(regexMaps);

        Dataset<Row> regexTokenized = sparkAlgorithm1.run(sentenceDataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);
    }
}
