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

import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithmFunction;
import com.tencent.bk.base.dataflow.ml.spark.codegen.Compiler;
import com.tencent.bk.base.dataflow.ml.spark.codegen.GeneratedAlgorithm;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestPCA {

    /**
     * 测试PCA算法的数据处理
     */
    public static void main(String[] args)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPCAExample")
                .master("local")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
                RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
                RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> maps = new HashMap<String, String>() {
            {
                put("inputCol", "features");
                put("outputCol", "pcaFeatures");
                put("k", "3");
            }
        };

        FeatureTrainedAlgorithmCodeGenerator codeGenerator = new FeatureTrainedAlgorithmCodeGenerator();
        GeneratedAlgorithm pca2 = codeGenerator.generateAlgorithm("org.apache.spark.ml.feature.PCA");

        Class<?> compile1 = Compiler.compile(pca2.name(), pca2.code());
        AbstractSparkAlgorithmFunction
                sparkAlgorithm1 = (AbstractSparkAlgorithmFunction) compile1.getConstructor(Map.class).newInstance(maps);

        MLWritable train1 = (MLWritable) sparkAlgorithm1.train(df);

        train1.write().overwrite().save("/tmp/pca");

//        org.apache.spark.ml.feature.PCAModel load = org.apache.spark.ml.feature.PCAModel.read().load("/tmp/pca");
//        Object pac = SPARK_MODEL_READERS.get("pca").load("/tmp/pca");
//        SparkAlgorithmFunction sparkAlgorithm2
//            = (SparkAlgorithmFunction) compile1.getConstructor(Model.class).newInstance(pac);
//
//        Dataset<Row> result = sparkAlgorithm2.run(df).select("pcaFeatures");
//        result.show(false);
    }
}
