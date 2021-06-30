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

package com.tencent.bk.base.dataflow.ml.spark.utils;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataUtil {


    public static Dataset<Row> getOutputDataset(SparkSession spark) {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0),
                RowFactory.create(1, 1),
                RowFactory.create(2, 2),
                RowFactory.create(3, 3),
                RowFactory.create(4, 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
        });

        return spark.createDataFrame(data, schema);
    }

    public static Dataset<Row> getInputDataset(SparkSession spark) {
        List<Row> data1 = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema1 = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        return spark.createDataFrame(data1, schema1);
    }

    public static Dataset<Row> getEmptyDataset(SparkSession spark) {
        List<Row> data1 = Arrays.asList();

        StructType schema1 = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        return spark.createDataFrame(data1, schema1);
    }
}
