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

package com.tencent.bk.base.dataflow.ml.spark.sink;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import com.tencent.bk.base.dataflow.ml.spark.util.HadoopUtil;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MLDataSinkTest {

    /**
     * 测试主函数
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPCAExample")
                .master("local")
                .getOrCreate();

        Configuration entries = spark.sparkContext().hadoopConfiguration();
        entries.addResource(new Path("core-site.xml"));
        entries.addResource(new Path("hdfs-site.xml"));
        HadoopUtil.setConf(entries);

        String path = "hdfs://xxxx/test/spark-mllib/source/591_dev_test0513";
        List<Row> data = Arrays.asList(
                RowFactory.create(1589339640000L,
                        "2020-05-13 11:14:00",
                        "2020-05-13 11:14:10",
                        1, "I wish Java could use case classes")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("dtEventTimeStamp", LongType, false, Metadata.empty()),
                new StructField("dtEventTime", StringType, false, Metadata.empty()),
                new StructField("localTime", StringType, false, Metadata.empty()),
                new StructField("id", IntegerType, false, Metadata.empty()),
                new StructField("sentence", StringType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);
        dataFrame.write().format("parquet").mode("overwrite").save(path);
    }
}
