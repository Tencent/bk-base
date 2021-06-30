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

package com.tencent.bk.base.dataflow.udf.tests.spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class HiveUdfSuiteSparkTest {

    private static List<String> jsonDs = Arrays.asList(
            "{\"a\":\"this is a simple string.\", \"b\":10, \"c\":21474836470, "
                    + "\"d\":92233720368547758070, \"e\":1.7976931348623157E308, "
                    + "\"f\":true, \"g\":3}",
            "{\"a\":\"this is another simple string.\", \"b\":11, \"c\":21474836469, "
                    + "\"d\":92233720368547758069, \"e\":1.7976931348623157E305, "
                    + "\"f\":false, \"g\":5}");


    @Test
    public void testSparkUdf() throws Exception {
        String sql = "select test_udf(a,a) as cc from table";

        sql(sql)
                .sourceData(jsonDs)
                .tableName("name")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdf'")
                .taskContains(
                        Arrays.asList(RowFactory.create("test___this is a simple string.___this is a simple string."),
                                RowFactory
                                        .create("test___this is another "
                                                + "simple string.___this is another simple string.")))
                .run();
    }

    @Test
    public void testPySuiteSparkUdf() throws Exception {
        String sql = "select test_udf(a,a) as cc from table";
        sql(sql)
                .sourceData(jsonDs)
                .tableName("name")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdf'")
                .taskContains(
                        Arrays.asList(RowFactory.create("test___this is a simple string.___this is a simple string."),
                                RowFactory
                                        .create("test___this is another "
                                                + "simple string.___this is another simple string.")))
                .run();
    }

    @Test
    public void testJavaSuiteSparkUdtf() throws Exception {
        String sql = "select test_udf(a,' ', 10) as (cc, dd) from table";
        sql(sql)
                .sourceData(jsonDs)
                .tableName("table")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdtf'")
                .taskContains(Arrays.asList(RowFactory.create("this,4"),
                        RowFactory.create("is,2"),
                        RowFactory.create("a,1"),
                        RowFactory.create("simple,6"),
                        RowFactory.create("string.,7"),
                        RowFactory.create("this,4"),
                        RowFactory.create("is,2"),
                        RowFactory.create("another,7"),
                        RowFactory.create("simple,6"),
                        RowFactory.create("string.,7")))
                .run();
    }

    @Test
    public void testPyUdtfSuiteSpark() throws Exception {
        String sql = "select test_udf(a,' ') as (cc, dd) from table";
        sql(sql)
                .sourceData(jsonDs)
                .tableName("table")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdtf'")
                .taskContains(Arrays.asList(RowFactory.create("this,4"),
                        RowFactory.create("is,2"),
                        RowFactory.create("a,1"),
                        RowFactory.create("simple,6"),
                        RowFactory.create("string.,7"),
                        RowFactory.create("this,4"),
                        RowFactory.create("is,2"),
                        RowFactory.create("another,7"),
                        RowFactory.create("simple,6"),
                        RowFactory.create("string.,7")))
                .run();
    }

    @Test
    public void testPyUdafSuiteSpark() throws Exception {
        String sql = "select test_udaf(b, g) as  cc from table";
        sql(sql)
                .sourceData(jsonDs)
                .tableName("table")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udaf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdaf'")
                .taskContains(Arrays.asList(RowFactory.create("10")))
                .run();
    }

    @Test
    public void testJavaUdafSuiteSpark() throws Exception {
        String sql = "select test_udaf(b, g) as  cc from table";
        sql(sql)
                .sourceData(jsonDs)
                .tableName("table")
                .registerUdf(
                        "CREATE TEMPORARY FUNCTION test_udaf "
                                + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdaf'")
                .taskContains(Arrays.asList(RowFactory.create("10.625")))
                .run();
    }

    private static void checkAnswer(Object actual, Object expected) {
        Assert.assertEquals(actual, expected);
    }

    public Tester sql(String sql) {
        return new Tester(sql);
    }

    private static class Tester {

        private String sql;
        private String registerUdf;
        private List<Row> expected;
        private String tableName;
        private Dataset<String> sourceData;
        private SparkSession spark;

        private Tester(String sql) {
            this.sql = sql;
            this.spark = SparkSession.builder()
                    .enableHiveSupport()
                    .master("local[*]")
                    .appName("testing")
                    .getOrCreate();
        }

        private Tester registerUdf(String registerUdf) {
            this.registerUdf = registerUdf;
            return this;
        }

        private Tester tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        private Tester taskContains(List<Row> expected) {
            this.expected = expected;
            return this;
        }

        private Tester sourceData(List<String> sourceData) {
            this.sourceData = spark.createDataset(sourceData, Encoders.STRING());
            return this;
        }

        private void run() throws Exception {
            Dataset<Row> df1 = spark.read().json(this.sourceData);
            df1.createOrReplaceTempView(this.tableName);
            spark.sql(this.registerUdf);
            List<Row> actual = spark.sql(this.sql).collectAsList();
            checkAnswer(actual.toString(), expected.toString());
            spark.stop();
        }
    }
}
