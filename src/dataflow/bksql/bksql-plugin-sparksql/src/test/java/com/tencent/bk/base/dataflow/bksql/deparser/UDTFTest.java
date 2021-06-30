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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.tencent.bk.base.dataflow.bksql.udtf.SparkHiveUDTFExample;
import org.apache.spark.sql.BkSparkSqlAnalyzer;
import org.apache.spark.sql.UdtfCodeGen;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.catalog.CatalogFunction;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.SimpleCompiler;
import org.codehaus.janino.util.ClassFile;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Map$;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// CI环境获取主机信息异常
public class UDTFTest {

  @Test
  public void testUDTF() {

    String sql = "select udtf_1(words) as (a1, a2) from test_in";
    CatalogTable testIn = BkSparkSqlAnalyzer.createTableDesc("test_in",
            new StructType(new StructField[] {
                    new StructField("words",
                    DataTypes.createArrayType(DataTypes.StringType), false,
                    Metadata.empty())
            }));
    List<CatalogTable> ts = new ArrayList<>();
    ts.add(testIn);

    Seq<CatalogTable> tables = JavaConverters.asScalaIteratorConverter(ts.iterator()).asScala().toSeq();
    // 注册函数(Hive)
    Seq<CatalogFunction> hiveUdfFunctions
            = BkSparkSqlAnalyzer.testHiveFunctions("udtf_1", SparkHiveUDTFExample.class.getName());
    // 注册Spark ScalaUDF函数
    FunctionRegistry functionRegistry = BkSparkSqlAnalyzer.getFunctionRegistry(Map$.MODULE$.empty());
    LogicalPlan logicalPlan = BkSparkSqlAnalyzer.parserSQL(sql,
            tables,
            functionRegistry,
            hiveUdfFunctions);

    Iterable<SparkSqlTableEntity.Field> fields = JavaConverters.asJavaIterableConverter(
            BkSparkSqlAnalyzer.getOutFields(logicalPlan)).asJava();
    for (SparkSqlTableEntity.Field field : fields) {
      System.out.println(field.getIndex() + ", " + field.getType() + ", "  + field.getField());
    }
  }


  @Test
  public void testUDTF2() throws Exception {

    CatalogTable testIn = BkSparkSqlAnalyzer.createTableDesc("test_in",
            new StructType(new StructField[] {
                    new StructField("words",
                            DataTypes.createArrayType(DataTypes.StringType), false,
                            Metadata.empty())
            }));


    List<CatalogTable> ts = new ArrayList<>();
    ts.add(testIn);
    Map<String, byte[]> udtfClass = new HashMap<>();

    ClassFile[] classFiles = UdtfCodeGen.genCode("Udtf_udtf_1",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    classFiles = UdtfCodeGen.genCode("Udtf_udtf_2",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    SimpleCompiler compiler = new SimpleCompiler();
    compiler.cook(udtfClass);

    Class.forName("Udtf_udtf_2", true, compiler.getClassLoader());
    Class.forName("Udtf_udtf_1", true, compiler.getClassLoader());
    Thread.currentThread().setContextClassLoader(compiler.getClassLoader());
    Seq<CatalogTable> tables = JavaConverters.asScalaIteratorConverter(ts.iterator()).asScala().toSeq();
    // 注册函数(Hive)
    Seq<CatalogFunction> hiveUdfFunctions = BkSparkSqlAnalyzer.testHiveFunctions("udtf_1", "Udtf_udtf_1");
    // 注册Spark ScalaUDF函数
    FunctionRegistry functionRegistry = BkSparkSqlAnalyzer.getFunctionRegistry(Map$.MODULE$.empty());

    String sql = "select udtf_1(words) as (a1, a2, a3) from test_in";
    LogicalPlan logicalPlan = BkSparkSqlAnalyzer.parserSQL(sql,
            tables,
            functionRegistry,
            hiveUdfFunctions);

    Iterable<SparkSqlTableEntity.Field> fields = JavaConverters.asJavaIterableConverter(
            BkSparkSqlAnalyzer.getOutFields(logicalPlan)).asJava();
    for (SparkSqlTableEntity.Field field : fields) {
      System.out.println(field.getIndex() + ", " + field.getType() + ", "  + field.getField());
    }
  }

  @Test
  public void testUDTF3() throws Exception {

    CatalogTable testIn = BkSparkSqlAnalyzer.createTableDesc("test_in",
            new StructType(new StructField[] {
                    new StructField("words",
                            DataTypes.createArrayType(DataTypes.StringType), false,
                            Metadata.empty())
            }));


    List<CatalogTable> ts = new ArrayList<>();
    ts.add(testIn);

    Map<String, byte[]> udtfClass = new HashMap<>();

    ClassFile[] classFiles = UdtfCodeGen.genCode("Udtf_udtf_1",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    classFiles = UdtfCodeGen.genCode("Udtf_udtf_2",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    SimpleCompiler compiler = new SimpleCompiler();
    compiler.cook(udtfClass);

    Class.forName("Udtf_udtf_2", true, compiler.getClassLoader());
    Class.forName("Udtf_udtf_1", true, compiler.getClassLoader());
    Thread.currentThread().setContextClassLoader(compiler.getClassLoader());
    Seq<CatalogTable> tables = JavaConverters.asScalaIteratorConverter(ts.iterator()).asScala().toSeq();
    // 注册函数(Hive)
    Seq<CatalogFunction> hiveUdfFunctions = BkSparkSqlAnalyzer.testHiveFunctions("udtf_1", "Udtf_udtf_1");
    // 注册Spark ScalaUDF函数
    FunctionRegistry functionRegistry = BkSparkSqlAnalyzer.getFunctionRegistry(Map$.MODULE$.empty());
    String sql = "select a1,a2, a3 from test_in LATERAL view udtf_1(words) t1 as a1, a2, a3";
    LogicalPlan logicalPlan = BkSparkSqlAnalyzer.parserSQL(sql,
            tables,
            functionRegistry,
            hiveUdfFunctions);

    Iterable<SparkSqlTableEntity.Field> fields = JavaConverters.asJavaIterableConverter(
            BkSparkSqlAnalyzer.getOutFields(logicalPlan)).asJava();
    for (SparkSqlTableEntity.Field field : fields) {
      System.out.println(field.getIndex() + ", " + field.getType() + ", "  + field.getField());
    }
  }

  @Test
  public void testUDTF4() throws Exception {

    CatalogTable testIn = BkSparkSqlAnalyzer.createTableDesc("test_in",
            new StructType(new StructField[] {
                    new StructField("words",
                            DataTypes.createArrayType(DataTypes.StringType), false,
                            Metadata.empty())
            }));


    List<CatalogTable> ts = new ArrayList<>();
    ts.add(testIn);

    Map<String, byte[]> udtfClass = new HashMap<>();

    ClassFile[] classFiles = UdtfCodeGen.genCode("Udtf_udtf_1",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    classFiles = UdtfCodeGen.genCode("Udtf_udtf_2",
            new String[]{"int", "long", "string"});
    for (ClassFile cf : classFiles) {
      udtfClass.put(cf.getThisClassName(), cf.toByteArray());
    }
    SimpleCompiler compiler = new SimpleCompiler();
    compiler.cook(udtfClass);

    Class.forName("Udtf_udtf_2", true, compiler.getClassLoader());
    Class.forName("Udtf_udtf_1", true, compiler.getClassLoader());
    Thread.currentThread().setContextClassLoader(compiler.getClassLoader());
    Seq<CatalogTable> tables = JavaConverters.asScalaIteratorConverter(ts.iterator()).asScala().toSeq();
    // 注册函数(Hive)
    Seq<CatalogFunction> hiveUdfFunctions = BkSparkSqlAnalyzer.testHiveFunctions("udtf_1", "Udtf_udtf_1");
    // 注册Spark ScalaUDF函数
    FunctionRegistry functionRegistry = BkSparkSqlAnalyzer.getFunctionRegistry(Map$.MODULE$.empty());
    String sql = "select udtf_1(words) as (a1, a2, a3) from test_in";
    LogicalPlan logicalPlan = BkSparkSqlAnalyzer.parserSQL(sql,
            tables,
            functionRegistry,
            hiveUdfFunctions);

    Iterable<SparkSqlTableEntity.Field> fields = JavaConverters.asJavaIterableConverter(
            BkSparkSqlAnalyzer.getOutFields(logicalPlan)).asJava();
    for (SparkSqlTableEntity.Field field : fields) {
      System.out.println(field.getIndex() + ", " + field.getType() + ", "  + field.getField());
    }
  }
}
