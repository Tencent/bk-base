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

package com.tencent.bk.base.dataflow.bksql.deparser.test.util

import org.apache.spark.sql.BkSparkSqlAnalyzer
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object SparkSqlFunctionTestUtilMain {

  def main(args: Array[String]): Unit = {
    // 函数验证
    val tables = new ListBuffer[CatalogTable]()
    tables.append(BkSparkSqlAnalyzer.createTableDesc(
      "1test1",
      new StructType()
        .add("_int", IntegerType)
        .add("_long", LongType)
        .add("_float", FloatType)
        .add("_double", DoubleType)
        .add("_str", StringType)))

    val sql =
      """
        |select
        |from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') as a1,
        |SUBSTRING('k1=v1;k2=v2', 2, 2) as a2,
        |SUBSTRING_INDEX('abc+dbc', '+', 1) as a3,
        |base64('Spark SQL') as a4,
        |CONCAT_WS('#', 'a', 'b' ) as a5,
        |CONCAT('Hello', 'World') as a6,
        |decode(encode('abc', 'utf-8'), 'utf-8') as a7,
        |encode('abc', 'utf-8') as a8,
        |format_string("Hello World %d %s", 100, "days") as a9,
        |from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') as a10,
        |lower('SparkSql') as a11,
        |trim(' SparkSQL ') as a12,
        |ltrim(' SparkSQL ') as a13,
        |rtrim(' SparkSQL ') as a15,
        |upper('SparkSql') as a16,
        |lpad('hi', 5, '??') as a17,
        |rpad('hi', 5, '??') as a18,
        |regexp_replace('100-200', '(\d+)', 'num') as a19,
        |regexp_extract('100-200', '(\d+)-(\d+)', 1) as a20,
        |replace('ABCabc', 'abc', 'DEF') as a21,
        |split('oneAtwoBthreeC', '[ABC]') as a22,
        |date_format('2016-04-08', 'y') as b1
        |from 1test1
      """.stripMargin
    val qe = BkSparkSqlAnalyzer.parserSQL(sql, tables, BkSparkSqlAnalyzer.getFunctionRegistry())
    // scalastyle:off println
    // println(qe.schema.fields.mkString(", "))
    qe.printSchema()
  }
}
