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

package com.tencent.bk.base.dataflow.spark.sql.udf

import com.tencent.bk.base.dataflow.spark.sql.function.SparkZipFieldsToRecords
import com.tencent.bk.base.dataflow.spark.sql.function.base.SparkFunctionFactory
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType}
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfter

class BatchUDFSuite extends BatchSharedSQLContext with BeforeAndAfter {

  before {
    spark.udf.register("ConvertToInteger", new ConvertToInteger, IntegerType)
    spark.udf.register("ConvertToLong", new ConvertToLong, LongType)

    // 补上历史的zip函数注册
    spark.udf.register(
      "zip", new SparkZipFieldsToRecords, DataTypes.createArrayType(DataTypes.StringType))
    spark.udf.register("split_index", new SparkSplitIndex, StringType)
    spark.udf.register("batch_scheduleTime", new BatchScheduleTime, StringType)
    val sparkFunctionFactory = new SparkFunctionFactory(this.spark)
    sparkFunctionFactory.registerAll()
  }

  test("test ConvertToInteger") {
    withTempView("tempView") {
      val df = spark.createDataFrame((1 to 10).map(i => (i, s"$i"))).toDF("key", "val")
      df.createOrReplaceTempView("tempView")
      val df2 = spark.sql("select key, ConvertToInteger(val) as int_val from tempView")
      df2.collect().foreach(
        row => {
          assertEquals(row.getAs[Int]("key"), row.getAs[Int]("int_val"))
        }
      )
    }
  }

  test("test ConvertToLong") {
    withTempView("tempView") {
      val df = spark.createDataFrame((1622476800000L to 1622476800010L)
        .map(i => (i, s"$i"))).toDF("key", "val")
      df.createOrReplaceTempView("tempView")
      val df2 = spark.sql("select key, ConvertToLong(val) as int_val from tempView")
      df2.collect().foreach(
        row => {
          assertEquals(row.getAs[Long]("key"), row.getAs[Long]("int_val"))
        }
      )
    }
  }

  test("test batchScheduleTime") {
    withTempView("tempView") {
      val df = spark.createDataFrame((1 to 10).map(i => (i, s"$i"))).toDF("key", "val")
      df.createOrReplaceTempView("tempView")
      val df2 = spark.sql(
        """
          |select batch_scheduleTime('yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai') as date_str
          |from tempView
          |""".stripMargin)
      df2.collect().foreach(
        row => {
          assertEquals("2021-06-01 00:00:00", row.getAs[String]("date_str"))
        }
      )
    }
  }

  test("test SplitIndex") {
    withTempView("tempView") {
      val df = spark.createDataFrame((1 to 10)
        .map(i => (i, s"abc;acb;cab"))).toDF("key", "val")
      df.createOrReplaceTempView("tempView")
      val df2 = spark.sql(
        """
          |select split_index(val, ';', 2) as split_str
          |from tempView
          |""".stripMargin)
      df2.collect().foreach(
        row => {
          assertEquals("acb", row.getAs[String]("split_str"))
        }
      )
    }
  }

  test("test GroupConcat") {
    withTempView("tempView") {
      val df = spark.createDataFrame((1 to 9)
        .map(i => (i, s"${i % 2}"))).toDF("key", "val")
      df.createOrReplaceTempView("tempView")
      val df2 = spark.sql("""
          |select val, GroupConcat(key, '|', key, 'desc') as group_concat_str
          |from tempView group by val
          |""".stripMargin)
      df2.collect().foreach(
        row => {
          if ("0".equals(row.getAs[String]("val"))) {
            assertEquals("8|6|4|2", row.getAs[String]("group_concat_str"))
          } else {
            assertEquals("9|7|5|3|1", row.getAs[String]("group_concat_str"))
          }
        }
      )
    }
  }

  after {
    spark.sql("drop temporary function ConvertToInteger")
    spark.sql("drop temporary function ConvertToLong")
    spark.sql("drop temporary function zip")
    spark.sql("drop temporary function split_index")
    spark.sql("drop temporary function batch_scheduleTime")
    spark.sql("drop temporary function GroupConcat")
  }
}
