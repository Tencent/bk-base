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

package com.tencent.bk.base.dataflow.spark

import java.util

import com.tencent.bk.base.dataflow.spark.pipeline.transform.BatchTransformStage
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.apache.spark.sql.{SQLContext, SQLImplicits}
import org.junit.Assert.assertEquals

class BatchTransformOperatorSuite extends BatchSharedSQLContext {
  var tmpDir: String = _

  object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  import internalImplicits._

  test("test BatchHDFSSinkOperator") {
    val transformNodeId = "hdfs_transform_node_1"

    val sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val transformDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()

    val df = spark.sparkContext.parallelize(
      TestData("1", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("2", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("3", "2021-05-20 00:00:00", 1621440000000L) :: Nil, 1)
      .toDF()
    df.createOrReplaceTempView("test_table_1")

    val expectedSql = "select id, test_time_str from test_table_1"
    val sqlTransformNode = TopoTransformNodeTest.createSimpleSqlTransformNode(
      transformNodeId,
      expectedSql)

    val stage = new BatchTransformStage(
      this.spark,
      sourceDataMap,
      transformDataMap)
    stage.transform(sqlTransformNode)

    val rowArray = transformDataMap.get(transformNodeId).getDataFrame().collect()
    var id = 1
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
        row => {
          val expectedId = id.toString
          assertEquals(2, row.size)
          assertEquals(expectedId, row.getAs[String]("id"))
          assertEquals(
            "2021-05-20 00:00:00",
            row.getAs[String]("test_time_str"))
          id = id + 1
        }
      )
  }
}
