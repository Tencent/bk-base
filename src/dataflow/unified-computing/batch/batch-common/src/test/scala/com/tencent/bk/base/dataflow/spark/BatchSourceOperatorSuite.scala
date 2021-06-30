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

import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchSourceStage
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.apache.spark.util.BatchUtils
import org.junit.Assert.{assertEquals, assertTrue}

class BatchSourceOperatorSuite extends BatchSharedSQLContext {

  var tmpDir: String = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = BatchUtils.createTempDir()
    ScalaTestUtils.createTableData1(
      spark,
      tmpDir,
      "2021052000",
      "2021052023")

    ScalaTestUtils.createTableData1(
      spark,
      s"$tmpDir/copy",
      "2021052500",
      "2021052501")
  }

  test("test BatchJsonSourceOperator") {
    val sourceNodeId = "1_json_source_node"
    var sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val sourceNode = TopoSourceNodeTest.createSimpleJsonSourceNode(sourceNodeId)
    val stage = new BatchSourceStage(
      this.spark,
      sourceDataMap)
    stage.source(sourceNode)
    val dfWrapper = sourceDataMap.get(sourceNodeId)
    assertEquals(3, dfWrapper.count())
    val df = spark.sql("select id, test_time_str, test_time_long from json_source_node_1")
    val rowArray = df.collect()
    var id = 1
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
        row => {
          val expectedId = id.toString
          val resultId = row.getAs[String]("id")
          assertEquals(resultId, expectedId)
          id = id + 1
        }
      )
  }

  test("test BatchHDFSSourceOperator") {
    val sourceNodeId = "1_hdfs_source_node"
    var sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val sourceNode = TopoSourceNodeTest.createSimpleHdfsSourceNode(
      sourceNodeId,
      tmpDir,
      "2021052005",
      "2021052010")

    val stage = new BatchSourceStage(
      this.spark,
      sourceDataMap)
    stage.source(sourceNode)

    assertTrue(sourceDataMap.containsKey(sourceNodeId))

    val dfWrapper = sourceDataMap.get(sourceNodeId)
    assertEquals(10, dfWrapper.count())

    val rowArray = dfWrapper.getDataFrame().collect()
    var id = 11
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
      row => {
        val expectedId = id.toString
        val resultId = row.getAs[String]("id")
        assertEquals(resultId, expectedId)
        id = id + 1
      }
    )
  }

  test("test BatchHDFSCopySourceOperator") {
    val sourceNodeId = "1_hdfs_source_node"
    var sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()

    val sourceNode = TopoSourceNodeTest.createSimpleHdfsCopySourceNode(
      sourceNodeId,
      tmpDir,
      "2021052500",
      "2021052502")

    val stage = new BatchSourceStage(
      this.spark,
      sourceDataMap)
    stage.source(sourceNode)

    assertTrue(sourceDataMap.containsKey(sourceNodeId))

    val dfWrapper = sourceDataMap.get(sourceNodeId)
    assertEquals(4, dfWrapper.count())

    val rowArray = dfWrapper.getDataFrame().collect()
    var id = 1
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
        row => {
          val expectedId = id.toString
          val resultId = row.getAs[String]("id")
          assertEquals(resultId, expectedId)
          id = id + 1
        }
      )
  }
}
