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

import com.tencent.bk.base.dataflow.spark.dmonitor.DMonitor
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchSinkStage
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchDebugHTTPSinkNodeOperator
import com.tencent.bk.base.dataflow.spark.topology.DefaultDataFrameWrapper
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.apache.spark.util.BatchUtils
import org.junit.Assert.{assertEquals, assertTrue}

import scala.collection.mutable

class BatchSinkOperatorSuite extends BatchSharedSQLContext {
  var tmpDir: String = _

  object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  import internalImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = BatchUtils.createTempDir()
    ScalaTestUtils.createTableData1(
      spark,
      tmpDir,
      "2021052000",
      "2021052023")
  }

  test("test BatchHDFSSinkOperator") {
    val sinkNodeId = "hdfs_sink_node_1"
    val expectedFormat = "parquet"
    val expectedOutputMode = "overwrite"
    val expectedDtEventTime = 1621440000000L
    val expectedScheduleTime = 1621526400000L

    val sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val transformDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val sinkDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()

    val df = spark.sparkContext.parallelize(
      TestData("1", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("2", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("3", "2021-05-20 00:00:00", 1621440000000L) :: Nil, 1)
      .toDF()
    val dfWraper = DefaultDataFrameWrapper(this.spark, df, sinkNodeId)

    transformDataMap.put(sinkNodeId, dfWraper)

    val options = new TopoSinkNodeTest.HdfsSinkOption
    options.setEnableCallMaintainInterface(false)
    options.setEnableCallShipperInterface(false)
    options.setForceUpdateReservedSchema(false)
    options.setReservedFieldEnabled(true)
    options.setDtEventTimeStamp(expectedDtEventTime)
    options.setScheduleTime(expectedScheduleTime)

    val hdfsSinkNode = TopoSinkNodeTest.createSimpleHdfsSinkNode(
      sinkNodeId,
      tmpDir,
      expectedFormat,
      expectedOutputMode,
      options)

    val stage = new BatchSinkStage(
      this.spark,
      sourceDataMap,
      transformDataMap,
      sinkDataMap)

    stage.sink(hdfsSinkNode)

    val resultDf = this.spark.read.parquet(s"$tmpDir/2021/05/20/00")
    val rowArray = resultDf.collect()
    var id = 1
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
        row => {
          val expectedId = id.toString
          assertEquals(expectedId, row.getAs[String]("id"))
          assertEquals("2021-05-20 00:00:00", row.getAs[String]("dtEventTime"))
          assertEquals(1621440000000L, row.getAs[Long]("dtEventTimeStamp"))
          assertEquals(20210520, row.getAs[String]("thedate"))
          id = id + 1
        }
      )
  }

  test("test BatchHDFSCopySinkOperator") {
    val sinkNodeId = "hdfs_sink_node_1"
    val expectedFormat = "parquet"
    val expectedOutputMode = "overwrite"
    val expectedDtEventTime = 1621440000000L
    val expectedScheduleTime = 1621526400000L

    val sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val transformDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val sinkDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()

    val df = spark.sparkContext.parallelize(
      TestData("1", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("2", "2021-05-20 00:00:00", 1621440000000L) ::
        TestData("3", "2021-05-20 00:00:00", 1621440000000L) :: Nil, 1)
      .toDF()
    val dfWraper = DefaultDataFrameWrapper(this.spark, df, sinkNodeId)

    transformDataMap.put(sinkNodeId, dfWraper)

    val options = new TopoSinkNodeTest.HdfsSinkOption
    options.setEnableCallMaintainInterface(false)
    options.setEnableCallShipperInterface(false)
    options.setForceUpdateReservedSchema(false)
    options.setReservedFieldEnabled(true)
    options.setDispatchToStorage(false)
    options.setDtEventTimeStamp(expectedDtEventTime)
    options.setScheduleTime(expectedScheduleTime)

    val hdfsSinkNode = TopoSinkNodeTest.createSimpleHdfsCopySinkNode(
      sinkNodeId,
      tmpDir,
      expectedFormat,
      expectedOutputMode,
      options)

    val stage = new BatchSinkStage(
      this.spark,
      sourceDataMap,
      transformDataMap,
      sinkDataMap)

    stage.sink(hdfsSinkNode)

    val resultDf = this.spark.read.parquet(s"$tmpDir/copy/2021/05/20/00")
    val rowArray = resultDf.collect()
    var id = 1
    rowArray
      .sortWith(_.getAs[String]("id").toInt < _.getAs[String]("id").toInt)
      .foreach(
        row => {
          val expectedId = id.toString
          assertEquals(expectedId, row.getAs[String]("id"))
          assertEquals("2021-05-20 00:00:00", row.getAs[String]("dtEventTime"))
          assertEquals(1621440000000L, row.getAs[Long]("dtEventTimeStamp"))
          assertEquals(20210520, row.getAs[String]("thedate"))
          id = id + 1
        }
      )
  }

  test("test BatchDebugHTTPSinkNodeOperator") {
    val debugId = "test_debug"
    val execId = 123456
    val httpSinkNode = TopoSinkNodeTest.createSimpleDebugHttpSinkNode(debugId, execId, true)
    DMonitor.dMonitorParam.input = scala.collection.mutable.Map.empty[String, Long]
    DMonitor.dMonitorParam.output = scala.collection.mutable.Map.empty[String, Long]
    DMonitor.dMonitorParam.input += ("fake_source" -> 10)
    val sourceDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val transformDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()
    val sinkDataMap: util.Map[String, DataFrameWrapper] =
      new util.LinkedHashMap[String, DataFrameWrapper]()

    val df = spark.createDataFrame((1 to 10)
      .map(i => (i, s"$i"))).toDF("key", "val")
    val dfWrapper = DefaultDataFrameWrapper(this.spark, df, debugId)

    val resultBuffer = mutable.Buffer.empty[String]
    resultBuffer.append("{\"key\":1,\"val\":\"1\"}")
    resultBuffer.append("{\"key\":2,\"val\":\"2\"}")
    resultBuffer.append("{\"key\":3,\"val\":\"3\"}")
    resultBuffer.append("{\"key\":4,\"val\":\"4\"}")
    resultBuffer.append("{\"key\":5,\"val\":\"5\"}")

    transformDataMap.put(debugId, dfWrapper);
    val testHttpSinkNodeOperator = new TestHttpSinkNodeOperator(
      httpSinkNode,
      this.spark,
      sourceDataMap,
      transformDataMap,
      sinkDataMap,
      resultBuffer.toList,
      10,
      5
    )

    testHttpSinkNodeOperator.createNode()
    DMonitor.dMonitorParam.input = scala.collection.mutable.Map.empty[String, Long]
    DMonitor.dMonitorParam.output = scala.collection.mutable.Map.empty[String, Long]
  }
}

class TestHttpSinkNodeOperator(
    node: BatchDebugHTTPSinkNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper],
    transformDataMap: util.Map[String, DataFrameWrapper],
    sinkDataMap: util.Map[String, DataFrameWrapper],
    expectedResultData: List[String],
    expectedInputTotalCount: Long,
    expectedOutputTotalCount: Long)
  extends BatchDebugHTTPSinkNodeOperator(
    node,
    spark,
    sourceDataMap,
    transformDataMap,
    sinkDataMap) {

  override def sendErrorData(
    errorCode: String,
    errorMessage: String,
    errorMessageEn: String): Unit = {

  }

  override def sendMetricInfo(inputTotalCount: Long, outputTotalCount: Long): Unit = {
    assertEquals(expectedInputTotalCount, inputTotalCount)
    assertEquals(outputTotalCount, outputTotalCount)
  }

  override def sendResultData(resultData: List[String]): Unit = {
    assertEquals(expectedResultData.size, resultData.size)
    expectedResultData.foreach(
      v => assertTrue(resultData.contains(v))
    )
  }
}
