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
import com.tencent.bk.base.dataflow.spark.exception.BatchInputNoDataException
import com.tencent.bk.base.dataflow.spark.pipeline.BatchPipeline
import com.tencent.bk.base.dataflow.spark.exception.BatchOutputNoDataException
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.apache.spark.util.BatchUtils
import org.junit.Assert.assertEquals

class BatchPipelineSuite extends BatchSharedSQLContext {

  var tmpSourceDir: String = _
  var tmpSinkDir: String = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    tmpSourceDir = BatchUtils.createTempDir()
    ScalaTestUtils.createTableData1(
      spark,
      tmpSourceDir,
      "2021052000",
      "2021052023")

    tmpSinkDir = BatchUtils.createTempDir()
  }

  test("test BatchPipeline") {
    val info = new util.HashMap[String, Object]
    info.put("job_id", "topo_test")
    info.put("job_name", "topo_test")
    info.put("job_type", "batch_sql")
    info.put("run_mode", "test")
    info.put("execute_id", "11111")
    val sourceNodeMap = new util.HashMap[String, Object]
    val sourceNodeConf = new util.HashMap[String, Object]
    sourceNodeConf.put("result_table_id", "8_hdfs_source_node")
    sourceNodeConf.put("root_path", this.tmpSourceDir)
    sourceNodeConf.put("start_time", "2021052000")
    sourceNodeConf.put("end_time", "2021052005")
    sourceNodeMap.put("8_hdfs_source_node", sourceNodeConf)
    info.put("source_node_info", sourceNodeMap)

    val transformNodeMap = new util.HashMap[String, Object]
    val transformNodeConf = new util.HashMap[String, Object]
    transformNodeConf.put("result_table_id", "8_hdfs_sink_node")
    transformNodeConf.put("sql", "select id, test_time_str from hdfs_source_node_8")
    transformNodeMap.put("8_hdfs_sink_node", transformNodeConf)
    info.put("transform_node_info", transformNodeMap)

    val sinkNodeMap = new util.HashMap[String, Object]
    val sinkNodeConf = new util.HashMap[String, Object]
    sinkNodeConf.put("result_table_id", "8_hdfs_sink_node")
    sinkNodeConf.put("root_path", this.tmpSinkDir)
    sinkNodeConf.put("dt_event_timestamp", 1621440000000L.asInstanceOf[java.lang.Long])
    sinkNodeConf.put("schedule_timestamp", 1621526400000L.asInstanceOf[java.lang.Long])
    sinkNodeMap.put("8_hdfs_sink_node", sinkNodeConf)
    info.put("sink_node_info", sinkNodeMap)

    val builder = new TopologyTest.TestBatchTopologyBuilder(info)
    val batchTopology: BatchTopology = builder.build()
    val testBatchPipeline = new TestBatchPipeline(batchTopology)
    testBatchPipeline.submit()
    assertEquals(10, DMonitor.dMonitorParam.input("8_hdfs_source_node"))
    assertEquals(10, DMonitor.dMonitorParam.output("8_hdfs_sink_node"))

    val resultDf = this.spark.read.parquet(s"$tmpSinkDir/2021/05/20/00")
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

  test("test SourceNoData") {
    val info = new util.HashMap[String, Object]
    info.put("job_id", "topo_test")
    info.put("job_name", "topo_test")
    info.put("job_type", "batch_sql")
    info.put("run_mode", "test")
    info.put("execute_id", "11111")
    val sourceNodeMap = new util.HashMap[String, Object]
    val sourceNodeConf = new util.HashMap[String, Object]
    sourceNodeConf.put("result_table_id", "8_hdfs_source_node")
    sourceNodeConf.put("root_path", this.tmpSourceDir)
    sourceNodeConf.put("start_time", "2021052100")
    sourceNodeConf.put("end_time", "2021052105")
    sourceNodeMap.put("8_hdfs_source_node", sourceNodeConf)
    info.put("source_node_info", sourceNodeMap)

    val transformNodeMap = new util.HashMap[String, Object]
    val transformNodeConf = new util.HashMap[String, Object]
    transformNodeConf.put("result_table_id", "8_hdfs_sink_node")
    transformNodeConf.put("sql", "select id, test_time_str from hdfs_source_node_8")
    transformNodeMap.put("8_hdfs_sink_node", transformNodeConf)
    info.put("transform_node_info", transformNodeMap)

    val sinkNodeMap = new util.HashMap[String, Object]
    val sinkNodeConf = new util.HashMap[String, Object]
    sinkNodeConf.put("result_table_id", "8_hdfs_sink_node")
    sinkNodeConf.put("root_path", this.tmpSinkDir)
    sinkNodeConf.put("dt_event_timestamp", 1621440000000L.asInstanceOf[java.lang.Long])
    sinkNodeConf.put("schedule_timestamp", 1621526400000L.asInstanceOf[java.lang.Long])
    sinkNodeMap.put("8_hdfs_sink_node", sinkNodeConf)
    info.put("sink_node_info", sinkNodeMap)

    val builder = new TopologyTest.TestBatchTopologyBuilder(info)
    val batchTopology: BatchTopology = builder.build()
    val testBatchPipeline = new TestBatchPipeline(batchTopology)
    try {
      testBatchPipeline.submit()
    } catch {
      case e: BatchInputNoDataException =>
        assertEquals("101 -> 上级数据节点没有数据输出", e.getMessage)
    }
  }

  test("test SinkNoData") {
    val info = new util.HashMap[String, Object]
    info.put("job_id", "topo_test")
    info.put("job_name", "topo_test")
    info.put("job_type", "batch_sql")
    info.put("run_mode", "test")
    info.put("execute_id", "11111")
    val sourceNodeMap = new util.HashMap[String, Object]
    val sourceNodeConf = new util.HashMap[String, Object]
    sourceNodeConf.put("result_table_id", "8_hdfs_source_node")
    sourceNodeConf.put("root_path", this.tmpSourceDir)
    sourceNodeConf.put("start_time", "2021052000")
    sourceNodeConf.put("end_time", "2021052005")
    sourceNodeMap.put("8_hdfs_source_node", sourceNodeConf)
    info.put("source_node_info", sourceNodeMap)

    val transformNodeMap = new util.HashMap[String, Object]
    val transformNodeConf = new util.HashMap[String, Object]
    transformNodeConf.put("result_table_id", "8_hdfs_sink_node")
    transformNodeConf.put("sql", "select id, test_time_str from hdfs_source_node_8 where 1=0")
    transformNodeMap.put("8_hdfs_sink_node", transformNodeConf)
    info.put("transform_node_info", transformNodeMap)

    val sinkNodeMap = new util.HashMap[String, Object]
    val sinkNodeConf = new util.HashMap[String, Object]
    sinkNodeConf.put("result_table_id", "8_hdfs_sink_node")
    sinkNodeConf.put("root_path", this.tmpSinkDir)
    sinkNodeConf.put("dt_event_timestamp", 1621440000000L.asInstanceOf[java.lang.Long])
    sinkNodeConf.put("schedule_timestamp", 1621526400000L.asInstanceOf[java.lang.Long])
    sinkNodeMap.put("8_hdfs_sink_node", sinkNodeConf)
    info.put("sink_node_info", sinkNodeMap)

    val builder = new TopologyTest.TestBatchTopologyBuilder(info)
    val batchTopology: BatchTopology = builder.build()
    val testBatchPipeline = new TestBatchPipeline(batchTopology)
    try {
      testBatchPipeline.submit()
    } catch {
      case e: BatchOutputNoDataException =>
        assertEquals("111 -> 计算结果无数据，请检查SQL。", e.getMessage)
    }
  }

}



class TestBatchPipeline(topology: BatchTopology) extends BatchPipeline(topology) {

  override def createRuntime(): TestBatchRuntime = {
    new TestBatchRuntime(this.topology)
  }

  override def afterSource(): Unit = {
    this.updateMetricsInput()
    if (DMonitor.isInputEmpty()) {
      throw new BatchInputNoDataException("上级数据节点没有数据输出")
    }
  }

  override def afterTransform(): Unit = {

  }

  override def afterSink(): Unit = {
    this.updateMetricsOutput()
    if (DMonitor.isOutputEmpty()) {
      throw new BatchOutputNoDataException("计算结果无数据，请检查SQL。")
    }
  }
}
