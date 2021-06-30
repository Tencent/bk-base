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

package com.tencent.bk.base.dataflow.spark.sql.dmonitor

import java.util

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component
import com.tencent.bk.base.dataflow.spark.TopologyTest
import com.tencent.bk.base.dataflow.spark.dmonitor.DMonitor
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology
import org.apache.spark.sql.test.BatchSharedSQLContext
import org.junit.Assert.{assertEquals, assertTrue}

class DmonitorSuite extends BatchSharedSQLContext {
  test("test dmonitor") {
    val fakeSourcePath = "/fake_source"
    val fakeSinkPath = "/fake_sink"
    val info = new util.HashMap[String, Object]
    info.put("job_id", "topo_test")
    info.put("job_name", "topo_test")
    info.put("job_type", "batch_sql")
    info.put("run_mode", "test")
    info.put("execute_id", "11111")
    val sourceNodeMap = new util.HashMap[String, Object]
    val sourceNodeConf = new util.HashMap[String, Object]
    sourceNodeConf.put("result_table_id", "8_hdfs_source_node")
    sourceNodeConf.put("root_path", fakeSourcePath)
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
    sinkNodeConf.put("root_path", fakeSinkPath)
    sinkNodeConf.put("dt_event_timestamp", 1621440000000L.asInstanceOf[java.lang.Long])
    sinkNodeConf.put("schedule_timestamp", 1621526400000L.asInstanceOf[java.lang.Long])
    sinkNodeMap.put("8_hdfs_sink_node", sinkNodeConf)
    info.put("sink_node_info", sinkNodeMap)

    val builder = new TopologyTest.TestBatchTopologyBuilder(info)
    val batchTopology: BatchTopology = builder.build()
    DMonitor.initBatchSqlMonitorParamByTopology(batchTopology)
    val metricObject = DMonitor.createMetricObject("spark", Component.spark_sql)
    val inputData = metricObject.getDataLoss.getInput
    assertTrue(inputData.getTags.containsKey("8_hdfs_source_node|1621440000_1621458000"))
    assertEquals(0L, inputData.getTags.get("8_hdfs_source_node|1621440000_1621458000"))
    val outputData = metricObject.getDataLoss.getOutput
    assertTrue(outputData.getTags.containsKey("8_hdfs_sink_node|1621440000_1621526400"))
    assertEquals(0L, outputData.getTags.get("8_hdfs_sink_node|1621440000_1621526400"))
    val data = metricObject.collectMetric()
    data
  }
}
