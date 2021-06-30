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

package com.tencent.bk.base.dataflow.spark.dmonitor

import java.net.InetAddress

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component
import com.tencent.bk.base.dataflow.core.metric.{MetricDataLoss, MetricDataStatics, MetricHttpManager, MetricObject, MetricTag}
import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode
import com.tencent.bk.base.dataflow.spark.utils.{CommonUtil, HadoopUtil}
import MDCLogger.Level
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil

import scala.collection.{JavaConverters, mutable}
import scala.beans.BeanProperty

/**
  * 数据埋点监控
  */
object DMonitor {

  val dMonitorParam: DMonitorParam = new DMonitorParam()

  def isInputEmpty(): Boolean = {
    !DMonitor.dMonitorParam.input.exists(p => p._2 > 0)
  }

  def isOutputEmpty(): Boolean = {
    !DMonitor.dMonitorParam.output.exists(p => p._2 > 0)
  }

  def initBatchSqlMonitorParamByTopology(topology: BatchTopology): Unit = {
    try {
      MDCLogger.logger(level = Level.INFO, errMsg = s"Start to init monitor by topology")

      DMonitor.dMonitorParam.input = scala.collection.mutable.Map.empty[String, Long]
      DMonitor.dMonitorParam.inputTimeRange = scala.collection.mutable.Map.empty[String, String]
      DMonitor.dMonitorParam.output = scala.collection.mutable.Map.empty[String, Long]
      DMonitor.dMonitorParam.outputTimeRange = scala.collection.mutable.Map.empty[String, String]

      DMonitor.dMonitorParam.execute_id = topology.getExecuteId()
      DMonitor.dMonitorParam.rt_id = topology.getJobId
      DMonitor.dMonitorParam.clusterName = HadoopUtil.getClusterName()

      val sourceTables = JavaConverters.mapAsScalaMapConverter(topology.getSourceNodes).asScala
      for (table <- sourceTables) {
        val parentRtID = table._1
        val sourceNode = table._2.asInstanceOf[AbstractBatchSourceNode]
        DMonitor.dMonitorParam.input += (parentRtID -> 0)
        DMonitor.dMonitorParam.inputTimeRange += (parentRtID -> sourceNode.getInputTimeRangeString)
      }

      val sinkTables = JavaConverters.mapAsScalaMapConverter(topology.getSinkNodes).asScala
      for (table <- sinkTables) {
        val childRtID = table._1
        val sinkNode = table._2.asInstanceOf[AbstractBatchSinkNode]
        DMonitor.dMonitorParam.output += (childRtID -> 0)
        DMonitor.dMonitorParam.outputTimeRange += (childRtID -> sinkNode.getOutputTimeRangeString)
      }
      MDCLogger.logger(
        level = Level.INFO,
        errMsg = s"input: ${DMonitor.dMonitorParam.input}, " +
          s"output: ${DMonitor.dMonitorParam.output}")
    } catch {
      case e: Exception => {
        val errMsg = s"统计埋点数据初始化失败 : ${e.getMessage}"
        MDCLogger.logger(errMsg, cause = e, level = Level.WARN)
      }
    }
  }

  // scalastyle:off
  def createMetricObject(component: String, jobType: Component): MetricObject = {
    val obj = new MetricObject()
    obj.setModule("batch")
    obj.setComponent(component)

    val addr = InetAddress.getLocalHost
    val ip = addr.getHostAddress
    val hostName = addr.getHostName

    val physicalTag = new MetricTag()
    physicalTag.setTag(ip + "|" + dMonitorParam.rt_id)
    val physicalDescMetric: mutable.Map[String, AnyRef] = mutable.Map()
    physicalDescMetric += ("result_table_id" -> dMonitorParam.rt_id)
    physicalDescMetric += ("hostname" -> hostName)
    physicalTag.setDesc(JavaConverters.mapAsJavaMapConverter(physicalDescMetric.toMap).asJava)
    obj.setPhysicalTag(physicalTag)
    val logicalTag = new MetricTag()
    logicalTag.setTag(dMonitorParam.rt_id)
    val logicalDescMetric: mutable.Map[String, AnyRef] = mutable.Map()
    logicalDescMetric += ("result_table_id" -> dMonitorParam.rt_id)
    logicalTag.setDesc(JavaConverters.mapAsJavaMapConverter(logicalDescMetric.toMap).asJava)
    obj.setLogicalTag(logicalTag)
    obj.setCluster("default")

    val customTags: mutable.Map[String, AnyRef] = mutable.Map()
    customTags += ("execute_id" -> dMonitorParam.execute_id)
    obj.setCustomTags(JavaConverters.mapAsJavaMapConverter(customTags.toMap).asJava)

    val storage: mutable.Map[String, AnyRef] = mutable.Map()
    jobType match {
      case Component.tdw_spark | Component.tdw_spark_jar => {
        storage += ("cluster_type" -> "tdw")
      }
      case _ => {
        storage += ("cluster_type" -> "hdfs")
      }
    }
    storage += ("cluster_name" -> dMonitorParam.clusterName)
    obj.setStorage(JavaConverters.mapAsJavaMapConverter(storage.toMap).asJava)

    val inputValue = dMonitorParam.input
    val outputValue = dMonitorParam.output
    val inputTimeRange = dMonitorParam.inputTimeRange
    val outputTimeRange = dMonitorParam.outputTimeRange
    val dataLoss = new MetricDataLoss

    val input = createInputOutputMetrics(inputValue, inputTimeRange)
    dataLoss.setInput(input)

    val output = createInputOutputMetrics(outputValue, outputTimeRange)
    dataLoss.setOutput(output)

    obj.setDataLoss(dataLoss)
    obj
  }
  // scalastyle:on

  /**
    * 发送埋点数据指定component
    * 默认component=spark
    */
  def sendMonitorMetrics(
      geogAreaCode: String,
      component: String = "spark",
      jobType: Component = Component.spark_sql,
      currentTimeStamp:Long = 0): Unit = {
    try {
      val obj = createMetricObject(component, jobType)
      val apiUrlPrefix = UCSparkConf.datamanageApiPrefix
      val fetchParamsUrl = s"$apiUrlPrefix/datamanage/dmonitor/metrics/report/"
      val metricManager = new MetricHttpManager()
      metricManager.send(obj, geogAreaCode, fetchParamsUrl)
    } catch {
      case e: Exception => {
        val errMsg = s"发送埋点数据失败 : ${e.getMessage}"
        MDCLogger.logger(errMsg, cause = e, level = Level.WARN)
      }
    }
  }

  def createInputOutputMetrics(
      valueMap: mutable.Map[String, Long],
      timeRangeMap: mutable.Map[String, String]): MetricDataStatics = {
    val metric = new MetricDataStatics
    val inputTag: mutable.Map[String, java.lang.Long] = mutable.Map()
    for ((rt_id, value) <- valueMap) {
      val timeRange = CommonUtil.getOptVal(timeRangeMap.get(rt_id))
      inputTag.put(rt_id + "|" + timeRange, new java.lang.Long(value))
    }
    metric.setTags(JavaConverters.mapAsJavaMapConverter(inputTag).asJava)
    val input_sum = valueMap.map(_._2).foldLeft(0L) { (sum, result) => sum + result }
    metric.setTotalCnt(input_sum)
    metric.setTotalCntIncrement(input_sum)
    metric
  }
}

class DMonitorParam() {
  @BeanProperty var rt_id: String = _
  @BeanProperty var input: scala.collection.mutable.Map[String, Long] = _
  @BeanProperty var inputTimeRange: scala.collection.mutable.Map[String, String] = _
  @BeanProperty var output: scala.collection.mutable.Map[String, Long] = _
  @BeanProperty var outputTimeRange: scala.collection.mutable.Map[String, String] = _
  @BeanProperty var clusterName: String = _
  @BeanProperty var execute_id: String = _
}


