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

package com.tencent.bk.base.dataflow.spark.pipeline

import scala.collection.JavaConverters._
import java.util

import com.tencent.bk.base.dataflow.core.pipeline.AbstractDefaultPipeline
import com.tencent.bk.base.dataflow.spark.dmonitor.DMonitor
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchSinkStage
import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchSourceStage
import com.tencent.bk.base.dataflow.spark.pipeline.transform.BatchTransformStage
import com.tencent.bk.base.dataflow.spark.runtime.BatchRuntime
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.AbstractBatchTransformNode
import com.tencent.bk.base.dataflow.spark.topology.{BatchTopology, DataFrameWrapper}
import MDCLogger.Level
import com.tencent.bk.base.dataflow.spark.exception.{BatchException, BatchNoDataException}
import org.apache.spark.sql.SparkSession

abstract class BatchPipeline(topology: BatchTopology)
  extends AbstractDefaultPipeline[BatchTopology, BatchRuntime](topology) {
  var spark: SparkSession = _

  // 使用java.util.map来和其他模块保持一致
  var sourceDataMap: util.Map[String, DataFrameWrapper] =
    new util.LinkedHashMap[String, DataFrameWrapper]()
  var transformDataMap: util.Map[String, DataFrameWrapper] =
    new util.LinkedHashMap[String, DataFrameWrapper]()
  var sinkDataMap: util.Map[String, DataFrameWrapper] =
    new util.LinkedHashMap[String, DataFrameWrapper]()

  var sourceStage: BatchSourceStage = _
  var transformStage: BatchTransformStage = _
  var sinkStage: BatchSinkStage = _

  override def prepare(): Unit = {
    this.spark = this.getRuntime.getEnv
    this.sourceStage = new BatchSourceStage(
      spark,
      sourceDataMap)
    this.transformStage = new BatchTransformStage(
      spark,
      sourceDataMap,
      transformDataMap)
    this.sinkStage = new BatchSinkStage(
      spark,
      sourceDataMap,
      transformDataMap,
      sinkDataMap)
  }

  override def source(): Unit = {
    val sourceNodes = this.topology.getSourceNodes.asScala
    MDCLogger.logger(level = Level.INFO, errMsg = s"Found ${sourceNodes.size} source nodes")
    sourceNodes.foreach {
      sourceTuple => {
        this.sourceStage.source(sourceTuple._2.asInstanceOf[AbstractBatchSourceNode])
      }
    }
  }

  override def transform(): Unit = {
    val transformNodes = this.topology.getTransformNodes.asScala
    MDCLogger.logger(level = Level.INFO, errMsg = s"Found ${transformNodes.size} transform nodes")
    transformNodes.foreach {
      transformTuple => {
        this.transformStage.transform(transformTuple._2.asInstanceOf[AbstractBatchTransformNode])
      }
    }
  }

  override def sink(): Unit = {
    val sinkNodes = this.topology.getSinkNodes.asScala
    MDCLogger.logger(level = Level.INFO, errMsg = s"Found ${sinkNodes.size} sink nodes")
    sinkNodes.foreach {
      sinkTuple => {
        this.sinkStage.sink(sinkTuple._2.asInstanceOf[AbstractBatchSinkNode])
      }
    }
  }

  override def submit(): Unit = {
    DMonitor.initBatchSqlMonitorParamByTopology(topology)
    try {
      this.prepare()
      this.source()
      this.afterSource()
      this.transform()
      this.afterTransform()
      this.sink()
      this.afterSink()
    } catch {
      case e @ (_: BatchException | _: BatchNoDataException) =>
        this.sendMetrics(topology)
        throw e
      case e: Exception =>
        this.sendMetrics(topology)
        MDCLogger.logger("计算框架异常，请联系系统管理员处理。", cause = e, level = Level.ERROR)
        throw new Exception("计算框架异常，请联系系统管理员处理。")
    } finally {
      this.getRuntime.close()
    }
  }

  def afterSource(): Unit

  def afterTransform(): Unit

  def afterSink(): Unit

  protected def updateMetricsInput(): Unit = {
    this.sourceDataMap.asScala.foreach{
      case (k, v) =>
        val cnt = v.count();
        MDCLogger.logger(s"Added input count: $k -> $cnt", level = Level.INFO)
        DMonitor.dMonitorParam.input += (k -> cnt)
    }
  }

  protected def updateMetricsOutput(): Unit = {
    this.sinkDataMap.asScala.foreach{
      case (k, v) =>
        val cnt = v.count()
        MDCLogger.logger(s"Added output count: $k -> $cnt", level = Level.INFO)
        DMonitor.dMonitorParam.output += (k -> cnt)
    }
  }

  protected def sendMetrics(topology: BatchTopology): Unit = {
    if (topology.enableMetrics()) {
      val geogAreaCode = topology.getGeogAreaCode()
      DMonitor.sendMonitorMetrics(geogAreaCode)
    }
  }
}
