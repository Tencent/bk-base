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

package com.tencent.bk.base.dataflow.spark.pipeline.sink

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.Date

import com.tencent.bk.base.dataflow.spark.dmonitor.DMonitor
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode
import MDCLogger.Level
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter
import com.tencent.bk.base.dataflow.spark.utils.{APIUtil, JsonUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.JavaConverters._

class BatchDebugHTTPSinkNodeOperator (
    node: BatchDebugHTTPSinkNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper],
    transformDataMap: util.Map[String, DataFrameWrapper],
    sinkDataMap: util.Map[String, DataFrameWrapper])
  extends BatchSinkOperator(
    node,
    sourceDataMap,
    transformDataMap,
    sinkDataMap) {

  override def createNode(): Unit = {
    try {
      val inputTotalCount = DMonitor.dMonitorParam.input.toList.map(i => i._2).sum

      val transformDF = transformDataMap.get(this.node.getNodeId)
      if (transformDF.isEmpty()) {
        val message = "计算结果过滤掉所有数据"
        val messageEn = "The calculation results filter out all data."
        sendErrorData(ErrorCode.NO_OUTPUT_DATA, message, messageEn)
      } else {
        val resultData = if (node.getLimitValue > 0) {
          transformDF.getDataFrame().limit(node.getLimitValue).toJSON.collectAsList()
        } else {
          transformDF.getDataFrame().toJSON.collectAsList()
        }
        val outputCount = resultData.size()
        DMonitor.dMonitorParam.output += (this.node.getNodeId -> outputCount)
        sendMetricInfo(inputTotalCount, outputCount)
        sendResultData(resultData.asScala.toList)
      }
    } catch {
      case e: Exception =>
        MDCLogger.logger(e.getMessage, cause = e, level = Level.ERROR)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        val exceptionAsString = sw.toString
        val message = s"Run SQL Error : ${exceptionAsString}"
        sendErrorData(ErrorCode.BATCH_JOB_ERROR, message, message)
        if (node.isEnableThrowException) {
          throw e
        }
    }
  }

  protected def sendErrorData(
      errorCode: String,
      errorMessage: String,
      errorMessageEn: String): Unit = {
    val params: mutable.Map[String, Any] = mutable.Map()
    params.put("job_id", node.getExecuteId)
    params.put("result_table_id", node.getNodeId)
    params.put("error_code", errorCode)
    params.put("error_message", errorMessage)
    params.put("error_message_en", errorMessageEn)
    params.put("debug_date", System.currentTimeMillis())
    APIUtil.errorData(node.getDebugId, params.toMap)
  }

  protected def sendMetricInfo(inputTotalCount: Long, outputTotalCount: Long): Unit = {
    val params: mutable.Map[String, Any] = mutable.Map()
    params.put("job_id", node.getExecuteId)
    params.put("input_total_count", inputTotalCount)
    params.put("output_total_count", outputTotalCount)
    params.put("result_table_id", node.getNodeId)
    APIUtil.metricInfo(node.getDebugId, params.toMap)
  }

  protected def sendResultData(resultData: List[String]): Unit = {
    val params: mutable.Map[String, Any] = mutable.Map()
    params.put("job_id", node.getExecuteId)
    params.put("result_table_id", node.getNodeId)
    val result_data_str = JsonUtil.listToJsonStr(resultData)
    params.put("result_data", result_data_str)
    val time = TimeFormatConverter.DAY_FORMAT.format(new Date())
    params.put("debug_date", System.currentTimeMillis())
    params.put("thedate", time.toInt)
    APIUtil.resultData(node.getDebugId, params.toMap)
  }
}
