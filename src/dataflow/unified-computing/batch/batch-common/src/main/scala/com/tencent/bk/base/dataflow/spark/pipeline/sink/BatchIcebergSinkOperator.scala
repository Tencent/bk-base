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

import java.sql.Timestamp
import java.util

import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, IcebergDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode
import com.tencent.bk.base.dataflow.spark.utils.APIUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class BatchIcebergSinkOperator(
    node: BatchIcebergSinkNode,
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
    val resultTableId = node.getNodeId

    val inputDataFrameWrapper = getInputDataFrameWrapper(resultTableId)

    if (inputDataFrameWrapper != null) {
      val dtEventTimeStamp = node.getDtEventTimeStamp
      val df = inputDataFrameWrapper.getDataFrame()
      val finalDf = createIcebergSinkDataFrame(
        df,
        dtEventTimeStamp,
        spark,
        node.isEnableIcebergPartitionColumn)

      val icebergOutput = node.getBatchIcebergOutput
      this.saveIcebergDataFrame(
        spark,
        finalDf,
        this.node.getNodeId,
        icebergOutput,
        node.getIcebergConf)
      val resultDfWrapper = IcebergDataFrameWrapper(
        spark,
        node.getIcebergConf,
        icebergOutput.getOutputInfo,
        icebergOutput,
        finalDf,
        this.node.getNodeId)
      this.sinkDataMap.put(resultTableId, resultDfWrapper)

      val count = resultDfWrapper.count()

      if (count > 0) {
        callApiAfterSave(resultTableId, node.getDtEventTimeStamp)
      }
    } else {
      throw new BatchException(
        s"${this.getClass.getSimpleName} requires parent node has output for sink",
        ErrorCode.BATCH_JOB_ERROR)
    }
  }

  protected def createIcebergSinkDataFrame(
    df: DataFrame,
    dtEventTimeStamp: Long,
    spark: SparkSession,
    enableIcebergPartitionColumn: Boolean): DataFrame = {
    val tmpDf = super.createSinkDataFrame(df, dtEventTimeStamp, spark, true)
    val tmpDfNoPartitionColumn = tmpDf.drop(UCSparkConf.icebergParitionColumn)
    val resultDf = if (enableIcebergPartitionColumn) {
      this.addIcebergPartitionColumn(tmpDfNoPartitionColumn, dtEventTimeStamp)
    } else {
      tmpDfNoPartitionColumn
    }
    resultDf
  }

  protected def callApiAfterSave(resultTableId: String, timestamp: Long): Unit = {
    if (node.isEnableCallShipperInterface) {
      val endTimeStamp = timestamp + 3600 * 1000L
      val timeRangeStr = s"$timestamp~$endTimeStamp"
      MDCLogger.logger(s"Iceberg Time range -> ${timeRangeStr}", kv = Map("rt_id" -> resultTableId))
      val scheduleTimeInHour = node.getScheduleTimeInHour()
      val addRtn = APIUtil.addConnector(
        resultTableId,
        timeRangeStr,
        scheduleTimeInHour
      )
      MDCLogger.logger(addRtn.toString(), kv = Map("rt_id" -> resultTableId))
    }
  }

  protected def addIcebergPartitionColumn(df: DataFrame, dtEventTimeStamp: Long): DataFrame = {
    val partitionColumn = UCSparkConf.icebergParitionColumn
    val dfWithPartitionColumn = df.withColumn(partitionColumn, lit(new Timestamp(dtEventTimeStamp)))
    MDCLogger.logger(s"Added $partitionColumn column with value: $dtEventTimeStamp")
    dfWithPartitionColumn
  }

}
