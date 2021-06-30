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

package com.tencent.bk.base.dataflow.spark.pipeline.source

import java.util

import com.tencent.bk.base.dataflow.spark.exception.BatchAccumulateNotInRangeException
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, IcebergDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode
import com.tencent.bk.base.datahub.iceberg.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class BatchIcebergSourceOperator(
    node: BatchIcebergSourceNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper])
  extends BatchSourceOperator[DataFrameWrapper](
    node,
    sourceDataMap) {

  override def createNode(): DataFrameWrapper = {
    if (this.node.isAccumulateNotInRange) throw new BatchAccumulateNotInRangeException("累加窗口不在范围内")
    val icebergDf = createDataFrame()
    MDCLogger.logger(icebergDf.schema.treeString)
    val icebergInput = this.node.getBatchIcebergInput
    val icebergDfWrapper = IcebergDataFrameWrapper(
      spark,
      node.getIcebergConf,
      icebergInput.getInputInfo,
      icebergInput,
      icebergDf,
      this.node.getNodeId)
    icebergDfWrapper.makeTempView()
    this.sourceDataMap.put(this.node.getNodeId, icebergDfWrapper)
    icebergDfWrapper
  }

  private def createDataFrame(): DataFrame = {
    val icebergInput = this.node.getBatchIcebergInput

    val filterExpression = icebergInput.getFilterExpression
    MDCLogger.logger(s"Iceberg dataframe filter expression -> $filterExpression")
    MDCLogger.logger(s"Iceberg hdfs conf -> ${node.getIcebergConf.toString}")
    val df = SparkUtils.readTable(spark, icebergInput.getInputInfo, node.getIcebergConf)
    val resultDf = df.filter(filterExpression)
    // if there is a limit value from input
    if (icebergInput.getLimit > 0) {
      return resultDf.limit(icebergInput.getLimit)
    }

    resultDf
  }
}
