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

import org.apache.spark.sql.SparkSession
import java.util

import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, HDFSDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSCopySinkNode
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil

class BatchHDFSCopySinkOperator(
    node: BatchHDFSCopySinkNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper],
    transformDataMap: util.Map[String, DataFrameWrapper],
    sinkDataMap: util.Map[String, DataFrameWrapper])
  extends BatchHDFSSinkOperator(
    node,
    spark,
    sourceDataMap,
    transformDataMap,
    sinkDataMap) {

  override def createNode(): Unit = {
    val resultTableId = node.getNodeId

    val inputDataFrameWrapper = getInputDataFrameWrapper(resultTableId)

    if (inputDataFrameWrapper != null) {
      val df = inputDataFrameWrapper.getDataFrame()
      val finalDf = createSinkDataFrame(df, node.getDtEventTimeStamp, spark)
      var hdfsPath = this.saveHdfsDataFrame(finalDf, resultTableId, node.getHdfsCopyOutput)

      if (node.isEnableMoveToOrigin) {
        hdfsPath = moveDataToOriginLocation(
          resultTableId,
          node.getHdfsCopyOutput,
          node.getHdfsOutput)
      }

      val resultDf = HDFSDataFrameWrapper(
        spark,
        finalDf,
        resultTableId,
        HadoopUtil.listStatus(hdfsPath))
      this.sinkDataMap.put(resultTableId, resultDf)
    } else {
      throw new BatchException(
        "HDFS Sink Node requires parent node has output for sink", ErrorCode.BATCH_JOB_ERROR);
    }
  }

  private def moveDataToOriginLocation(
      resultTableId: String,
      hdfsCopyPath: BatchSinglePathOutput,
      hdfsTargetOutput: BatchSinglePathOutput): String = {
    MDCLogger.logger(
      s"Moving data from copy path ${hdfsCopyPath.getOutputInfo} " +
        s"to origin path ${hdfsTargetOutput.getOutputInfo}")
    val files = HadoopUtil.listStatus(hdfsCopyPath.getOutputInfo)
    val isFileExits = files.map(_.getPath.getName).exists(_.endsWith(".parquet"))

    if (isFileExits) {
      HadoopUtil.deleteDirectory(hdfsTargetOutput.getOutputInfo)
      HadoopUtil.moveFile(hdfsCopyPath.getOutputInfo, hdfsTargetOutput.getOutputInfo)
      callApiAfterSave(resultTableId, hdfsTargetOutput.getOutputInfo)
    }
    hdfsTargetOutput.getOutputInfo
  }
}




