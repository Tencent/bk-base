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

import java.util

import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, HDFSDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode
import com.tencent.bk.base.dataflow.spark.utils.{APIUtil, CommonUtil, HadoopUtil}
import org.apache.spark.sql.SparkSession


class BatchHDFSSinkOperator(
    node: BatchHDFSSinkNode,
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
      val df = inputDataFrameWrapper.getDataFrame()
      val finalDf = createSinkDataFrame(df, node.getDtEventTimeStamp, spark)
      val hdfsPath = this.saveHdfsDataFrame(finalDf, resultTableId, node.getHdfsOutput)
      val resultDf = HDFSDataFrameWrapper(
        spark,
        finalDf,
        resultTableId,
        HadoopUtil.listStatus(hdfsPath))
      this.sinkDataMap.put(resultTableId, resultDf)

      val count = resultDf.count()

      if (count > 0) {
        callApiAfterSave(resultTableId, hdfsPath)
      }
    } else {
      throw new BatchException(
        s"${this.getClass.getSimpleName} requires parent node has output for sink",
        ErrorCode.BATCH_JOB_ERROR);
    }
  }

  protected def callApiAfterSave(resultTableId: String, hdfsPath: String): Unit = {
    if (node.isEnableCallMaintainInterface) {
      if (node.getDtEventTimeStamp == 0) {
        throw new BatchException(
          "sink node dt_event_time setting can't be 0", ErrorCode.BATCH_JOB_ERROR);
      }
      // 直接根据输出结果路径计算调用storekit维护api
      val days = CommonUtil.getDaysDiff(node.getDtEventTimeStamp, System.currentTimeMillis())
      val maintainDaysStr = s"-${days.toString}"
      APIUtil.callStorekitMaintain(resultTableId, maintainDaysStr)
    }

    if (node.isEnableCallShipperInterface) {
      MDCLogger.logger(s"hdfsPath -> ${hdfsPath}", kv = Map("rt_id" -> resultTableId))
      val scheduleTimeInHour = node.getScheduleTimeInHour()
      val addRtn = APIUtil.addConnector(
        resultTableId,
        hdfsPath,
        scheduleTimeInHour
      )
      MDCLogger.logger(addRtn.toString(), kv = Map("rt_id" -> resultTableId))
    }
  }

}
