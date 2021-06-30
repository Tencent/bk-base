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

package com.tencent.bk.base.dataflow.spark.topology

import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util

import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.sql.parquet.SparkParquetNumRow
import com.tencent.bk.base.dataflow.spark.topology.nodes.SupportIcebergCount
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil
import com.tencent.bk.base.datahub.iceberg.SparkUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.ignite.internal.binary.BinaryObjectImpl
import org.apache.ignite.spark.JavaIgniteRDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

abstract class DataFrameWrapper(
    dataframe: DataFrame,
    resultTableId: String) {
  def count(): Long

  def makeTempView(): Unit = {
    val tableName = CommonUtil.formatTableName(this.resultTableId)
    dataframe.createOrReplaceTempView(tableName)
  }

  def getDataFrame(): DataFrame = this.dataframe


  def getResultTableId(): String = this.resultTableId

  def isEmpty(): Boolean = {
    dataframe.take(1).isEmpty
  }
}

case class DefaultDataFrameWrapper(
    spark: SparkSession,
    dataframe: DataFrame,
    resultTableId: String)
  extends DataFrameWrapper(dataframe, resultTableId) {
  override def count(): Long = {
    dataframe.count()
  }
}

case class HDFSDataFrameWrapper(
    spark: SparkSession,
    dataframe: DataFrame,
    resultTableId: String,
    pathList: Seq[FileStatus])
  extends DataFrameWrapper(
    dataframe: DataFrame,
    resultTableId: String) {

  var rowCount: Long = -1

  override def count(): Long = {
    if (rowCount <= 0) {
      val hasJsonFile = pathList.map(_.getPath.toString).find(_.endsWith(".json")) match {
        case Some(path: String) => true
        case _ => false
      }
      val dataType = if (hasJsonFile) "json" else "parquet"
      val rowNum = if (hasJsonFile) {
        Try(dataframe.javaRDD.count()) match {
          case Success(result) => result
          case Failure(e) =>
            val errMsg = "Failure to check the calculation results," + e.getMessage
            throw new BatchException(errMsg, ErrorCode.BATCH_JOB_ERROR)
        }
      } else {
        SparkParquetNumRow.doComputeStatsForParquetRowNum(spark, pathList)
      }
      MDCLogger.logger(
        s"getInputRowNum -> ${rowNum}",
        kv = Map("parent_rt_id" -> resultTableId,
          "data_type" -> dataType))
      this.rowCount = rowNum
    }
    this.rowCount
  }
}

case class IgniteDataFrameWrapper(
    spark: SparkSession,
    dataframe: DataFrame,
    originRdd: JavaIgniteRDD[Object, BinaryObjectImpl],
    resultTableId: String)
  extends DataFrameWrapper(
    dataframe: DataFrame,
    resultTableId: String) {

  override def count(): Long = {
    originRdd.count()
  }
}

case class IcebergDataFrameWrapper(
    spark: SparkSession,
    icebergConf: util.Map[java.lang.String, Object],
    icebergTableName: String,
    supportIcebergCount: SupportIcebergCount,
    dataframe: DataFrame,
    resultTableId: String)
  extends DataFrameWrapper(
    dataframe,
    resultTableId) {

  var rowCount: Long = -1

  override def count(): Long = {
    if (this.rowCount <= 0) {
      if (supportIcebergCount.isIcebergOptimizedCountEnable) {
        if (!supportIcebergCount.isCountTotalValue) {
          import collection.JavaConverters._
          val timeRangeListCount = supportIcebergCount.getTimeRangeList.asScala.map(
            value => {
              countValue(value.getStartTime, value.getEndTime)
            }
          ).sum

          this.rowCount = timeRangeListCount
        } else {
          this.rowCount = SparkUtils.calcTableRecordCount(icebergTableName, icebergConf)
        }
      } else {
        this.rowCount = dataframe.count()
      }
    }
    this.rowCount
  }

  // At least count one hour data
  private def countValue(time: Long): Long = {
    this.countValue(time, time + 3600 * 1000L)
  }

  private def countValue(startTime: Long, endTime: Long): Long = {
    val startInstant = Instant.ofEpochMilli(startTime)
    val endInstant = Instant.ofEpochMilli(endTime)
    val startOffsetDateTime =
      OffsetDateTime.ofInstant(startInstant, ZoneId.of(UCSparkConf.dataTimezone))
    val endOffsetDateTime =
      OffsetDateTime.ofInstant(endInstant, ZoneId.of(UCSparkConf.dataTimezone))
    SparkUtils.calcRecordCount(
      icebergTableName,
      icebergConf,
      startOffsetDateTime,
      endOffsetDateTime)
  }
}

case class ExternalCountDataFrameWrapper(
    dataframe: DataFrame,
    rowCount: Long,
    resultTableId: String)
  extends DataFrameWrapper(
    dataframe: DataFrame,
    resultTableId: String) {

  override def count(): Long = {
    this.rowCount
  }
}