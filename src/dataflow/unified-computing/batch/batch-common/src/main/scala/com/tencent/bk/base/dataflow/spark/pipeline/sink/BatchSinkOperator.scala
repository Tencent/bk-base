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

import com.tencent.bk.base.dataflow.core.sink.AbstractSink
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import com.tencent.bk.base.dataflow.spark.topology.nodes.{BatchIcebergOutput, BatchSinglePathOutput}
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode
import com.tencent.bk.base.datahub.iceberg.SparkUtils
import com.tencent.bk.base.dataflow.spark.sql.BatchJavaEnumerations.{IcebergReservedField, ReservedField, StartEndTimeField}
import com.tencent.bk.base.dataflow.spark.utils.{DataFrameUtil, HadoopUtil}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer

abstract class BatchSinkOperator(
    node: AbstractBatchSinkNode,
    sourceDataMap: util.Map[String, DataFrameWrapper],
    transformDataMap: util.Map[String, DataFrameWrapper],
    sinkDataMap: util.Map[String, DataFrameWrapper]) extends AbstractSink {
  MDCLogger.logger(s"Initial ${this.getClass.getSimpleName} for ${node.getNodeId}")

  protected def getInputDataFrameWrapper(resultTableId: String): DataFrameWrapper = {
    if (this.transformDataMap.get(resultTableId) != null) {
      MDCLogger.logger(s"${this.getClass.getSimpleName} get dataframe from transform data map")
      this.transformDataMap.get(resultTableId)
    }
    else {
      MDCLogger.logger(s"${this.getClass.getSimpleName} get dataframe from source data map")
      this.sourceDataMap.get(resultTableId)
    }
  }

  protected def createSinkDataFrame(
      df: DataFrame,
      dtEventTimeStamp: Long,
      spark: SparkSession,
      enableIcebergSupport: Boolean = false): DataFrame = {
    val tmpDf = if (node.isReservedFieldEnabled) {
      val dfAfterDrop = if (node.isForceUpdateReservedSchema) {
        MDCLogger.logger(s"Start to drop old reserved field if exists")
        this.dropReservedColumn(df, enableIcebergSupport = enableIcebergSupport)
      } else {
        df
      }
      val fieldOrdersArray = dfAfterDrop.schema.fields.map(_.name)
      val dtEventTime = dtEventTimeStamp
      MDCLogger.logger(s"Start to add reserved field, dtEventTime is $dtEventTime")
      val newRdd = dfAfterDrop.rdd.map(x => {
        val lb = new ListBuffer[Any]
        lb.appendAll(x.toSeq)
        // Todo: checkReservedField need period value?
        DataFrameUtil.checkReservedField(
          lb, fieldOrdersArray.toBuffer, dtEventTime, enableIcebergSupport = enableIcebergSupport)
        lb.toList
      }).map(Row.fromSeq(_))
      val newSchema = DataFrameUtil.mergeSchema(dfAfterDrop.schema, enableIcebergSupport)
      // 类型转换，使存储类型和元数据类型一致。
      spark.createDataFrame(newRdd, newSchema)
    } else {
      if (enableIcebergSupport) {
        val fieldNames = df.schema.fieldNames
        var lowercaseDf = df
        for(name <- fieldNames) {
          lowercaseDf = lowercaseDf.withColumnRenamed(name, name.toLowerCase)
        }
        MDCLogger.logger(s"Iceberg df with schema ${lowercaseDf.schema}")
        lowercaseDf
      } else {
        df
      }
    }

    val dfAfterStartEndTime = if (this.node.isEnableStartEndTimeField) {
      addStartEndTimeColumn(tmpDf)
    } else {
      tmpDf
    }

    val finalDf = DataFrameUtil.convertType(dfAfterStartEndTime)
    finalDf
  }

  private def addStartEndTimeColumn(df: DataFrame): DataFrame = {
    MDCLogger.logger(
      s"Start to add starttime(${this.node.getStartTimeString}), " +
        s"endtime(${this.node.getEndTimeString})")
    var resultDf = df
    for (field <- StartEndTimeField.values()) {
      resultDf = resultDf.drop(field.name())
    }

    resultDf = resultDf.withColumn(
      StartEndTimeField._starttime_.name(), lit(this.node.getStartTimeString).cast(StringType))
    resultDf = resultDf.withColumn(
      StartEndTimeField._endtime_.name(), lit(this.node.getEndTimeString).cast(StringType))
    resultDf
  }

  private def dropReservedColumn(
      df: DataFrame,
      enableIcebergSupport: Boolean = false): DataFrame = {
    var resultDf = df
    val reservedFields = if (enableIcebergSupport) {
      IcebergReservedField.values()
    } else {
      ReservedField.values()
    }
    for (reservedField <- reservedFields) {
      resultDf = resultDf.drop(reservedField.name())
    }
    resultDf
  }

  protected def saveIcebergDataFrame(
    spark: SparkSession,
    df: DataFrame,
    resultTableId: String,
    icebergOutput: BatchIcebergOutput,
    storageConf: util.Map[java.lang.String, Object]): Unit = {
    val saveMode = icebergOutput.getMode.toLowerCase() match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _ => throw new BatchException(
        s"find unsupported save format for iceberg", ErrorCode.BATCH_JOB_ERROR);
    }
    MDCLogger.logger(s"Save dataframe $resultTableId by using iceberg in $saveMode mode")
    MDCLogger.logger(df.schema.treeString)
    MDCLogger.logger(s"Iceberg hdfs conf -> ${storageConf.toString}")
    SparkUtils writeTable(icebergOutput.getOutputInfo, storageConf, df, saveMode)
  }

  protected def saveHdfsDataFrame(
      df: DataFrame,
      resultTableId: String,
      hdfsOutput: BatchSinglePathOutput): String = {
    // 从storageArgs获取存储格式，支持parquet、json。
    val dataType = hdfsOutput.getFormat
    val hdfsPath = hdfsOutput.getOutputInfo
    MDCLogger.logger(
      (s"saveDataFrameToHDFS -> ${dataType}"), kv = Map("ri_id" -> resultTableId))
    HadoopUtil.saveDataFrameToHDFS(df, hdfsPath, resultTableId, dataType, hdfsOutput.getMode)
    hdfsPath
  }
}
