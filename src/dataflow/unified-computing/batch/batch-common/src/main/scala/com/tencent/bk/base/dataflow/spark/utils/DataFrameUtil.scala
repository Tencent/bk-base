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

package com.tencent.bk.base.dataflow.spark.utils

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import com.tencent.bk.base.dataflow.core.topo.NodeField
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.sql.BatchJavaEnumerations.{IcebergReservedField, ReservedField}
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period.Period
import MDCLogger.Level
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DataFrameUtil {
  // scalastyle:off
  def str2DataType(typeStr: String): DataType = {
    typeStr.toLowerCase match {
      // String类型
      case "byte" => ByteType
      case "varchar" | "string" => StringType
      // 数值类型
      case "float" => FloatType
      case "double" => DoubleType
      case "short" => ShortType
      case "int" | "integer" => IntegerType
      case "bigint" | "long" => LongType
      case "timestamp" => TimestampType // 时间戳
      case "boolean" => BooleanType // 布尔
      case "binary" => BinaryType // 二进制
      case "null" => NullType // 空值
      case other => StringType
    }
  }
  // scalastyle:on


  def mergeSchema(schema: StructType, enableIcebergSupport: Boolean = false): StructType = {
    val structFields = schema.fields.toBuffer
    val reservedFieldsAndType = if (enableIcebergSupport) {
      IcebergReservedField.values().map(x => {
        x.name() -> str2DataType(x.getFieldType)
      })
    } else {
      ReservedField.values().map(x => {
        x.name() -> str2DataType(x.getFieldType)
      })
    }
    for (index <- 0 until reservedFieldsAndType.size) {
      val fieldNames = structFields.map(_.name)
      val (reservedFieldName, reservedFieldType) = reservedFieldsAndType.toList.apply(index)
      val fieldIndex = fieldNames.indexOf(reservedFieldName)
      if (fieldNames.contains(reservedFieldName) && fieldIndex != index) {
        structFields.remove(fieldIndex)
      }
      structFields.insert(index, StructField(reservedFieldName, reservedFieldType, true))
    }

    val finalFields = if (enableIcebergSupport) {
      structFields.map{
        value => StructField(value.name.toLowerCase(), value.dataType, value.nullable, value.metadata)
      }
    } else {
      structFields
    }

    MDCLogger.logger(s"structFields-> ${finalFields.toList.toString()}")
    StructType(finalFields)
  }

  /**
   * 检查保留字段
   *
   * @param lb                数据
   * @param fieldOrdersBuffer 字段顺序buffer
   * @param timeStamp         数据起始时间
   */
  // scalastyle:off
  def checkReservedField(
      lb: ListBuffer[Any],
      fieldOrdersBuffer: mutable.Buffer[String],
      timeStamp: Long,
      period: Period = Period.HOUR,
      enableIcebergSupport: Boolean = false): Unit = {
    /**
     * 按照保留字段顺序对数据排序
     * 没有保留字段插入墙上时间，有保留字段使用数据本身时间
     **/
    val reservedFieldsValues = if (enableIcebergSupport) {
      IcebergReservedField.values()
    } else {
      ReservedField.values()
    }
    for (reservedField <- reservedFieldsValues) {
      val fieldIndex = fieldOrdersBuffer.indexOf(reservedField.name())
      if (fieldIndex != reservedField.ordinal()) {
        if (fieldIndex != -1) {
          fieldOrdersBuffer.remove(fieldIndex)
          lb.insert(reservedField.ordinal(), lb.remove(fieldIndex))
        } else {
          val localTimeName = if (enableIcebergSupport)
            IcebergReservedField.localtime.name()
          else
            ReservedField.localTime.name()
          /** 如果是localtime字段，而且localtime字段在数据中不存在需要插入，那么插入本地时间 */
          val toInsertTimeStamp = if (Seq(localTimeName).indexOf(reservedField.name()) != -1) {
            System.currentTimeMillis()
          } else {
            timeStamp
          }
          lb.insert(reservedField.ordinal(), toInsertTimeStamp)
        }
        fieldOrdersBuffer.insert(reservedField.ordinal(), reservedField.name())
      }
    }
    val dtEventTimeStampOrdinal = if (enableIcebergSupport)
      IcebergReservedField.dteventtimestamp.ordinal()
    else
      ReservedField.dtEventTimeStamp.ordinal()

    val thedateOrdinal = if (enableIcebergSupport)
      IcebergReservedField.thedate.ordinal()
    else
      ReservedField.thedate.ordinal()

    val dtEventTimeOrdinal = if (enableIcebergSupport)
      IcebergReservedField.dteventtime.ordinal()
    else
      ReservedField.dtEventTime.ordinal()

    val localTimeOrdinal = if (enableIcebergSupport)
      IcebergReservedField.localtime.ordinal()
    else
      ReservedField.localTime.ordinal()
    /**
     * 格式处理：
     * 在数据已经按找格式排好顺序之后，
     * 特殊处理thedate、dtEventTime、dtEventTimeStamp和localTime。
     **/
    val dtEventTimeStamp = lb.apply(dtEventTimeStampOrdinal)
    val localTime = lb.apply(localTimeOrdinal)

    //thedate
    val thedate = lb.apply(thedateOrdinal)
    val date = if (thedate.isInstanceOf[Long])
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(thedate.toString.toLong), ZoneId.of(TimeFormatConverter.dataTimeZone))
    else if (thedate.isInstanceOf[Int])
    // LocalDateTime.parse(thedate.toString, theDateFormatter)
      LocalDateTime.ofInstant(
        LocalDate.parse(thedate.toString, TimeFormatConverter.theDateFormatter)
          .atStartOfDay(ZoneId.of(TimeFormatConverter.dataTimeZone))
          .toInstant,
        ZoneId.of(TimeFormatConverter.dataTimeZone))
    else if (thedate != null)
    // LocalDateTime.parse(thedate.toString, formatter)
      LocalDateTime.ofInstant(
        LocalDate.parse(thedate.toString, TimeFormatConverter.formatter)
          .atStartOfDay(ZoneId.of(TimeFormatConverter.dataTimeZone))
          .toInstant,
        ZoneId.of(TimeFormatConverter.dataTimeZone))
    else
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(dtEventTimeStamp.toString.toLong),
        ZoneId.of(TimeFormatConverter.dataTimeZone))

    //dtEventTime
    val dtEventTime = lb.apply(dtEventTimeOrdinal)
    val dtEventTimeDate = if (dtEventTime.isInstanceOf[Long])
      LocalDateTime.ofInstant(Instant.ofEpochMilli(dtEventTime.toString.toLong),
        ZoneId.of(TimeFormatConverter.dataTimeZone))
    else
      LocalDateTime.parse(dtEventTime.toString, TimeFormatConverter.formatter)
    val dtEventTimeFormatter = if (period.equals(Period.DAY)) {
      TimeFormatConverter.dayFormatter
    } else {
      TimeFormatConverter.hourFormatter
    }

    //dtEventTimeStamp
    //lb.update(ReservedField.dtEventTimeStamp.ordinal(), dtEventTimeStamp)

    //localTime
    val localTimeDate = if (localTime.isInstanceOf[Long])
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(localTime.toString.toLong), ZoneId.of(TimeFormatConverter.dataTimeZone))
    else
      LocalDateTime.parse(localTime.toString, TimeFormatConverter.formatter)

    lb.update(thedateOrdinal, date.format(TimeFormatConverter.theDateFormatter).toInt)
    lb.update(dtEventTimeOrdinal, dtEventTimeDate.format(dtEventTimeFormatter))
    lb.update(localTimeOrdinal, localTimeDate.format(TimeFormatConverter.formatter))
  }
  // scalastyle:on


  /**
   * 类型转换，使存储类型和元数据类型一致<br/>
   * 和bksql解析类型兼容
   * @param df
   * @return
   */
  // scalastyle:off
  def convertType(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.col
    // 转换规则需要和BKsql一致
    val columns = df.schema.fields
      .map(f => f.dataType match {
        case StringType | IntegerType | LongType | FloatType | DoubleType | TimestampType=> col(f.name)
        case ByteType | ShortType => col(f.name).cast(IntegerType)
        case v: DecimalType if (v.scale == 0 && v.precision <= 20)  => col(f.name).cast(LongType)
        case v: DecimalType if (v.scale > 0  && v.precision <= 30) => col(f.name).cast(DoubleType)
        case v: DecimalType => {
          throw new BatchException(
            s"""The column "${f.name}" unsupported return ${v.typeName} type, requiring cast to target type.""",
            ErrorCode.BATCH_SPARK_UNCATCH_ERROR)
        }
        case BooleanType | BinaryType |
             ArrayType(_, _) | MapType(_, _, _)=> throw new BatchException(
          s"""The column "${f.name}" unsupported return ${f.dataType.typeName} type, requiring cast to target type.""",
          ErrorCode.BATCH_SPARK_UNCATCH_ERROR)
        case _ => col(f.name).cast(StringType)
      })
    df.select(columns: _*)
  }
  // scalastyle:on

  def buildSchema(
      fields: mutable.Buffer[NodeField], excludeReservedField: Boolean = false): StructType = {
    val sf: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    for (field <- fields) {
      sf.append(StructField(field.getField, str2DataType(field.getType), true))

    }

    val schema = if (!excludeReservedField) {
      StructType(sf)
        .add("thedate", IntegerType, true)
        .add("dtEventTime", StringType, true)
        .add("dtEventTimeStamp", LongType, true)
        .add("localTime", StringType, true)
    } else {
      StructType(sf)
    }

    MDCLogger.logger(s"Build schema-> $schema")
    schema
  }

  // scalastyle:off
  def optimizedMakeDataframe(
    spark: SparkSession,
    paths: List[String],
    schema: StructType,
    filesBuff:ListBuffer[FileStatus] = new ListBuffer[FileStatus]): DataFrame = {
    val parquetListBuffer = new ListBuffer[String]
    val jsonListBuffer = new ListBuffer[String]
    var isParquet = false;
    var isJson = false;

    for (path <- paths) {
      if (HadoopUtil.isDirectory(path)) {
        val files = HadoopUtil.listStatus(path)
        isParquet = files.map(_.getPath.getName).exists(_.endsWith(".parquet"))
        isJson = files.map(_.getPath.getName).exists(_.endsWith(".json"))
        if (isParquet) {
          parquetListBuffer.append(path)
          MDCLogger.logger(s"Added parquet path for dataframe path buffer: $path")
          filesBuff.append(files: _*)
        } else if (isJson) {
          jsonListBuffer.append(path)
          MDCLogger.logger(s"Added json path for dataframe path buffer: $path")
          filesBuff.append(files: _*)
        }
        try {
          HadoopUtil.createNewFile(s"${path}/_READ")
        } catch {
          case e: Exception => MDCLogger.logger(s"${e}", level = Level.WARN)
        }
      }
    }

    val parquetDf = if (parquetListBuffer.nonEmpty) {
      MDCLogger.logger(s"Found total ${parquetListBuffer.size} path with parquet files")
      if (schema != null)
        spark.read.schema(schema).parquet(parquetListBuffer: _*)
      else
        spark.read.parquet(parquetListBuffer: _*)
    } else {
      spark.emptyDataFrame
    }

    val jsonDf = if (jsonListBuffer.nonEmpty) {
      MDCLogger.logger(s"Found total ${jsonListBuffer.size} path with json files")
      if (schema != null)
        spark.read.schema(schema).json(jsonListBuffer: _*)
      else
        spark.read.json(jsonListBuffer: _*)
    } else {
      spark.emptyDataFrame
    }

    if (parquetListBuffer.nonEmpty && jsonListBuffer.nonEmpty) {
      import org.apache.spark.sql.functions.col
      val columns = parquetDf.columns.map(col)
      parquetDf.select(columns: _*).unionAll(jsonDf.select(columns: _*))
    } else if(parquetListBuffer.nonEmpty && jsonListBuffer.isEmpty) {
      parquetDf
    } else if(parquetListBuffer.isEmpty && jsonListBuffer.nonEmpty) {
      jsonDf
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
  }
  // scalastyle:on
}
