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

package com.tencent.bk.base.dataflow.spark.sql.topology.helper

import java.util.{Calendar, TimeZone}

import com.tencent.bk.base.dataflow.core.conf.ConstantVar
import ConstantVar.PeriodUnit
import com.tencent.bk.base.dataflow.spark.{BatchEnumerations, TimeFormatConverter, UCSparkConf}
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{HDFSPathConstructor, PathConstructor}
import com.tencent.bk.base.dataflow.spark.utils.APIUtil
import BatchEnumerations.Period
import com.tencent.bk.base.dataflow.spark.sql.parser.{CodeNodeJsonParamParser, SQLNodeJsonParamParser}
import com.tencent.bk.base.dataflow.spark.sql.parser.SQLNodeJsonParamParser
import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor
import com.tencent.bk.base.dataflow.spark.UCSparkConf

import scala.collection.JavaConverters._

object TopoBuilderScalaHelper {

  /**
   * @param processing_id processing_id
   * @param start 时间段开始时间
   * @param end 时间段结束时间
   * @param timestamp 调度时间
   * @param parentType 父表数据类型
   * @param period 周期单位
   * @param root 根目录地址
   * @param pathConstructor 构建路径的类 tdw和hdfs不一样
   * @param isManaged
   * @return 构建的路径以及时间范围
   */
  // scalastyle:off
  def buildWindowInputPathsAsJava(
    processing_id: String,
    start: Int,
    end: Int,
    timestamp: Long,
    parentType: String,
    period: PeriodUnit,
    root: String,
    pathConstructor: PathConstructor,
    isManaged: Boolean): (java.util.List[String], Long, Long) = {
    val scalaResult = buildWindowInputPaths(
      processing_id,
      start,
      end,
      timestamp,
      parentType,
      period,
      root,
      pathConstructor,
      isManaged)
    (scalaResult._1.asJava, scalaResult._2, scalaResult._3)
  }

  /**
   * 离线计算上下游关系：
   * 小时 -> 小时
   * 小时 -> 天
   * 小时 -> 周
   * 小时 -> 月
   * 天 -> 周
   * 天 -> 月
   * 周 -> 周
   * 月 -> 月
   * 当前表周期需是父表周期的整数倍，
   * 以下选择窗口逻辑基于不存在多个父表选不同窗口单位(例如一个选天一个选月)的情形，
   * 因为存在如果一个窗口选30天，一个窗口选1个月很难算出最小窗口是多少的情况，
   * 所以如下逻辑假设一个离线计算父表的窗口都使用同样窗口单位。
   */
  def buildWindowInputPaths(
    processing_id: String,
    start: Int,
    end: Int,
    timestamp: Long,
    parentType: String,
    period: PeriodUnit,
    root: String,
    pathConstructor: PathConstructor,
    isManaged: Boolean): (List[String], Long, Long) = {
    if (end < start)
      throw new Exception(s"wrong time format," +
        s"because end(${end}) < start(${start}).")
    val bootTimeMills = timestamp

    val hourToMills: Long = 3600 * 1000L

    BatchEnumerations.DataProcessingType.withName(parentType.toUpperCase()) match {
      case BatchEnumerations.DataProcessingType.STREAM => {
        period match {
          case PeriodUnit.hour => {
            val startTimeMills = bootTimeMills - end * hourToMills
            val endTimeMills = bootTimeMills - start * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.day => {
            val startTimeMills = bootTimeMills - end * 24 * hourToMills
            val endTimeMills = bootTimeMills - start * 24 * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.week => {
            val startTimeMills = bootTimeMills - end * 24 * 7 * hourToMills
            val endTimeMills = bootTimeMills - start * 24 * 7 * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.month => {
            val calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))
            calendar.setTimeInMillis(bootTimeMills)
            calendar.add(Calendar.MONTH, -end)
            val startTimeMills = calendar.getTimeInMillis
            calendar.setTimeInMillis(bootTimeMills)
            calendar.add(Calendar.MONTH, -start)
            val endTimeMills = calendar.getTimeInMillis
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
        }
      }
      case BatchEnumerations.DataProcessingType.BATCH if (!isManaged) => {
        period match {
          case PeriodUnit.hour => {
            val startTimeMills = bootTimeMills - end * hourToMills
            val endTimeMills = bootTimeMills - start * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.day => {
            val startTimeMills = bootTimeMills - end * 24 * hourToMills
            val endTimeMills = bootTimeMills - start * 24 * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.week => {
            val startTimeMills = bootTimeMills - end * 24 * 7 * hourToMills
            val endTimeMills = bootTimeMills - start * 24 * 7 * hourToMills
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.month => {
            val calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))
            calendar.setTimeInMillis(bootTimeMills)
            calendar.add(Calendar.MONTH, -end)
            val startTimeMills = calendar.getTimeInMillis
            calendar.setTimeInMillis(bootTimeMills)
            calendar.add(Calendar.MONTH, -start)
            val endTimeMills = calendar.getTimeInMillis
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
              startTimeMills,
              endTimeMills)
          }
        }
      }
      case BatchEnumerations.DataProcessingType.BATCH if (isManaged) => {
        val apiParams = APIUtil.fetchBKSQLFullParams(processing_id)

        val isCodeNode = apiParams.get("job_type") match {
          case Some(x) => x.asInstanceOf[String].equals(UCSparkConf.codeJobType)
          case _ => false
        }

        val parentArgs = if (isCodeNode) {
          new CodeNodeJsonParamParser(apiParams)
        } else {
          new SQLNodeJsonParamParser(processing_id, apiParams)
        }
        val windowTuple = parentArgs.getMinWindowSize()
        val parentPeriod = parentArgs.getSchedulePeriod()
        val windowSize = windowTuple._1.longValue()
        val windowUnit = windowTuple._2

        period match {
          case PeriodUnit.hour => {
            val startTimeMills: Long = bootTimeMills - (end + windowSize - 1) * hourToMills
            val endTimeMills: Long = bootTimeMills - (start + windowSize - 1) * hourToMills

            val startTimeStr = TimeFormatConverter.platformCTDateFormat.format(startTimeMills)
            val endTimeStr = TimeFormatConverter.platformCTDateFormat.format(endTimeMills)

            MDCLogger.logger(
              s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
            (pathConstructor.getAllTimePerHour(root, startTimeStr, endTimeStr, 1),
              TimeFormatConverter.platformCTDateFormat.parse(startTimeStr).getTime,
              TimeFormatConverter.platformCTDateFormat.parse(endTimeStr).getTime)
          }
          case PeriodUnit.day => {

            //不管统计频率多长,都按最小单位遍历数据
            val periodMills: Long = (if (parentPeriod.equals(Period.HOUR)) 1 else 24) * hourToMills

            val formatStart = if (parentPeriod.equals(Period.HOUR)) start * 24 else start
            val formatEnd = if (parentPeriod.equals(Period.HOUR)) end * 24 else end

            val startTimeMills: Long = bootTimeMills - (formatEnd - 1) * periodMills - windowSize * hourToMills
            val endTimeMills: Long = bootTimeMills - (formatStart - 1) * periodMills - windowSize * hourToMills

            MDCLogger.logger(
              s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
            val curInterval = if (parentPeriod.equals(Period.HOUR)) 1 else 24
            (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, curInterval),
              startTimeMills,
              endTimeMills)
          }
          case PeriodUnit.week => {
            parentPeriod match {
              case Period.HOUR => {
                val startTimeMills = bootTimeMills - end * 24 * 7 * hourToMills + hourToMills - windowSize * hourToMills
                val endTimeMills = bootTimeMills - start * 24 * 7 * hourToMills + hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
                  startTimeMills,
                  endTimeMills)
              }
              case Period.DAY => {
                val startTimeMills = bootTimeMills - end * 24 * 7 * hourToMills + 24 * hourToMills - windowSize * hourToMills
                val endTimeMills = bootTimeMills - start * 24 * 7 * hourToMills + 24 * hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 24),
                  startTimeMills,
                  endTimeMills)
              }
              case Period.WEEK => {
                val startTimeMills = bootTimeMills - end * 24 * 7 * hourToMills + 24 * 7 * hourToMills - windowSize * hourToMills
                val endTimeMills = bootTimeMills - start * 24 *7 * hourToMills + 24 * 7 * hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 24 * 7),
                  startTimeMills,
                  endTimeMills)
              }
              case _ =>
                throw new IllegalArgumentException(
                s"window config is not supported: window unit is week, parent count frequency is month")

            }
          }
          case PeriodUnit.month => {

            val calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))

            parentPeriod match {
              case Period.HOUR => {
                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -end)
                val startTimeMills = calendar.getTimeInMillis + hourToMills - windowSize * hourToMills

                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -start)
                val endTimeMills = calendar.getTimeInMillis + hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1),
                  startTimeMills,
                  endTimeMills)
              }
              case Period.DAY => {
                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -end)
                val startTimeMills = calendar.getTimeInMillis + 24 * hourToMills - windowSize * hourToMills

                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -start)
                val endTimeMills = calendar.getTimeInMillis + 24 * hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 24),
                  startTimeMills,
                  endTimeMills)
              }
              // week to month probably not supported in the production environment.
              case Period.WEEK => {
                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -end)
                val startTimeMills = calendar.getTimeInMillis + 24 * hourToMills - windowSize * hourToMills

                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, -start)
                val endTimeMills = calendar.getTimeInMillis + 24 * hourToMills - windowSize * hourToMills
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                (pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 24),
                  startTimeMills,
                  endTimeMills)
              }
              case Period.MONTH => { // parent period is month, parent window can only be month
                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, 1 - end - windowSize.toInt)
                val startTimeMills = calendar.getTimeInMillis

                calendar.setTimeInMillis(bootTimeMills)
                calendar.add(Calendar.MONTH, 1 - start - windowSize.toInt)
                val endTimeMills = calendar.getTimeInMillis
                MDCLogger.logger(
                  s"Input Result Table: ${processing_id}, start -> ${startTimeMills}, end -> ${endTimeMills}")
                val pathList = pathConstructor
                  .asInstanceOf[HDFSPathConstructor]
                  .getAllTimePerMonth(root, startTimeMills, endTimeMills)
                (pathList, startTimeMills, endTimeMills)
              }
            }
          }
        }
      }
    }
  }

  /**
   * @param start 时间段开始时间
   * @param end 时间段结束时间
   * @param timestamp 调度时间
   * @param root 根目录地址
   * @param pathConstructor 构建路径的类 tdw和hdfs不一样
   * @return 构建的路径以及时间范围
   */
  def buildAccInputPathsAsJava(
    start: Int,
    end: Int,
    timestamp: Long,
    root: String,
    pathConstructor: PathConstructor): (java.util.List[String], Long, Long) = {
    val scalaResult = buildAccInputPaths(
      start,
      end,
      timestamp,
      root,
      pathConstructor
    )
    (scalaResult._1.asJava, scalaResult._2, scalaResult._3)
  }

  def buildAccInputPaths(
    start: Int,
    end: Int,
    timestamp: Long,
    root: String,
    pathConstructor: PathConstructor): (List[String], Long, Long) = {
    /**
     * dataStart和dataEnd为数据时间
     */
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))
    calendar.setTimeInMillis(timestamp)

    /** 设置分钟和秒为0 */
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    if ((hour - 1 + 24) % 24 < start || (hour - 1 + 24) % 24 > end) {
      val errMsg = f"Not In Run Period, Exist.Expect Range ${(start + 1) % 24} " +
        f"-> ${(end + 1) % 24}, Now is $hour"
      MDCLogger.logger(errMsg)
      throw new BatchException(errMsg, ErrorCode.ILLEGAL_ARGUMENT)
    }
    if (hour == 0) {
      calendar.add(Calendar.DATE, -1)
    }
    calendar.set(Calendar.HOUR_OF_DAY, start)
    val startTimeMills = calendar.getTimeInMillis

    /** 计算结束时间 */
    calendar.setTimeInMillis(timestamp)
    calendar.set(Calendar.HOUR_OF_DAY, hour)

    val endTimeMills = calendar.getTimeInMillis
    val paths = pathConstructor.getAllTimePerHour(root, startTimeMills, endTimeMills, 1)
    (paths, startTimeMills, endTimeMills)
  }

  def getDtEventTimeInMs(
      timestamp: Long, windowSize: Long, windowUnit: ConstantVar.PeriodUnit): Long = {
    val startTimeMills = windowUnit match {
      case PeriodUnit.month => {
        val calendar =
          Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))
        calendar.setTimeInMillis(timestamp)
        calendar.add(Calendar.MONTH, -windowSize.toInt)
        calendar.getTimeInMillis
      }
      case _ => timestamp - windowSize * 3600 * 1000L
    }
    startTimeMills
  }
}
