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

import java.util
import java.util.{Calendar, Map, TimeZone}

import com.tencent.bk.base.dataflow.spark.{TimeFormatConverter, UCSparkConf}
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.utils.{APIUtil, CommonUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait PathConstructor {

  def getAllTimePerHour(
    fsPath: String, startTime: String, endTime: String, timeInterval: Int): List[String]

  def getAllTimePerHour(
    fsPath: String, startTimeMills: Long, endTimeMills: Long, timeInterval: Int): List[String]

  def getAllTimePerHour(args: Array[String]): List[String]

  def getAllTimePerHourByRollBackWindow(
    fsPath: String, bootTimeMills: Long, rollbackWindow: Int, timeInterval: Int): List[String]

  def timeToPath(root: String, timestamp: Long): String

  def getAllTimePerHourAsJava(
      fsPath: String,
      startTime: String,
      endTime: String,
      timeInterval: Int): util.List[String] = {
    this.getAllTimePerHour(fsPath, startTime, endTime, timeInterval).asJava
  }

  def getAllTimePerHourAsJava(
      fsPath: String,
      startTimeMills: Long,
      endTimeMills: Long,
      timeInterval: Int): util.List[String] = {
    this.getAllTimePerHour(fsPath, startTimeMills, endTimeMills, timeInterval).asJava
  }

  def getAllTimePerHourAsJava(args: Array[String]): util.List[String] = {
    this.getAllTimePerHour(args).asJava
  }

  def getAllTimePerHourByRollBackWindowAsJava(
      fsPath: String,
      bootTimeMills: Long,
      rollbackWindow: Int,
      timeInterval: Int): util.List[String] = {
    this.getAllTimePerHourByRollBackWindow(
      fsPath,
      bootTimeMills,
      rollbackWindow,
      timeInterval).asJava
  }
}

class HDFSPathConstructor extends PathConstructor {

  /**
   *
   * @param fsPath       :hdfs://${hdfs_address}/kafka/data/
   * @param startTime    : load data from the date,timeformat yyyyMMddHH
   * @param endTime      : ent time,not include this time
   * @param timeInterval ;time interval
   */
  def getAllTimePerHour(
      fsPath: String,
      startTime: String,
      endTime: String,
      timeInterval: Int): List[String] = {
    val startTimeMills = TimeFormatConverter.platformCTDateFormat.parse(startTime).getTime
    val endTimeMills = TimeFormatConverter.platformCTDateFormat.parse(endTime).getTime
    getAllTimePerHour(fsPath, startTimeMills, endTimeMills, timeInterval)
  }

  def getAllTimePerHour(
      fsPath: String,
      startTimeMills: Long,
      endTimeMills: Long,
      timeInterval: Int): List[String] = {
    val pathBuf = new ListBuffer[String]
    val interval = timeInterval * 3600 * 1000L
    val formattedStartTimeMills = TimeFormatConverter.dataCTDateFormat.format(startTimeMills)
    val formattedEndTimeMills = TimeFormatConverter.dataCTDateFormat.format(endTimeMills)
    MDCLogger.logger(
      f"HDFSPathParser -> from ${formattedStartTimeMills} " +
      f"until ${formattedEndTimeMills} step by ${interval / 3600000}hour")
    for (currentTimeMills <- startTimeMills until endTimeMills by interval) {
      pathBuf.append(timeToPath(fsPath, currentTimeMills))
    }
    pathBuf.toList
  }

  def getAllTimePerHourToEnd(
      fsPath: String,
      startTime: String,
      endTime: String,
      timeInterval: Int): List[String] = {
    val startTimeMills = TimeFormatConverter.platformCTDateFormat.parse(startTime).getTime
    val endTimeMills = TimeFormatConverter.platformCTDateFormat.parse(endTime).getTime
    getAllTimePerHourToEnd(fsPath, startTimeMills, endTimeMills, timeInterval)
  }

  def getAllTimePerHourToEnd(
      fsPath: String,
      startTimeMills: Long,
      endTimeMills: Long,
      timeInterval: Int): List[String] = {
    val pathBuf = new ListBuffer[String]
    val interval = timeInterval * 3600 * 1000L
    val formattedStartTimeMills = TimeFormatConverter.dataCTDateFormat.format(startTimeMills)
    val formattedEndTimeMills = TimeFormatConverter.dataCTDateFormat.format(endTimeMills)
    MDCLogger.logger(f"HDFSPathParser -> from ${formattedStartTimeMills} " +
      f"until ${formattedEndTimeMills} step by ${interval / 3600000}hour")
    for (currentTimeMills <- startTimeMills to endTimeMills by interval) {
      pathBuf.append(timeToPath(fsPath, currentTimeMills))
    }
    pathBuf.toList
  }

  def getAllTimePerHourToEndAsJava(
      fsPath: String,
      startTimeMills: Long,
      endTimeMills: Long,
      timeInterval: Int): util.List[String] = {
    getAllTimePerHourToEnd(fsPath, startTimeMills, endTimeMills, timeInterval).asJava
  }

  def getAllTimePerHourToEndAsJava(
      fsPath: String,
      startTime: String,
      endTime: String,
      timeInterval: Int): util.List[String] = {
    getAllTimePerHourToEnd(fsPath, startTime, endTime, timeInterval).asJava
  }

  def getAllTimePerHour(args: Array[String]): List[String] = {
    getAllTimePerHour(args(0), args(1), args(2), 1)
  }

  def getAllTimePerMonth(fsPath: String, startTimeMills: Long, endTimeMills: Long): List[String] = {
    val pathBuf = new ListBuffer[String]
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeFormatConverter.platformTimeZone))
    calendar.setTimeInMillis(startTimeMills)
    var calendarTimeStamp = calendar.getTimeInMillis
    while(calendarTimeStamp < endTimeMills) {
      MDCLogger.logger(s"Added in timestamp $calendarTimeStamp for path")
      pathBuf.append(timeToPath(fsPath, calendarTimeStamp))
      calendar.add(Calendar.MONTH, 1)
      calendarTimeStamp = calendar.getTimeInMillis
    }
    pathBuf.toList
  }

  def timeToPath(fsPath: String, currentTimeMills: Long): String = {
    val cutSlashRootLoc = if (fsPath.endsWith("/")) {
      fsPath.substring(0, fsPath.length - 1)
    } else {
      fsPath
    }
    val hdfsPath = cutSlashRootLoc + TimeFormatConverter.pathDateFormat.format(currentTimeMills)
    MDCLogger.logger(f"HDFSPathParser ->  ${hdfsPath}")
    hdfsPath
  }

  def timeToPath(fsPath: String, currentTime: String): String = {
    val currentTimeMills = TimeFormatConverter.platformCTDateFormat.parse(currentTime).getTime
    timeToPath(fsPath, currentTimeMills)
  }

  /**
   *
   * @param fsPath         : hdfs://${hdfs_address}/kafka/data/
   * @param bootTimeMills  : long,
   * @param rollbackWindow : roll back hours
   */
  def getAllTimePerHourByRollBackWindow(
      fsPath: String,
      bootTimeMills: Long,
      rollbackWindow: Int,
      timeInterval: Int): List[String] = {
    val startTimeMills = bootTimeMills - rollbackWindow * 3600 * 1000L
    getAllTimePerHour(fsPath, startTimeMills, bootTimeMills, timeInterval)
  }

  def getHdfsRoot(rt_id: String): String = {
    val nameService = getHdfsNameService(rt_id)
    s"$nameService/${APIUtil.getHDFSPath(rt_id)}"
  }

  def getHdfsNameService(rt_id: String): String = {
    val (_, clusterName) = APIUtil.getRTCluster(rt_id)
    APIUtil.getClusterNameService("hdfs", clusterName)
  }

  def getHDFSCopyRoot(rt_id: String): String = {
    val nameService = getHdfsNameService(rt_id)
    val ownerNum = CommonUtil.getRtOwnerNum(rt_id)
    s"${nameService}/${UCSparkConf.copyPathPrefix}/${ownerNum}/$rt_id"
  }

  def getHdfsNameService(storageInfo: Map[String, String]): String = {
    storageInfo.get("name_service")
  }

  def getHDFSCopyRoot(rt_id: String, storageInfo: Map[String, String]): String = {
    val nameService = getHdfsNameService(storageInfo)
    val ownerNum = CommonUtil.getRtOwnerNum(rt_id)
    s"${nameService}/${UCSparkConf.copyPathPrefix}/${ownerNum}/$rt_id"
  }

  def getHdfsRoot(storageInfo: Map[String, String]): String = {
    val nameService = getHdfsNameService(storageInfo)

    // 如果hdfs存储格式为iceberg情况下，physical_table_name将不再是路径而是iceberg表名，原有hdfs路径将为hdfs_root_path
    val physicalName =
      if (null != storageInfo.get("data_type")
        && storageInfo.get("data_type").toLowerCase == "iceberg") {
      storageInfo.get("hdfs_root_path")
    } else {
      storageInfo.get("physical_table_name")
    }
    s"$nameService$physicalName"
  }


}
