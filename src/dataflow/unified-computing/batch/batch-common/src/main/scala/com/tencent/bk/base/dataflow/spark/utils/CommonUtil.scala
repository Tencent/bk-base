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

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.tencent.bk.base.dataflow.spark.TimeFormatConverter
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CommonUtil {

  object LogHolder extends Serializable {
    @transient lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  }

  /**
    * 使用这个方法将Optin[T] -> T
    * 如果option为None直接抛异常
    */
  def getOptVal[T](option: Option[T]): T = {
    option match {
      case Some(a: T) => a != null match {
        case true => a.asInstanceOf[T]
        case false => {
          val errMsg = "value is null"
          throw new BatchException("value is null", ErrorCode.ILLEGAL_ARGUMENT)
        }
      }
      case None => {
        val errMsg = s"Optional format Exception! : ${option.toString}"
        throw new BatchException(
          s"Optional format Exception! : ${option.toString}", ErrorCode.ILLEGAL_ARGUMENT)
      }
    }
  }

  def getDaysDiff(start: Long, end: Long): Long = {
    val dayFormat = new SimpleDateFormat("yyyyMMdd")
    val startStr = dayFormat.format(start)
    val endStr = dayFormat.format(end)
    val daysDiffMillis: Long = dayFormat.parse(endStr).getTime - dayFormat.parse(startStr).getTime
    val daysDiff: Long = daysDiffMillis / (24 * 3600 * 1000)
    daysDiff
  }

  def getRtOwnerNum(rt_id: String): String = {
    if (rt_id != null && rt_id.length != 0) {
      rt_id.split("_", 2)(0)
    } else {
      throw new IllegalArgumentException(
        "Failed to get Rt business number because of wrong Rt name format")
    }
  }

  def formatTableName(rtName: String): String = {
    rtName.substring(rtName.indexOf("_") + 1) + "_" + rtName.substring(0, rtName.indexOf("_"))
  }

  def roundScheduleTimeStampToHour(scheduleTime: Long): Long = {
    val dateTime =
      new DateTime(scheduleTime, DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai")))
    TimeFormatConverter.dateFormatDefault.parse(dateTime.toString("yyyyMMddHH")).getTime
  }

  // to convert nested scala collection to a nested java collection
  def collectionToJava(x: Any): Any = {
    import scala.collection.JavaConverters._
    x match {
      case y: scala.collection.MapLike[_, _, _] =>
        y.map { case (d, v) => collectionToJava(d) -> collectionToJava(v) } asJava
      case y: scala.collection.SetLike[_,_] =>
        y map { item: Any => collectionToJava(item) } asJava
      case y: mutable.Buffer[_] =>
        y map { item: Any => collectionToJava(item) } asJava
      case y: List[_] =>
        y map { item: Any => collectionToJava(item) } asJava
      case y: Iterable[_] =>
        y.map { item: Any => collectionToJava(item) } asJava
      case y: Iterator[_] =>
        collectionToJava(y.toIterable)
      case _ =>
        x
    }
  }

  def formatNumberValueToLong(value: Any): Any = {
    value match {
      case n: Number => n.longValue
      case _ => value
    }
  }
}
