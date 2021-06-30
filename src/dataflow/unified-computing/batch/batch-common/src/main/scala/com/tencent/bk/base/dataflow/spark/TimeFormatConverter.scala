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

package com.tencent.bk.base.dataflow.spark

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object TimeFormatConverter {
  val dataTimeZone = UCSparkConf.dataTimezone

  val platformTimeZone = UCSparkConf.platformTimezone

  val pathDateFormat = new SimpleDateFormat("/yyyy/MM/dd/HH") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone))
    }
  }

  val platformCTDateFormat = new SimpleDateFormat("yyyyMMddHH") {
    {
      setTimeZone(TimeZone.getTimeZone(platformTimeZone));
    }
  }

  val dataCTDateFormat = new SimpleDateFormat("yyyyMMddHH") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone));
    }
  }

  val dateFormatDefault = new SimpleDateFormat("yyyyMMddHH") {
    {
      setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }
  }

  val icebergFilterTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone));
    }
  }

  val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone));
    }
  }
  val sdf2Hour = new SimpleDateFormat("yyyy-MM-dd HH:00:00") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone));
    }
  }
  val sdf2Day = new SimpleDateFormat("yyyy-MM-dd 00:00:00") {
    {
      setTimeZone(TimeZone.getTimeZone(dataTimeZone));
    }
  }

  val formatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(TimeZone.getTimeZone(dataTimeZone).toZoneId)
  val theDateFormatter = DateTimeFormatter
    .ofPattern("yyyyMMdd")
    .withZone(TimeZone.getTimeZone(dataTimeZone).toZoneId)
  val dayFormatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd 00:00:00")
    .withZone(TimeZone.getTimeZone(dataTimeZone).toZoneId)
  val hourFormatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:00:00")
    .withZone(TimeZone.getTimeZone(dataTimeZone).toZoneId)

  val DAY_FORMAT = new SimpleDateFormat("yyyyMMdd") {
    {
      setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    }
  }
}
