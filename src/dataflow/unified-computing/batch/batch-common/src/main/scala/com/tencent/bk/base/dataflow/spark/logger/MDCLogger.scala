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

package com.tencent.bk.base.dataflow.spark.logger

import com.tencent.bk.base.dataflow.spark.utils.CommonUtil.LogHolder
import com.tencent.bk.base.dataflow.spark.utils.JsonUtil
import MDCLogger.Level.Level

import scala.collection.mutable

object MDCLogger {
  private val commonLogger = LogHolder.logger

  object Level extends Enumeration {
    type Level = Value
    val TRACE, DEBUG, INFO, WARN, ERROR = Value
  }

  // scalastyle:off
  def logger(
      errMsg: String,
      cause: scala.Throwable = null,
      errCode: String = null,
      level: Level = Level.INFO,
      logType: String = "STATUS",
      kv: Map[String, String] = Map.empty): Unit = {
    val logMap = mutable.Map(
      "LOG_TYPE" -> logType,
      "MESSAGE" -> errMsg
    )
    if (errCode != null) {
      logMap.put("result_code", errCode)
    }
    kv.foreach {
      case (k, v) => {
        logMap.put(k, v)
      }
    }

    UdpLog.log(logMap.toMap, errCode, level, cause)

    val logMsg = s"${JsonUtil.mapToJsonStr(logMap.toMap)}"
    level match {
      case Level.TRACE => cause match {
        case null => commonLogger.trace(logMsg)
        case other => commonLogger.trace(logMsg, cause)
      }
      case Level.DEBUG => cause match {
        case null => commonLogger.debug(logMsg)
        case other => commonLogger.debug(logMsg, cause)
      }
      case Level.INFO => cause match {
        case null => commonLogger.info(logMsg)
        case other => commonLogger.info(logMsg, cause)
      }
      case Level.WARN => cause match {
        case null => commonLogger.warn(logMsg)
        case other => commonLogger.warn(logMsg, cause)
      }
      case Level.ERROR => cause match {
        case null => commonLogger.error(logMsg)
        case other => commonLogger.error(logMsg, cause)
      }
    }
  }
  // scalastyle:on
}
