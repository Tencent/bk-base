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
package com.tencent.bk.base.dataflow.spark.exception

import com.tencent.bk.base.dataflow.spark.exception.code.{CodePrefix, ErrorCode}
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import MDCLogger.Level

/**
  * 基础异常
  */
abstract class BaseException(message: String) extends RuntimeException(message) {
  def getCodePrefix(): CodePrefix

  @deprecated
  def this(message: String, cause: Option[Throwable], errCode: Option[String]) {
    this(s"${errCode.getOrElse(ErrorCode.DEFAULT)} -> ${message}")
    cause match {
      case Some(e: Throwable) => initCause(e)
      case _ =>
    }
    errCode match {
      case Some(code: String) =>
        MDCLogger.logger(message, errCode = s"${getCodePrefix()}${code}", level = Level.ERROR)
      case _ =>
    }
  }

  def this(message: String, cause: Throwable, errCode: String) {
    this(message, Option(cause), Option(errCode))
  }

  def this(message: String, errCode: String) {
    this(message, None, Option(errCode))
  }

  @deprecated
  def this(message: String, cause: Throwable) {
    this(message, Option(cause), None)
  }

  @deprecated
  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  @deprecated
  def this() {
    this("", None, None)
  }
}
