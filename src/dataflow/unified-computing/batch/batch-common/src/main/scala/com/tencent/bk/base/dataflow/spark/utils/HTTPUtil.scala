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

import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ListBuffer

/**
  * Created by Tencent BlueKing on 2016/8/16.
  */
object HTTPUtil {
  /**
    * http请求封装，默认30秒超时，UTF-8编码，json
    *
    * @param url
    * @param params
    * @param headers
    * @return
    */
  def sendHttpRequest(
      url: String,
      params: Map[String, Any] = Map.empty[String, String],
      headers: Map[String, String] = Map.empty[String, String],
      timeout: Int = 30000): HttpResponse[String] = {
    sendHttpRequestWithRetry(url, params, headers, timeout, new ListBuffer[String])
  }

  def sendHttpRequestWithRetry(
      url: String,
      params: Map[String, Any] = Map.empty[String, String],
      headers: Map[String, String] = Map.empty[String, String],
      timeout: Int = 30000,
      exceptions: ListBuffer[String]): HttpResponse[String] = {
    for (count <- 1 to UCSparkConf.maxHttpRetryTimes) {
      try {
        var httpClient =
          Http(url).headers(Map("Content-Type" -> "application/json", "Charset" -> "UTF-8"))
        if (params.nonEmpty) {
          httpClient = httpClient.postData(JsonUtil.mapToJsonStr(params))
        }
        if (headers.nonEmpty) {
          httpClient = httpClient.headers(headers)
        }
        val httpResponse = httpClient.timeout(timeout, timeout).asString
        MDCLogger.logger(s"${url} response code: ${httpResponse.code}")
        if (!httpResponse.isServerError) {
          return httpResponse
        } else {
          exceptions.append(s"$count->Http response gets error code ${httpResponse.code}")
          MDCLogger.logger(s"Fault response body: ${httpResponse.body}")
        }
      } catch {
        case e: Exception => {
          exceptions.append(s"$count->${e.toString}")
        }
      }
      Thread.sleep(3000)
    }
    throw new BatchException(
      s"retry over max times(${UCSparkConf.maxHttpRetryTimes}), ${exceptions.toList.toString()}",
      ErrorCode.BATCH_JOB_ERROR)
  }
}
