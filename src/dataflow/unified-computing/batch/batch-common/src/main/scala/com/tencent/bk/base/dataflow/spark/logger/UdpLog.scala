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

import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.bk.base.dataflow.spark.utils.JsonUtil
import MDCLogger.Level
import MDCLogger.Level.Level
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable

object UdpLog {

  var jobId: String = ""
  var dataTime: String = ""
  var host: String = ""
  var port: Int = 0

  def init(jobId: String, dataTime: String, logUrl: String = ""): Unit = {
    this.jobId = jobId
    this.dataTime = dataTime
    try {
      if (null != logUrl && !logUrl.isEmpty) {
        val strs = logUrl.split(":")
        if (strs.length >= 2) {
          this.host = strs(0)
          this.port = strs(1).toInt
        }
      }
    } catch {
      case e: Exception => {
        // 忽略异常
      }
    }
  }

  private[logger] def log(
      kv: Map[String, String],
      errCode: String = null,
      level: Level = Level.INFO,
      cause: scala.Throwable = null): Unit = {
    try {
      if (host.isEmpty || port == 0) {
        return
      }
      val logMap = mutable.Map(
        "JOB_ID" -> jobId,
        "JOB_DATA_TIME" -> this.dataTime,
        "@timestamp" -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date),
        "LEVEL" -> level.toString
      )
      if (null != cause) {
        logMap.put("exception", ExceptionUtils.getStackTrace(cause))
      }
      kv.foreach {
        case (k, v) => {
          logMap.put(k, v)
        }
      }
      val logMsg = s"${JsonUtil.mapToJsonStr(logMap.toMap)}" + "\n"
      // 1.定义服务器的地址，端口号，数据
      val address = new InetSocketAddress(host, port);
      val data = logMsg.getBytes();
      // 2.创建数据报，包含发送的数据信息
      val packet = new DatagramPacket(data, data.length, address);
      // 3.创建DatagramSocket用于数据的传输
      val socket = new DatagramSocket();
      // 4.向服务器端发送数据报
      socket.send(packet);
    } catch {
      case e: Exception => {
        // 忽略异常
      }
    }
  }

  /**
    * 远程打印UDP日志，方便调试。日志发送异常，不影响主流程执行。
    * @param log
    */
  def sendLog(log: String): Unit = {
    try {
      if (host.isEmpty || port == 0) {
        return
      }
      val log1 = jobId + "|" + dataTime + "|" +
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + "|>" + log + "\n";
      // 1.定义服务器的地址，端口号，数据
      val address = new InetSocketAddress(host, port);
      val data = log1.getBytes();
      // 2.创建数据报，包含发送的数据信息
      val packet = new DatagramPacket(data, data.length, address);
      // 3.创建DatagramSocket用于数据的传输
      val socket = new DatagramSocket();
      // 4.向服务器端发送数据报
      socket.send(packet);
    } catch {
      case e: Exception => {
        // 忽略异常
      }
    }
  }
}
