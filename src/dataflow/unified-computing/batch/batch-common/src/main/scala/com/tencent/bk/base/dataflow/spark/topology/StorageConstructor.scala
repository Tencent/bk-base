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

import com.tencent.bk.base.dataflow.spark.utils.{APIUtil, CommonUtil, JsonUtil}

abstract class StorageConstructor(resultTableId: String, StorageType: String) {

  protected var metaInfo: Option[Map[String, Any]] = None
  protected var storageClusterMeta: Option[Map[String, Any]] = None

  def getPhysicalName: String = this.parsePhysicalName()

  def getDataType: String = this.parseDataType()

  protected def parseDataType(): String = {
    this.getMetaInfo().get("data_type") match {
      case Some(x) => if (x != null) x.toString else ""
      case None => ""
    }
  }

  protected def parsePhysicalName(): String

  protected def getMetaInfo(): Map[String, Any] = {
    this.metaInfo match {
      case Some(metaMap) => metaMap
      case None =>
        this.metaInfo = Some(APIUtil.getStorageMetaInfo(this.resultTableId, StorageType))
        this.metaInfo.get
    }
  }

  protected def getStorageClusterMeta(): Map[String, Any] = {
    this.storageClusterMeta match {
      case Some(metaMap) => metaMap
      case None =>
        this.storageClusterMeta = Some(CommonUtil
          .getOptVal(this.getMetaInfo().get("storage_cluster")).asInstanceOf[Map[String, Any]])
        this.storageClusterMeta.get
    }
  }
}

class IgniteStorageConstructor(resultTableId: String)
  extends StorageConstructor(resultTableId, "ignite"){

  var connectionMap: Option[Map[String, Any]] = None

  def getConnectionInfo: Map[String, Any] = parseConnectionInfo()

  def getConnectionInfoAsJava: java.util.Map[String, Any] = {
    import scala.collection.JavaConverters._
    parseConnectionInfo().asJava
  }

  override protected def parsePhysicalName(): String = {
    val fullPhysicalName =
      CommonUtil.getOptVal(this.getMetaInfo().get("physical_table_name")).asInstanceOf[String]
    if (fullPhysicalName.contains(".")) {
      fullPhysicalName.substring(fullPhysicalName.lastIndexOf(".") + 1)
    } else {
      fullPhysicalName
    }
  }

  private def parseConnectionInfo(): Map[String, Any] = {
    this.connectionMap match {
      case Some(map) => map
      case None =>
        val rawConnectionInfo = JsonUtil.jsonToMap(
        CommonUtil.getOptVal(
          this.getStorageClusterMeta().get("connection_info")).asInstanceOf[String])
        val connectionInfo = formatConnectionInfo(rawConnectionInfo)
        this.connectionMap = Some(connectionInfo)
        this.connectionMap.get
    }
  }

  def setConnectionMap(connection: Map[String, Any]): Unit = {
    this.connectionMap = Some(connection)
  }

  private def formatConnectionInfo(connectionInfo: Map[String, Any]): Map[String, Any] = {
    var connectionInfoParsed = Map.empty[String, String]
    connectionInfoParsed +=
      ("cluster.name" -> formatString(CommonUtil.getOptVal(connectionInfo.get("cluster.name"))))
    connectionInfoParsed +=
      ("zk.domain" -> formatString(CommonUtil.getOptVal(connectionInfo.get("zk.domain"))))
    connectionInfoParsed +=
      ("zk.root.path" -> formatString(CommonUtil.getOptVal(connectionInfo.get("zk.root.path"))))
    connectionInfoParsed +=
      ("zk.port" -> formatString(CommonUtil.getOptVal(connectionInfo.get("zk.port"))))
    connectionInfoParsed +=
      ("session.timeout" ->
        formatString(CommonUtil.getOptVal(connectionInfo.get("session.timeout"))))
    connectionInfoParsed +=
      ("join.timeout" -> formatString(CommonUtil.getOptVal(connectionInfo.get("join.timeout"))))
    connectionInfoParsed +=
      ("user" -> formatString(CommonUtil.getOptVal(connectionInfo.get("user"))))
    connectionInfoParsed +=
      ("password" -> formatString(CommonUtil.getOptVal(connectionInfo.get("password"))))
    connectionInfoParsed
  }

  private def formatString(value: Any): String = {
    if (value.isInstanceOf[Double]) {
      value.toString.substring(0, value.toString.indexOf("."))
    } else {
      value.toString
    }
  }
}

class HDFSStorageConstructor(resultTableId: String)
  extends StorageConstructor(resultTableId, "hdfs"){

  def getIcebergConf: Map[String, Any] = parseIcebergConf()

  def getIcebergConfAsJava: java.util.Map[String, Any] = {
    import scala.collection.JavaConverters._
    getIcebergConf.asJava
  }

  override protected def parsePhysicalName(): String = {
    CommonUtil.getOptVal(this.getMetaInfo().get("physical_table_name")).asInstanceOf[String]
  }

  protected def parseIcebergConf(): Map[String, Any] = {
    val hdfsConf = APIUtil.getHdfsStorageConf(this.resultTableId)
    var icebergConf = Map.empty[String, Any]
    icebergConf += (
      "hive.metastore.uris" ->
        CommonUtil.getOptVal(hdfsConf.get("hive.metastore.uris")).asInstanceOf[String])
    icebergConf
  }
}