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

import com.tencent.bk.base.dataflow.spark.BatchEnumerations.BatchStorageType
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.BatchStorageType.BatchStorageType
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger.Level
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.util

import com.tencent.bk.base.dataflow.spark.BatchEnumerations.BatchStorageType
import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger

object APIUtil {

  def fetchBKSQLFullParamsAsJava(rt_id: String): util.Map[String, Object] = {
    CommonUtil.collectionToJava(fetchBKSQLFullParams(rt_id)).asInstanceOf[util.Map[String, Object]]
  }

  def getBatchProcessingAsJava(rt_id: String): util.Map[String, Object] = {
    CommonUtil.collectionToJava(getBatchProcessing(rt_id)).asInstanceOf[util.Map[String, Object]]
  }

  def fetchOneTimeSqlFullParamsAsJava(jobId: String): util.Map[String, Object] = {
    CommonUtil.collectionToJava(fetchOneTimeSqlFullParams(jobId)).asInstanceOf[util.Map[String, Object]]
  }

  def fetchBKSQLFullParams(rt_id: String): Map[String, Object] = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/jobs/${rt_id}/get_param/"
    MDCLogger.logger(s"bksql request url -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Object]]
    }
  }


  def fetchOneTimeSqlFullParams(jobId: String): Map[String, Object] = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/custom_jobs/${jobId}/get_param/"
    MDCLogger.logger(s"bksql request url -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Object]]
    }
  }

  def fetchADHOCFullParams(query_id: String): Map[String, String] = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/adhoc/${query_id}/get_param/"
    MDCLogger.logger(s"adhoc request url -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, String]]
    }
  }

  def getBatchProcessing(rt_id: String): Map[String, Any] = {
    // TODO 后面改成esb
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/processings/${rt_id}/"
    MDCLogger.logger(s"fetch rt -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
    CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Any]]
  }

  def getProcessingUdfAsJava(rt_id: String): util.List[util.Map[String, Object]] = {
    CommonUtil.collectionToJava(getProcessingUdf(rt_id).toBuffer)
      .asInstanceOf[util.List[util.Map[String, Object]]]
  }

  def getProcessingUdf(rt_id: String): List[Map[String, Any]] = {
    // TODO 后面改成esb
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/processings/${rt_id}/get_udf/"
    MDCLogger.logger(s"fetch rt -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
    CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[List[Map[String, Any]]]
  }

  def getResultTable(
      rt_id: String,
      related_fields: Boolean = false,
      retryTimes: Int = 1): Map[String, Any] = {
    // TODO 后面改成esb
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    var fetchParamsUrl = s"${apiUrlPrefix}/meta/result_tables/${rt_id}/"
    if (related_fields) {
      fetchParamsUrl = fetchParamsUrl + "?related=fields"
    }
    MDCLogger.logger(s"fetch rt -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      if (retryTimes >= UCSparkConf.maxHttpRetryTimes) {
        throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
      } else {
        MDCLogger.logger(s"get_result_table url retry -> ${retryTimes}")
        getResultTable(rt_id, related_fields, retryTimes + 1)
      }
    }
    CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Any]]
  }

  def getResultTableFields(rt_id: String, retryTimes: Int = 1): List[Map[String, Any]] = {
    // TODO 后面改成esb
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/meta/result_tables/${rt_id}/fields/"
    MDCLogger.logger(s"fetch rt -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      if (retryTimes >= UCSparkConf.maxHttpRetryTimes) {
        throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
      } else {
        MDCLogger.logger(s"get_result_table_fields url retry -> ${retryTimes}")
        getResultTableFields(rt_id, retryTimes + 1)
      }
    }
    CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[List[Map[String, Any]]]
  }

  def getStorageMetaInfo(resultTableId: String, storageType: String): Map[String, Any] = {
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    val igniteMetaDataUrl = s"${apiUrlPrefix}/meta/result_tables/${resultTableId}/?related=storages"
    val headers = Map("blueking-language" -> "en")
    val metaRepsonse =
      JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(igniteMetaDataUrl, headers = headers).body)
    MDCLogger.logger(s"Storage Meta Response: ${metaRepsonse}")
    if (!metaRepsonse.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(metaRepsonse.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
    val data = CommonUtil.getOptVal(metaRepsonse.get("data")).asInstanceOf[Map[String, Any]]
    if (data.contains(storageType)) {
      throw new BatchException(
        s"IgniteMetaDataParser can't find ignite meta data", ErrorCode.API_UNAVALIABLE)
    }

    val storageInfo =
      CommonUtil.getOptVal(
        CommonUtil.getOptVal(data.get("storages")).asInstanceOf[Map[String, Any]].get(storageType))
        .asInstanceOf[Map[String, Any]]
    storageInfo
  }

  def getHdfsStorageConf(resultTableId: String): Map[String, Any] = {
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    val igniteMetaDataUrl = s"${apiUrlPrefix}/storekit/hdfs/${resultTableId}/hdfs_conf/"
    val headers = Map("blueking-language" -> "en")
    val storageConfRepsonse =
      JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(igniteMetaDataUrl, headers = headers).body)
    MDCLogger.logger(s"Storage Conf: ${storageConfRepsonse}")
    if (!storageConfRepsonse.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(storageConfRepsonse.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
    val data = CommonUtil.getOptVal(storageConfRepsonse.get("data")).asInstanceOf[Map[String, Any]]
    data
  }

  // TODO: IF rt input different from output , not adapter
  def getRTCluster(rt_id: String): (String, String) = {
    // TODO 后面改成esb
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/meta/result_tables/${rt_id}/storages/"
    MDCLogger.logger(s"fetch storages -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      val data = CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Any]]
      if (data.contains("hdfs")) {
        val hdfs = CommonUtil.getOptVal(data.get("hdfs")).asInstanceOf[Map[String, Any]]
        val storage_cluster =
          CommonUtil.getOptVal(hdfs.get("storage_cluster")).asInstanceOf[Map[String, Any]]
        (CommonUtil.getOptVal(storage_cluster.get("cluster_group")).asInstanceOf[String],
          CommonUtil.getOptVal(storage_cluster.get("cluster_name")).asInstanceOf[String])
      } else {
        ("default", "default")
      }
    }
  }

  def getClusterNameService(cluster_type: String, cluster: String): String = {
    val apiUrlPrefix = UCSparkConf.storekitApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/storekit/clusters/${cluster_type}/${cluster}/"
    MDCLogger.logger(s"fetch cluster name service -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      val data = CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Any]]
      val connection: Map[String, Any] =
        CommonUtil.getOptVal(data.get("connection")).asInstanceOf[Map[String, Any]]
      CommonUtil.getOptVal(connection.get("hdfs_url")).toString
    }
  }

  /**
   * 根据result_table_id和类型获取hdfs的存储路径
   *
   * @param rt_id result_table id
   * @return
   */
  def getHDFSPath(rt_id: String): String = {
    val apiUrlPrefix = UCSparkConf.metaApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/meta/result_tables/${rt_id}/storages/?cluster_type=hdfs"
    MDCLogger.logger(s"fetch hdfs path -> ${fetchParamsUrl}")
    val headers = Map("blueking-language" -> "en")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, headers = headers).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    } else {
      val data = CommonUtil.getOptVal(rtn.get("data")).asInstanceOf[Map[String, Any]]
      val hdfs: Map[String, Any] =
        CommonUtil.getOptVal(data.get("hdfs")).asInstanceOf[Map[String, Any]]

      val dataType = hdfs.get("data_type") match {
        case Some(x) => if (x != null) x.toString else ""
        case None => ""
      }
      if (dataType.toLowerCase().equals("iceberg")) {
        val storageConfig =
          JsonUtil.jsonToMap(CommonUtil.getOptVal(hdfs.get("storage_config")).toString)
        storageConfig.get("parquet") match {
          case Some(x) => x.toString
          case None => CommonUtil.getOptVal(storageConfig.get("json")).toString
        }

      } else {
        CommonUtil.getOptVal(hdfs.get("physical_table_name")).toString
      }
    }
  }

  def addRetryRecord(
      rt_id: String,
      data_dir: String,
      schedule_time: Long,
      storageType: BatchStorageType = BatchStorageType.HDFS): Map[String, Any] = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val headers = Map("blueking-language" -> "en")
    val addRetryUrl =
      s"${apiUrlPrefix}/jobs/${rt_id}/add_retry/?data_dir=${data_dir}" +
        s"&schedule_time=${schedule_time}&batch_storage_type=${storageType.toString}"
    JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(addRetryUrl, headers = headers).body)
  }

  /**
   * 增加Connector，调用总线分发
   *
   * @param rt_id
   * @param data_dir
   * @param schedule_time
   * @return
   */
  def addConnector(rt_id: String, data_dir: String, schedule_time: Long): Map[String, Any] = {
    val apiUrlPrefix = UCSparkConf.databusApiPrefix
    val callConnectorUrl = s"${apiUrlPrefix}/databus/import_hdfs/"

    try {
      val params = Map("result_table_id" -> rt_id, "data_dir" -> data_dir, "description" -> "")
      val headers = Map("blueking-language" -> "en")
      val rtn =
        JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(
          callConnectorUrl,
          headers = headers,
          params = params,
          timeout = 60000).body)
      if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
        val errMsg = CommonUtil.getOptVal(rtn.get("message")).toString
        /* todo
         * 总线没有状态码，返回如下：
         * message -> Can not find storage connection info [wangyou] [offline], code -> 00, data -> Map(), result -> false
         * 临时适配
         */
        if (errMsg.startsWith("Can not find")) {
          throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
        }
        MDCLogger.logger(errMsg, level = Level.ERROR)
        addRetryRecord(rt_id, data_dir, schedule_time)
      }
      rtn
    } catch {
      case e: Exception => {
        MDCLogger.logger(e.getMessage, cause = e, level = Level.ERROR)
        addRetryRecord(rt_id, data_dir, schedule_time)
      }
    }
  }

  def callStorekitMaintain(rtName: String, days: String): Unit = {
    val apiUrlPrefix = UCSparkConf.storekitApiPrefix
    val storekitMaintainUrl = s"${apiUrlPrefix}/storekit/hdfs/${rtName}/maintain/?delta_day=$days"
    MDCLogger.logger(s"call storekit hdfs maintain interface -> $storekitMaintainUrl")
    try {
      val headers = Map("blueking-language" -> "en")
      val responseBody = HTTPUtil.sendHttpRequest(storekitMaintainUrl, headers = headers).body
      MDCLogger.logger(s"Storekit hdfs maintain response: $responseBody")
    } catch {
      case e: Exception => MDCLogger.logger(e.getMessage, level = Level.WARN)
    }
  }

  // Here is all the debug mode api
  def errorData(debug_id: String, data: Map[String, Any]): Unit = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/debugs/${debug_id}/error_data/"
    MDCLogger.logger(s"bksql request url -> ${fetchParamsUrl}")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, data).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
  }


  def metricInfo(debug_id: String, data: Map[String, Any]): Unit = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/debugs/${debug_id}/metric_info/"
    MDCLogger.logger(s"bksql request url -> ${fetchParamsUrl}")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, data).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
  }

  def resultData(debug_id: String, data: Map[String, Any]): Unit = {
    val apiUrlPrefix = UCSparkConf.batchApiPrefix
    val fetchParamsUrl = s"${apiUrlPrefix}/debugs/${debug_id}/result_data/"
    val dataStr = JsonUtil.mapToJsonStr(data)
    MDCLogger.logger(s"bksql request url -> ${fetchParamsUrl}")
    MDCLogger.logger(s"bksql request data -> ${dataStr}")
    val rtn = JsonUtil.jsonToMap(HTTPUtil.sendHttpRequest(fetchParamsUrl, data).body)
    if (!rtn.getOrElse("result", false).asInstanceOf[Boolean]) {
      val errMsg = CommonUtil.getOptVal(rtn.get("message")).asInstanceOf[String]
      throw new BatchException(errMsg, ErrorCode.API_UNAVALIABLE)
    }
  }
}
