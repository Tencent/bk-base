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

package com.tencent.bk.base.dataflow.spark.sql.parser

import com.tencent.bk.base.dataflow.core.conf.ConstantVar
import ConstantVar.PeriodUnit
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period.Period
import com.tencent.bk.base.dataflow.spark.utils.JsonUtil
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SQLNodeJsonParamParser(resultTableID: String, apiParams: Map[String, Object])
  extends NodeJsonParamParser {

  override def getCountFreq(): Int =
    CommonUtil.getOptVal(apiParams.get("count_freq")).asInstanceOf[Double].toInt

  // scalastyle:off
  private def getStorages(): mutable.Buffer[(String, Map[String, String])] = {
    val storages: ListBuffer[(String, Map[String, String])] = ListBuffer()
    val apiStorages = CommonUtil.getOptVal(apiParams.get("storages")).asInstanceOf[Map[String, Any]]
    // 如果存在多输出
    val storagesConfig = apiStorages.get("multi_outputs") match {
      case Some(o) => {
        if (o.isInstanceOf[Map[String, Any]]) {
          CommonUtil.getOptVal(o.asInstanceOf[Map[String, Any]].get(resultTableID))
            .asInstanceOf[Map[String, Any]]
        } else {
          throw new BatchException(
            s"Optional format Exception! : ${o.toString}",
            ErrorCode.ILLEGAL_ARGUMENT)
        }
      }
      case _ => apiStorages
    }

    // 存储类型默认是json
    val dataType = storagesConfig.get("hdfs") match {
      case Some(x) if (x.isInstanceOf[Map[String, Any]]) => {
        x.asInstanceOf[Map[String, Any]].get("data_type") match {
          case Some(a) if (a.isInstanceOf[String]) => a.asInstanceOf[String]
          case _ => "json"
        }
      }
      case _ => "json"
    }
    storages += ("hdfs" -> Map("rt_id" -> getResultTableID(), "data_type" -> dataType))
    storagesConfig.keys.foreach(x => {
      if (x.toLowerCase != "hdfs" && x.toLowerCase != "kafka") {
        storages += ("default" -> Map("rt_id" -> getResultTableID()))
      }
    })
    storages
  }
  // scalastyle:on
  /**
   * tdw结果
   * 1、必然有一个tdw结构输出
   * 2、其他的结果输出通知总线
   * @return
   */
  private def getTdwStorages(rt_id: String): mutable.Buffer[(String, Map[String, String])] = {
    val storages: ListBuffer[(String, Map[String, String])] = ListBuffer()
    storages += ("tdw" -> Map("rt_id" -> rt_id))
    val storagesConfig =
      CommonUtil.getOptVal(apiParams.get("storages")).asInstanceOf[Map[String, Any]]
    storagesConfig.keys.foreach(x => {
      if (x.toLowerCase != "tdw") {
        // 其他输出通过总线tdw_shipper通道
        storages += ("tdw_shipper" -> Map("rt_id" -> rt_id))
      }
    })
    storages
  }

  def getStoragesMap(): Map[String, Any] = {
    CommonUtil.getOptVal(apiParams.get("storages")).asInstanceOf[Map[String, Any]]
  }

  def getOutputs(): Map[String, mutable.Buffer[(String, Map[String, String])]] = {
    // 非tdw暂时只支持单输出
    Map(getResultTableID() -> getStorages())
  }

  def getTdwOutputs(): Map[String, mutable.Buffer[(String, Map[String, String])]] = {
    // TDW节点如果有multi_outputs 属性，可支持多输出; 如果没有multi_outputs节点，适配回老的storages节点的单输出。
    val storagesConfig = CommonUtil.getOptVal(apiParams.get("storages")).asInstanceOf[Map[String, Any]]
    storagesConfig.get("multi_outputs").asInstanceOf[Option[Map[String, Any]]] match {
      case Some(value) if value != null && !value.isEmpty => {
        value.map(x => (x._1 -> getTdwStorages(x._1)))
      }
      case _ => Map(getResultTableID() -> getTdwStorages(getResultTableID()))
    }
  }

  def getCluster(): String = CommonUtil.getOptVal(apiParams.get("cluster_name")).toString

  def getResultTableID(): String = resultTableID

  def getSchedulePeriodStr(): String =
    CommonUtil.getOptVal(apiParams.get("schedule_period")).toString.toUpperCase()

  override def getSchedulePeriod(): Period = Period.withName(getSchedulePeriodStr())

  def getSQL(): String = CommonUtil.getOptVal(apiParams.get("content")).toString.replace("==", "=")

  override def getParentsIDS: Map[String, Any] =
    CommonUtil.getOptVal(apiParams.get("parent_ids")).asInstanceOf[Map[String, Any]]

  def getDelay(): Int = CommonUtil.getOptVal(apiParams.get("delay")).asInstanceOf[Double].toInt

  override def isAccumulate(): Boolean =
    CommonUtil.getOptVal(apiParams.get("accumulate")).asInstanceOf[Boolean]

  def getDataStart(): Int =
    CommonUtil.getOptVal(apiParams.get("data_start")).asInstanceOf[Double].toInt

  def getDataEnd(): Int = CommonUtil.getOptVal(apiParams.get("data_end")).asInstanceOf[Double].toInt

  def getUDF(): Map[String, Any] = {
    val map: mutable.Map[String, Any] = mutable.Map()
    val udfs = CommonUtil.getOptVal(apiParams.get("udf")).asInstanceOf[List[Map[String, Any]]]
    var list: mutable.Buffer[Map[String, Any]] = mutable.Buffer.empty[Map[String, Any]];
    for (udf <- udfs) {
      val udf_info = JsonUtil.jsonToMap(CommonUtil.getOptVal(udf.get("udf_info")).toString)
      list += udf_info
    }
    map += ("config" -> list)
    map.toMap
  }

  override def toString(): String = apiParams.toString()

  /**
   * 获取多个父表中最小窗口,单位小时
   *
   */
  // scalastyle:off
  override def getMinWindowSize(): (Int, ConstantVar.PeriodUnit) = {
    val parent_tables = getParentsIDS
    val windowSizeCollect = new ListBuffer[Int]
    var hasMinus1WindowSize = false
    var unit = PeriodUnit.hour
    for (table <- parent_tables) {
      val parameters = table._2.asInstanceOf[Map[String, Any]]
      val windowSize = if (isAccumulate()) {
        1
      } else {
        val window_size_period = PeriodUnit.valueOf(
          CommonUtil.getOptVal(parameters.get("window_size_period")).toString.toLowerCase)
        window_size_period match {
          case PeriodUnit.hour => {
            CommonUtil.getOptVal(parameters.get("window_size")).asInstanceOf[Double].toInt
          }
          case PeriodUnit.day => {
            CommonUtil.getOptVal(parameters.get("window_size")).asInstanceOf[Double].toInt * 24
          }
          case PeriodUnit.week => {
            CommonUtil.getOptVal(parameters.get("window_size")).asInstanceOf[Double].toInt * 24 * 7
          }
          case PeriodUnit.month => {
            unit = PeriodUnit.month
            CommonUtil.getOptVal(parameters.get("window_size")).asInstanceOf[Double].toInt
          }
        }
      }

      if (windowSize == -1) {
        hasMinus1WindowSize = true
        MDCLogger.logger(s"Found -1 window size for table ${table._1}")
      } else {
        windowSizeCollect.append(windowSize)
      }
    }

    if (hasMinus1WindowSize && windowSizeCollect.isEmpty) {
      val window_size_period = PeriodUnit.valueOf(getSchedulePeriod().toString.toLowerCase)
      val windowSize = window_size_period match {
        case PeriodUnit.hour => {
          getCountFreq()
        }
        case PeriodUnit.day => {
          getCountFreq() * 24
        }
        case PeriodUnit.week => {
          getCountFreq() * 24 * 7
        }
        case PeriodUnit.month => {
          unit = PeriodUnit.month
          getCountFreq()
        }
      }
      windowSizeCollect.append(windowSize)
      MDCLogger.logger(s"Added count_freq as window size($windowSize)")
    }
    (windowSizeCollect.min, unit)
  }
  // scalastyle:on
}