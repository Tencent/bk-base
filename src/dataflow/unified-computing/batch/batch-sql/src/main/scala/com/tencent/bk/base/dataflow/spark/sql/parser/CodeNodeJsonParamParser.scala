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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.PeriodUnit
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Period.Period

import scala.collection.mutable.ListBuffer

class CodeNodeJsonParamParser(apiParams: Map[String, Object]) extends NodeJsonParamParser {

  private val accumulateType = "accumulate"

  override def getSchedulePeriod(): Period = {
    val transform =
      CommonUtil.getOptVal(getCodeNodes.get("transform")).asInstanceOf[Map[String, Any]]
    val window =
      CommonUtil.getOptVal(transform.get("window")).asInstanceOf[Map[String, Any]]
    Period.withName(
      CommonUtil.getOptVal(window.get("period_unit")).asInstanceOf[String].toUpperCase())
  }

  override def getParentsIDS: Map[String, Any] =
    CommonUtil.getOptVal(getCodeNodes.get("source")).asInstanceOf[Map[String, Any]]

  override def isAccumulate(): Boolean = {
    val transform =
      CommonUtil.getOptVal(getCodeNodes.get("transform")).asInstanceOf[Map[String, Any]]
    val window = CommonUtil.getOptVal(transform.get("window")).asInstanceOf[Map[String, Any]]
    CommonUtil.getOptVal(window.get("type")).asInstanceOf[String].equals(accumulateType)
  }

  private def getCodeNodes: Map[String, Any] =
    CommonUtil.getOptVal(apiParams.get("nodes")).asInstanceOf[Map[String, Any]]

  override def getCountFreq(): Int = {
    val transform =
      CommonUtil.getOptVal(getCodeNodes.get("transform")).asInstanceOf[Map[String, Any]]
    val window = CommonUtil.getOptVal(transform.get("window")).asInstanceOf[Map[String, Any]]
    CommonUtil.getOptVal(window.get("count_freq")).asInstanceOf[Int]
  }

  def getMinWindowSize(): (Int, PeriodUnit) = {
    val parent_tables = getParentsIDS
    val windowSizeCollect = new ListBuffer[Int]
    var unit = PeriodUnit.hour
    for (table <- parent_tables) {
      val parameters = table._2.asInstanceOf[Map[String, Any]]
      val windowSize = if (isAccumulate()) {
        1
      } else {
        val window = CommonUtil.getOptVal(parameters.get("window")).asInstanceOf[Map[String, Any]]
        val segment = CommonUtil.getOptVal(window.get("segment")).asInstanceOf[Map[String, Any]]
        val window_size_period =
          PeriodUnit.valueOf(
            CommonUtil.getOptVal(segment.get("unit")).asInstanceOf[String].toLowerCase)
        window_size_period match {
          case PeriodUnit.hour => {
            val endValue = CommonUtil.getOptVal(segment.get("end")).asInstanceOf[Double].toInt
            val startValue = CommonUtil.getOptVal(segment.get("start")).asInstanceOf[Double].toInt
            endValue - startValue
          }
          case PeriodUnit.day => {
            val endValue = CommonUtil.getOptVal(segment.get("end")).asInstanceOf[Double].toInt
            val startValue = CommonUtil.getOptVal(segment.get("start")).asInstanceOf[Double].toInt
            (endValue - startValue) * 24
          }
        }
      }

      windowSizeCollect.append(windowSize)
    }
    MDCLogger.logger(s"Parent code node min window size(${windowSizeCollect.min})")
    (windowSizeCollect.min, unit)
  }
}