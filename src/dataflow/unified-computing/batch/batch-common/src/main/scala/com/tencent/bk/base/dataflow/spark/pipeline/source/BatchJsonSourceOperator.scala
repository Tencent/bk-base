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

package com.tencent.bk.base.dataflow.spark.pipeline.source

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import java.util

import com.tencent.bk.base.dataflow.core.topo.NodeField
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, DefaultDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchJsonSourceNode
import com.tencent.bk.base.dataflow.spark.utils.DataFrameUtil
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable

class BatchJsonSourceOperator(
    node: BatchJsonSourceNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper])
  extends BatchSourceOperator[DataFrameWrapper](
    node,
    sourceDataMap) {

  override def createNode(): DataFrameWrapper = {
    val fields = node.getFields.asScala
    val paths =
      node.getJsonInput().getInputInfo.asScala.toList.asInstanceOf[List[util.Map[String, Object]]]
    createTable(paths, fields)
  }

  def createTable(
      data: List[util.Map[String, Object]],
      fields: mutable.Buffer[NodeField]): DataFrameWrapper = {
    val schema = DataFrameUtil.buildSchema(fields)
    val debugRowList = new util.ArrayList[Row]()
    for (debugDataRow <- data) {
      val debugRow = new Array[Any](fields.size)
      for (i <- fields.indices) {
        debugRow(i) = parseDebugValue(debugDataRow.get(fields(i).getField), fields(i).getType)
      }
      val row = new GenericRowWithSchema(debugRow, schema)
      MDCLogger.logger("row info " + row.toString())
      debugRowList.add(row)
    }
    MDCLogger.logger("row schema : " + debugRowList.get(0).schema)
    val df = spark.createDataFrame(debugRowList, debugRowList.get(0).schema)

    val dataFrameWrapper = DefaultDataFrameWrapper(this.spark, df, node.getNodeId)
    dataFrameWrapper.makeTempView()
    this.sourceDataMap.put(this.node.getNodeId, dataFrameWrapper)
    dataFrameWrapper
  }

  private def parseDebugValue(value: Any, fieldType: String): Any = {
    fieldType.toLowerCase() match {
      case "int" => value.toString.toInt
      case "long" => value.toString.toLong
      case "float" => value.toString.toFloat
      case _ => value
    }
  }
}
