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

import java.util

import com.tencent.bk.base.dataflow.core.topo.NodeField
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

case class TestData(id: String, test_time_str: String, test_time_long: Long)

object ScalaTestUtils {

  def createTableField1(tableId: String): java.util.List[NodeField] = {

    val resultField: util.LinkedList[NodeField] = new util.LinkedList[NodeField]
    val field1: NodeField = new NodeField
    field1.setField("id")
    field1.setType("String")
    field1.setOrigin(tableId)
    field1.setDescription("id")
    resultField.add(field1)
    val field2: NodeField = new NodeField
    field2.setField("test_time_str")
    field2.setType("String")
    field2.setOrigin(tableId)
    field2.setDescription("test_time_str")
    resultField.add(field2)
    val field3: NodeField = new NodeField
    field3.setField("test_time_long")
    field3.setType("long")
    field3.setOrigin(tableId)
    field3.setDescription("test_time_long")
    resultField.add(field3)
    resultField
  }

  def createTableData1(
      spark: SparkSession,
      root: String,
      startTime: String,
      endTime: String): java.util.List[String] = {

    object internalImplicits extends SQLImplicits {
      protected override def _sqlContext: SQLContext = spark.sqlContext
    }

    import internalImplicits._

    val pathList = new util.ArrayList[String]()
    val startTimeMilli = TimeFormatConverter.dataCTDateFormat.parse(startTime).getTime
    val endTimeMilli = TimeFormatConverter.dataCTDateFormat.parse(endTime).getTime
    val interval = 3600 * 1000L
    var id = 1
    for (currentTimeMills <- startTimeMilli to endTimeMilli by interval) {
      val hdfsPath = s"$root${TimeFormatConverter.pathDateFormat.format(currentTimeMills)}"
      val dateStr = TimeFormatConverter.sdf2.format(currentTimeMills)
      spark.sparkContext.parallelize(
        TestData(id.toString, dateStr, currentTimeMills) ::
          TestData((id + 1).toString, dateStr, currentTimeMills) :: Nil, 1)
        .toDF().write.parquet(hdfsPath)
      pathList.add(hdfsPath)
      id = id + 2
    }
    pathList
  }

  def getIgniteFakeConfMap: Map[String, Any] = {
    var igniteConfMap: Map[String, Any] = Map.empty
    igniteConfMap += ("test_key_1" -> "test_value_1")
    igniteConfMap += ("test_key_2" -> "test_value_2")
    igniteConfMap += ("test_key_3" -> "test_value_3")
    igniteConfMap
  }

  def getIgniteFakeConfMapAsJava(): java.util.Map[String, Any] = {
    import scala.collection.JavaConverters._
    this.getIgniteFakeConfMap.asJava
  }
}
