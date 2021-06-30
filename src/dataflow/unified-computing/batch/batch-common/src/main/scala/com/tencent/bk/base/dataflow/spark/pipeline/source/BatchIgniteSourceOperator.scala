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

import java.util

import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.{DataFrameWrapper, IgniteDataFrameWrapper}
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode
import com.tencent.bk.base.dataflow.spark.utils.DataFrameUtil
import com.tencent.bk.base.datahub.cache.ignite.SparkIgCache
import org.apache.ignite.internal.binary.BinaryObjectImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

class BatchIgniteSourceOperator(
    node: BatchIgniteSourceNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper])
  extends BatchSourceOperator[DataFrameWrapper](
    node,
    sourceDataMap) {

  override def createNode(): DataFrameWrapper = {
    val javaMap = node.getConnectionInfo()
    val igniteIgCache = new SparkIgCache(javaMap, false)
    val igniteContext = igniteIgCache.buildIgniteContext(spark)

    MDCLogger.logger(s"Try to load ignite table ${node.getPhysicalName}")

    val internalRdd =
      igniteContext.fromCache(node.getPhysicalName).withKeepBinary[Object, BinaryObjectImpl]()

    val schema = DataFrameUtil.buildSchema(node.getFields.asScala, true)
    val cacheRowRdd: RDD[Row] = internalRdd.rdd.map(s => {
      Row.fromSeq(schema.toList.map(field => s._2.field[Object](field.name)))
    })

    val df = spark.createDataFrame(cacheRowRdd, schema)
    val dataFrameWrapper = IgniteDataFrameWrapper(spark, df, internalRdd, node.getNodeId)
    dataFrameWrapper.makeTempView()
    this.sourceDataMap.put(this.node.getNodeId, dataFrameWrapper)
    dataFrameWrapper
  }
}
