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
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSCopySourceNode
import com.tencent.bk.base.dataflow.spark.utils.DataFrameUtil
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class BatchHDFSCopySourceOperator(
    node: BatchHDFSCopySourceNode,
    spark: SparkSession,
    sourceDataMap: util.Map[String, DataFrameWrapper])
  extends BatchHDFSSourceOperator(
    node,
    spark,
    sourceDataMap) {

  override def createNode(): DataFrameWrapper = {
    super.createNode()
    val originDataFrameWrapper = this.sourceDataMap.get(this.node.getNodeId)
    if (originDataFrameWrapper.isEmpty()) {
      MDCLogger.logger(s"${this.node.getNodeId} don't have data in origin path, " +
        s"try to create dataframe from copy path")
      val schema = DataFrameUtil.buildSchema(node.getFields.asScala)
      val paths = node.getHdfsCopyInput.getInputInfo.asScala.toList
      this.createTable(paths, schema)
    } else {
      originDataFrameWrapper
    }
  }
}
