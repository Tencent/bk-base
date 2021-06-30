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

import org.apache.spark.sql.SparkSession
import java.util

import com.tencent.bk.base.dataflow.core.source.AbstractSource
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.{AbstractBatchSourceNode, BatchHDFSCopySourceNode, BatchHDFSSourceNode, BatchIcebergCopySourceNode, BatchIcebergSourceNode, BatchIgniteSourceNode, BatchJsonSourceNode}

class BatchSourceStage(
  spark: SparkSession,
  sourceDataMap: util.Map[String, DataFrameWrapper]) {

  protected def createSourceOperator(
      node: AbstractBatchSourceNode): AbstractSource[DataFrameWrapper] = {
    node match {
      case icebergCopyNode: BatchIcebergCopySourceNode =>
        new BatchIcebergCopySourceOperator(icebergCopyNode, spark, sourceDataMap)
      case icebergNode: BatchIcebergSourceNode =>
        new BatchIcebergSourceOperator(icebergNode, spark, sourceDataMap)
      case hdfsCopyNode: BatchHDFSCopySourceNode =>
        new BatchHDFSCopySourceOperator(hdfsCopyNode, spark, sourceDataMap)
      case hdfsNode: BatchHDFSSourceNode =>
        new BatchHDFSSourceOperator(hdfsNode, spark, sourceDataMap)
      case igniteNode: BatchIgniteSourceNode =>
        new BatchIgniteSourceOperator(igniteNode, spark, sourceDataMap)
      case jsonNode: BatchJsonSourceNode =>
        new BatchJsonSourceOperator(jsonNode, spark, sourceDataMap)
      case _ =>
        throw new BatchException(
          s"Can't find valid operator for sink node ${node.getNodeId}", ErrorCode.ILLEGAL_ARGUMENT)
    }
  }

  def source(node: AbstractBatchSourceNode): Unit = {
    MDCLogger.logger(s"Source stage for node ${node.getNodeId}")
    val source = createSourceOperator(node)
    source.createNode()
  }
}
