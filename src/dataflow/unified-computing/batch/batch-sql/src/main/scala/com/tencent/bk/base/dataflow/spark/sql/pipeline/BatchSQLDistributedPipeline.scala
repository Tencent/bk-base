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

package com.tencent.bk.base.dataflow.spark.sql.pipeline

import com.tencent.bk.base.dataflow.core.pipeline.AbstractDefaultPipeline
import com.tencent.bk.base.dataflow.core.topo.Topology
import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.runtime.BatchRuntime
import MDCLogger.Level
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology

class BatchSQLDistributedPipeline(topology: Topology)
  extends AbstractDefaultPipeline[BatchSQLTopology, BatchRuntime](topology) {
  override def createRuntime(): BatchRuntime = {
    null
  }

  override def source(): Unit = {

  }

  override def transform(): Unit = {

  }

  override def sink(): Unit = {

  }

  override def submit(): Unit = {
    if (topology.isInstanceOf[BatchSQLTopology]) {
      val batchSQLTopology = topology.asInstanceOf[BatchSQLTopology]
      import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType._
      val batchPipeline = batchSQLTopology.getBatchType match {
        case BATCH_SQL =>
          MDCLogger.logger(level = Level.INFO, errMsg = "Start Batch SQL pipeline")
          new BatchSQLPipeline(batchSQLTopology)
        case UDF_DEBUG | SQL_DEBUG | DATA_MAKEUP =>
          MDCLogger.logger(level = Level.INFO, errMsg = "Start debug pipeline")
          new BatchSQLNoMonitorPipeline(batchSQLTopology)
        case _ =>
          throw new BatchException(
            s"${batchSQLTopology.getBatchType} is not a valid batch sql type", ErrorCode.UNEXPECT)
      }
      batchPipeline.submit()
    } else {
      throw new BatchException(s"Topology is not a valid type", ErrorCode.UNEXPECT)
    }
  }
}
