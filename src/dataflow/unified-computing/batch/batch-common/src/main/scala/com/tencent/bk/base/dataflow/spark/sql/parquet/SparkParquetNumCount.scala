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

package com.tencent.bk.base.dataflow.spark.sql.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormatProxy
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

import java.util

import scala.collection.mutable.ListBuffer


object SparkParquetNumRow {

  def countParquetRowNum(pathArray: util.List[String], sparkSession: SparkSession): Long = {
    val pathes = createParquetFileStatusArray(
      pathArray,
      sparkSession.sparkContext.hadoopConfiguration)
    doComputeStatsForParquetRowNum(sparkSession, pathes)
  }

  private def createParquetFileStatusArray(
    pathArray: util.List[String],
    hadoopConf: Configuration): Seq[FileStatus] = {
    val resultArray = new ListBuffer[FileStatus]
    pathArray.asScala.foreach {
      path => {
        val p = new Path(path)
        val fs = p.getFileSystem(hadoopConf)
        val files = fs.listStatus(new Path(path)).toSeq
        val isParquet = files.map(_.getPath.getName).exists(_.endsWith(".parquet"))
        if (isParquet) {
          resultArray.append(files: _*)
        }
      }
    }
    resultArray
  }

  def doComputeStatsForParquetRowNum(
      sparkSession: SparkSession,
      filesToTouch: Seq[FileStatus]): Long = {
    val partialFileStatusInfo = filesToTouch
      .filter(_.getPath.toString.endsWith(".parquet"))
      .map(f => (f.getPath.toString, f.getLen))

    if (partialFileStatusInfo.nonEmpty) {
      val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
        sparkSession.sparkContext.defaultParallelism)

      val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
      // 支持序列化的配置（读取parquet的meta信息需要）
      val serializedConf = ParquetFileFormatProxy.getSerializedConf(sparkSession)

      // 通过spark的job任务并行的读取parquet的行记录
      val partiallyRowNums =
        sparkSession
          .sparkContext
          .parallelize(partialFileStatusInfo, numParallelism)
          .mapPartitions { iterator =>
            // 重建serialized `FileStatus` 只包含path 和 length信息
            val fakeFileStatuses = iterator.map { case (path, length) =>
              new FileStatus(
                length, false, 0, 0, 0, 0, null, null, null, new Path(path))
            }.toSeq
            // 在每个任务中以多线程方式读取页脚(footers)
            val footers =
              ParquetFileFormatProxy.readParquetFootersInParallel(
                ParquetFileFormatProxy.getSerializedConfValue(serializedConf),
                fakeFileStatuses, ignoreCorruptFiles)

            if (footers.isEmpty) {
              Iterator.empty
            } else {
              val rowNum = footers.flatMap(_.getParquetMetadata.getBlocks.toArray)
                .map(_.asInstanceOf[BlockMetaData].getRowCount).sum
              Iterator.single(rowNum)
            }
          }.collect()

      if (partiallyRowNums.isEmpty) {
        0L
      } else {
        partiallyRowNums.sum.toLong
      }
    } else {
      0L
    }
  }

}
