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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

import scala.collection.parallel.ForkJoinTaskSupport

object ParquetFileFormatProxy {

  def readParquetFootersInParallel(
      conf: Configuration,
      partFiles: Seq[FileStatus],
      ignoreCorruptFiles: Boolean): Seq[Footer] = {
    val parFiles = partFiles.par
    val pool = ThreadUtils.newForkJoinPool("readingParquetFooters", 8)
    parFiles.tasksupport = new ForkJoinTaskSupport(pool)
    try {
      parFiles.flatMap { currentFile =>
        try {
          // 读取文件失败抛出异常
          Some(new Footer(currentFile.getPath(),
            ParquetFileReader.readFooter(conf, currentFile, NO_FILTER)))
        } catch { case e: RuntimeException =>
          if (ignoreCorruptFiles) {
            // logWarning(s"Skipped the footer in the corrupted file: $currentFile", e)
            None
          } else {
            throw new IOException(s"Could not read footer for file: $currentFile", e)
          }
        }
      }.seq
    } finally {
      pool.shutdown()
    }
  }

  def getSerializedConf(sparkSession: SparkSession): Serializable = {
    val serializedConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())
    serializedConf
  }

  def getSerializedConfValue(serializedConf: Serializable): Configuration = {
    serializedConf.asInstanceOf[SerializableConfiguration].value
  }

}
