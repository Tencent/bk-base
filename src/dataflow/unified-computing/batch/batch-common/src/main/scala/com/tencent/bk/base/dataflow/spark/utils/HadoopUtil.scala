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

package com.tencent.bk.base.dataflow.spark.utils

import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Env
import com.tencent.bk.base.dataflow.spark.BatchEnumerations.Env.Env
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FsStatus, Path}
import org.apache.spark.sql.DataFrame

object HadoopUtil {
  private var config = new Configuration()
  private val maxRetryTimes = 10
  private var currentClusterGroup = "DEFAULT"
  private var currentClusterName = "hdfs_default"
  private var runMode = Env.PRODUCT

  def getNameService(): String = {
    APIUtil.getClusterNameService("hdfs", currentClusterName)
  }

  def setConf(config: Configuration): Unit = {
    this.config = config
  }

  def getConf(): Configuration = {
    this.config
  }

  def setEnv(env: Env): Unit = {
    this.runMode = env
  }

  def setClusterName(clusterName: String): Unit = {
    this.currentClusterName = clusterName
  }

  def getClusterName(): String = {
    this.currentClusterName
  }

  def setClusterGroup(clusterGroup: String): Unit = {
    this.currentClusterGroup = clusterGroup
  }

  def getClusterGroup(): String = {
    this.currentClusterGroup
  }

  def listStatus(path: String): Seq[FileStatus] = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    fs.listStatus(new Path(path)).toSeq
  }

  def getStatus(path: String): FsStatus = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    fs.getStatus(new Path(path))
  }

  def isExist(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    fs.exists(new Path(path))
  }

  def hasDataFiles(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    if (fs.exists(new Path(path))) {
      fs.listStatus(new Path(path))
        .toSeq
        .map(_.getPath.getName)
        .exists(v => v.endsWith(".parquet") || v.endsWith(".json"))
    } else {
      false
    }
  }

  def createNewFile(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    fs.createNewFile(p)
  }

  def moveFile(src: String, dst: String): Boolean = {
    val p1 = new Path(src)
    val fs = p1.getFileSystem(config)
    val p2 = new Path(dst)
    MDCLogger.logger(s"Try to move $src -> $dst")
    fs.rename(p1, p2)
  }

  def isDirectory(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    fs.isDirectory(p)
  }

  def deleteDirectory(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    if (fs.isDirectory(p)) {
      MDCLogger.logger(s"Delete directory: $path")
      fs.delete(p, true)
    } else {
      MDCLogger.logger(
        s"Failed to delete directory: $path, it doesn't exists or it's not a directory")
      false
    }
  }

  /**
   * delete hdfs path
   *
   * @param path
   * @return
   */
  def delete(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(config)
    if (fs.exists(p)) {
      MDCLogger.logger("delete hdfs path: " + path)
      fs.delete(new Path(path), true)
    } else {
      true
    }
  }

  /**
   * save dataframe to hdfs dir
   * 写HDFS，不写内存
   */
  def saveDataFrameToHDFS(
      df: DataFrame,
      dir: String,
      rt_id: String = null,
      dataType: String = "parquet",
      mode: String = "overwrite"): Unit = {
    MDCLogger.logger(s"Save to HDFS : $dir, mode: $mode, type: $dataType")
    df.write.format(dataType).mode(mode).save(dir)
  }
}
