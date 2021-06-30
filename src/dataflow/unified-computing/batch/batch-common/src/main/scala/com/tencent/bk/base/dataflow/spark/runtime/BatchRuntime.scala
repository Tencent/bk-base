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

package com.tencent.bk.base.dataflow.spark.runtime

import java.io.File
import java.util.UUID

import com.tencent.bk.base.dataflow.core.runtime.AbstractDefaultRuntime
import com.tencent.bk.base.dataflow.spark.UCSparkConf
import com.tencent.bk.base.dataflow.spark.logger.MDCLogger
import com.tencent.bk.base.dataflow.spark.sql.function.SparkZipFieldsToRecords
import com.tencent.bk.base.dataflow.spark.sql.function.base.{SparkFunctionFactory, SparkUserDefinedFunctionFactory}
import com.tencent.bk.base.dataflow.spark.sql.udf.{BatchScheduleTime, ConvertToInteger, ConvertToLong, SparkSplitIndex}
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil
import MDCLogger.Level
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType}

abstract class BatchRuntime(topology: BatchTopology)
  extends AbstractDefaultRuntime[SparkSession, BatchTopology](topology) {

  @Override
  def asEnv(): SparkSession = {
    val sparkSession = SparkSession.getActiveSession.get

    addHdfsConf(sparkSession.sparkContext.hadoopConfiguration)
    setHadoopConf(sparkSession.sparkContext.hadoopConfiguration)
    BatchRuntime.cleanExpireWarehouse()
    BatchRuntime.cleanExpireTmpDir()
    this.registerInternalFunction(sparkSession)
    sparkSession
  }

  def registerInternalFunction(sparkSession: SparkSession): Unit = {
    BatchRuntime.registerUDF(sparkSession)
    val sparkFunctionFactory = new SparkFunctionFactory(sparkSession)
    sparkFunctionFactory.registerAll()
    SparkUserDefinedFunctionFactory.registerAll(sparkSession, topology.getUserDefinedFunctions)
    BatchRuntime.registerUDTF(sparkSession)
  }

  def setHadoopConf(hadoopConf: Configuration): Unit = {
    /*
      * builtin hive derby db ：设置javax.jdo.option.ConnectionURL
    */
    val metastoreDB = UCSparkConf.sparkConf.get("spark.sql.warehouse.dir")
    hadoopConf.set(
      "javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${metastoreDB};create=true")

    val tmpCacheDir = s"${UCSparkConf.sparkConf.get("spark.bkdata.tmp.dir")}/${UUID.randomUUID()}"
    UCSparkConf.sparkConf.set("spark.bkdata.tmp.cache.dir", tmpCacheDir)
    hadoopConf.set("hive.exec.local.scratchdir", tmpCacheDir)
    hadoopConf.set(
      "hive.downloaded.resources.dir",
      tmpCacheDir + File.separator + "${hive.session.id}_resources")
    HadoopUtil.setConf(hadoopConf)
  }

  def addHdfsConf(hadoopConf: Configuration): Unit

  protected def addHdfsConfBasedOnTopo(hadoopConf: Configuration): Unit = {
    val storageClusterGroup = topology.getStorageClusterGroup();
    hadoopConf.clear()
    hadoopConf.addResource("core-site.xml")
    hadoopConf.addResource("hdfs-site.xml")
    MDCLogger.logger(s"Added hadoop config file core-site.xml and hdfs-site.xml")
    val clusterCoreFile = s"core-site.xml.${storageClusterGroup.toString.toLowerCase()}"
    val clusterHdfsFile = s"hdfs-site.xml.${storageClusterGroup.toString.toLowerCase()}"
    hadoopConf.addResource(clusterCoreFile)
    hadoopConf.addResource(clusterHdfsFile)
    MDCLogger.logger(s"Added hadoop config file $clusterCoreFile and $clusterHdfsFile")
  }

  override def execute(): Unit = {

  }

  def close(): Unit = {
    BatchRuntime.removeAllCurrentTempDir()
  }
}

object BatchRuntime {

  def removeAllCurrentTempDir(): Unit = {
    val spark = SparkSession.getActiveSession.get
    BatchRuntime.cleanWarehouse()
    BatchRuntime.cleanTmpCacheDir()
  }

  def registerUDF(spark: SparkSession): Unit = {
    /** 注册udf */
    //    spark.udf.register("GroupConcat", GroupConcat)
    spark.udf.register("ConvertToInteger", new ConvertToInteger, IntegerType)
    spark.udf.register("ConvertToLong", new ConvertToLong, LongType)

    // 补上历史的zip函数注册
    spark.udf.register(
      "zip", new SparkZipFieldsToRecords, DataTypes.createArrayType(DataTypes.StringType))
    spark.udf.register("split_index", new SparkSplitIndex, StringType)
    spark.udf.register("batch_scheduleTime", new BatchScheduleTime, StringType)
  }

  def registerUDTF(spark: SparkSession): Unit = {
    try {
      val ipCityRegisterClazz = Class.forName(
        "com.tencent.bk.base.dataflow.spark.sql.extend.udtf.IpCitySparkRegister")
      val registerMethod =
        ipCityRegisterClazz.getMethod("registerUDTF",
          Class.forName("org.apache.spark.sql.SparkSession"))
      registerMethod.invoke(null, spark)
    } catch {
      case e: ClassNotFoundException =>
        MDCLogger.logger(s"Can't load IpCitySparkRegister: ${e.getMessage}")
    }
  }

  def cleanWarehouse(): Unit = {
    // 清理metestoredb
    val metastoreDB = UCSparkConf.sparkConf.get("spark.sql.warehouse.dir")
    MDCLogger.logger(s"Start to clean spark.sql.warehouse.dir: $metastoreDB")
    try {
      FileUtils.deleteDirectory(new File(metastoreDB).getParentFile)
    } catch {
      case e: Exception => MDCLogger.logger(
        s"""
           | delete hive derby local metastore failed,
           | patch: ${metastoreDB}
           | message: ${e.getMessage}
          """.stripMargin)
    }
  }

  def cleanTmpCacheDir(): Unit = {
    // 清理临时缓存目录
    val tmpCache = UCSparkConf.sparkConf.get("spark.bkdata.tmp.cache.dir")
    MDCLogger.logger(s"Start to clean spark.bkdata.tmp.cache.dir: $tmpCache")
    try {
      FileUtils.deleteDirectory(new File(tmpCache))
    } catch {
      case e: Exception => MDCLogger.logger(
        s"""
           | delete hive cache dir failed,
           | patch: ${tmpCache}
           | message: ${e.getMessage}
          """.stripMargin)
    }
  }

  def cleanExpireWarehouse(): Unit = {
    // 清理三天前的过期metastoredb，防止有文件夹异常情况未删除
    val bkdataWarehouseDir = UCSparkConf.sparkConf.get("spark.bkdata.warehouse.dir")
    MDCLogger.logger(s"check expire warehouse ${bkdataWarehouseDir}.",
      level = Level.INFO)
    val f = new File(bkdataWarehouseDir)
    if (f.exists()) {
      val children = f.listFiles()
      val now = System.currentTimeMillis()
      if (children != null) {
        for (child <- children) {
          MDCLogger.logger(s"check child warehouse ${child}, lastmodify ${child.lastModified()}.",
            level = Level.INFO)
          if (now - child.lastModified() > 1000 * 60 * 60 * 24 * 3) {
            MDCLogger.logger(s"clear expire warehouse ${child.getCanonicalPath}.",
              level = Level.INFO)
            try {
              FileUtils.deleteDirectory(child)
            } catch {
              case e: Exception => MDCLogger.logger("clean child warehouse error.", e)
            }
          }
        }
      }
    }
  }

  def cleanExpireTmpDir(): Unit = {
    // 清理三天前的过期tmp目录，防止有文件夹异常情况未删除
    val bkdataTmpDir = UCSparkConf.sparkConf.get("spark.bkdata.tmp.dir")
    MDCLogger.logger(s"check expire warehouse ${bkdataTmpDir}.", level = Level.INFO)
    val f = new File(bkdataTmpDir)
    if (f.exists()) {
      val children = f.listFiles()
      val now = System.currentTimeMillis()
      if (children != null) {
        for (child <- children) {
          MDCLogger.logger(s"check child tmp dir ${child}, lastmodify ${child.lastModified()}.",
            level = Level.INFO)
          if (now - child.lastModified() > 1000 * 60 * 60 * 24 * 3) {
            MDCLogger.logger(s"clear tmp dir ${child.getCanonicalPath}.", level = Level.INFO)
            try {
              FileUtils.deleteDirectory(child)
            } catch {
              case e: Exception => MDCLogger.logger("clean child warehouse error.", e)
            }
          }
        }
      }
    }
  }
}