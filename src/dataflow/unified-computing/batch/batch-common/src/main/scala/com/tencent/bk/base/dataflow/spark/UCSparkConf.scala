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

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.internal.SQLConf

object UCSparkConf {

  // SparkEnv.get = null for special case
  private val conf = if (SparkEnv.get == null) new SparkConf() else SparkEnv.get.conf

  def sparkConf: SparkConf = conf

  val SPARK_BKDATA_ICEBERG_PARTITION_COLUMN =
    SQLConf.buildConf("spark.bkdata.iceberg.partition.column")
      .internal()
      .doc("To config iceberg partition column")
      .stringConf
      .createWithDefault("____et")

  val UC_SPARK_CHECKPOIN_DIR =
    SQLConf.buildConf("spark.bkdata.checkpoint.dir")
      .internal()
      .doc("To config check point dir")
      .stringConf
      .createWithDefault(null)

  val SPARK_SQL_IPV4_LIB_PATH =
    SQLConf.buildConf("spark.sql.ipv4.library.path")
      .internal()
      .doc("To config ipv4 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/gslb/ipv4.dat")

  val SPARK_SQL_IPV6_LIB_PATH =
    SQLConf.buildConf("spark.sql.ipv6.library.path")
      .internal()
      .doc("To config ipv6 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/gslb/ipv6.dat")

  val SPARK_SQL_IPV4_TGEO_GENERIC_BUSINESS_PATH =
    SQLConf.buildConf("spark.sql.ipv4.tgeo_generic_business.path")
      .internal()
      .doc("To config ipv4 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/tgb/ipv4.dat")

  val SPARK_SQL_IPV6_TGEO_GENERIC_BUSINESS_PATH =
    SQLConf.buildConf("spark.sql.ipv6.tgeo_generic_business.path")
      .internal()
      .doc("To config ipv6 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/tgb/ipv6.dat")

  val SPARK_SQL_IPV4_TGEO_BASE_NETWORK_PATH =
    SQLConf.buildConf("spark.sql.ipv4.tgeo_base_network.path")
      .internal()
      .doc("To config ipv4 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/tbn/ipv4.dat")

  val SPARK_SQL_IPV6_TGEO_BASE_NETWORK_PATH =
    SQLConf.buildConf("spark.sql.ipv6.tgeo_base_network.path")
      .internal()
      .doc("To config ipv6 library local path")
      .stringConf
      .createWithDefault("/data/ieg/bkdata/iplib/tbn/ipv6.dat")

  val SPARK_BKDATA_METAAPI_URL =
    SQLConf.buildConf("spark.bkdata.metaapi.url")
      .internal()
      .doc("To config meta api url")
      .stringConf
      .createWithDefault("")

  val SPARK_BKDATA_DATAFLOW_URL =
    SQLConf.buildConf("spark.bkdata.dataflow.url")
      .internal()
      .doc("To config dataflow api url")
      .stringConf
      .createWithDefault("")

  val SPARK_BKDATA_DATABUS_URL =
    SQLConf.buildConf("spark.bkdata.databus.url")
      .internal()
      .doc("To config databus api url")
      .stringConf
      .createWithDefault("")

  val SPARK_BKDATA_STOREKIT_URL =
    SQLConf.buildConf("spark.bkdata.storekit.url")
      .internal()
      .doc("To config storekit api url")
      .stringConf
      .createWithDefault("")

  val SPARK_BKDATA_DATAMANAGE_URL =
    SQLConf.buildConf("spark.bkdata.datamanage.url")
      .internal()
      .doc("To config datamanage api url")
      .stringConf
      .createWithDefault("")

  val SPARK_BKDATA_DATA_TIMEZONE =
    SQLConf.buildConf("spark.bkdata.data.timezone")
      .internal()
      .doc("To config data timezone")
      .stringConf
      .createWithDefault("Asia/Shanghai")

  val SPARK_BKDATA_PLATFORM_TIMEZONE =
    SQLConf.buildConf("spark.bkdata.platform.timezone")
      .internal()
      .doc("To config platform timezone")
      .stringConf
      .createWithDefault(Calendar.getInstance().getTimeZone.getID)

  val SPARK_BKDATA_HTTP_RETRY_TIME =
    SQLConf.buildConf("spark.bkdata.http.retry.time")
      .internal()
      .doc("To config api retry time")
      .intConf
      .createWithDefault(3)

  val SPARK_BKDATA_EXCEPTION_RETRY_TIME =
    SQLConf.buildConf("spark.bkdata.exception.retry.time")
      .internal()
      .doc("To config exception retry time")
      .intConf
      .createWithDefault(3)

  val SPARK_BKDATA_COPY_PATH_PREFIX =
    SQLConf.buildConf("spark.bkdata.self.depend.copy.path")
      .internal()
      .doc("To copy path for self depend")
      .stringConf
      .createWithDefault("/api/flow_copy")

  val SPARK_BKDATA_SQL_DEBUG_DATA_LIMIT =
    SQLConf.buildConf("spark.bkdata.sql.debug.data.limit")
      .internal()
      .doc("To config sql debug limit value")
      .intConf
      .createWithDefault(1000)

  val SPARK_BKDATA_SQL_DEBUG_DATA_RANGE =
    SQLConf.buildConf("spark.bkdata.sql.debug.data.range")
      .internal()
      .doc("To config latest time range")
      .longConf
      .createWithDefault(24L)

  def icebergParitionColumn: String = sparkConf.get(
    SPARK_BKDATA_ICEBERG_PARTITION_COLUMN.key,
    SPARK_BKDATA_ICEBERG_PARTITION_COLUMN.defaultValue.get)

  def ucSparkCheckPointDir: String = sparkConf.get(
    UC_SPARK_CHECKPOIN_DIR.key,
    UC_SPARK_CHECKPOIN_DIR.defaultValue.get)

  def dataTimezone: String = sparkConf.get(
    SPARK_BKDATA_DATA_TIMEZONE.key,
    SPARK_BKDATA_DATA_TIMEZONE.defaultValue.get)

  def platformTimezone: String = sparkConf.get(
    SPARK_BKDATA_PLATFORM_TIMEZONE.key,
    SPARK_BKDATA_PLATFORM_TIMEZONE.defaultValue.get)

  def maxHttpRetryTimes: Int = sparkConf.getInt(
    SPARK_BKDATA_HTTP_RETRY_TIME.key,
    SPARK_BKDATA_HTTP_RETRY_TIME.defaultValue.get)

  def maxExceptionRetryTimes: Int = sparkConf.getInt(
    SPARK_BKDATA_EXCEPTION_RETRY_TIME.key,
    SPARK_BKDATA_EXCEPTION_RETRY_TIME.defaultValue.get)

  def sqlDebugDataRange: Long = sparkConf.getLong(
    SPARK_BKDATA_SQL_DEBUG_DATA_RANGE.key,
    SPARK_BKDATA_SQL_DEBUG_DATA_RANGE.defaultValue.get)

  def sqlDebugDataLimit: Int = sparkConf.getInt(
    SPARK_BKDATA_SQL_DEBUG_DATA_LIMIT.key,
    SPARK_BKDATA_SQL_DEBUG_DATA_LIMIT.defaultValue.get)

  def batchApiPrefix: String = s"${sparkConf.get(
    SPARK_BKDATA_DATAFLOW_URL.key,
    SPARK_BKDATA_DATAFLOW_URL.defaultValue.get)}/dataflow/batch"

  def streamApiPrefix: String = s"${sparkConf.get(
    SPARK_BKDATA_DATAFLOW_URL.key,
    SPARK_BKDATA_DATAFLOW_URL.defaultValue.get)}/dataflow/stream"

  def metaApiPrefix: String = sparkConf.get(
    SPARK_BKDATA_METAAPI_URL.key,
    SPARK_BKDATA_METAAPI_URL.defaultValue.get)

  def storekitApiPrefix: String = sparkConf.get(
    SPARK_BKDATA_STOREKIT_URL.key,
    SPARK_BKDATA_STOREKIT_URL.defaultValue.get)

  def databusApiPrefix: String = sparkConf.get(
    SPARK_BKDATA_DATABUS_URL.key,
    SPARK_BKDATA_DATABUS_URL.defaultValue.get)

  def datamanageApiPrefix: String = sparkConf.get(
    SPARK_BKDATA_DATAMANAGE_URL.key,
    SPARK_BKDATA_DATAMANAGE_URL.defaultValue.get)

  def copyPathPrefix: String = sparkConf.get(
    SPARK_BKDATA_COPY_PATH_PREFIX.key,
    SPARK_BKDATA_COPY_PATH_PREFIX.defaultValue.get)

  def codeJobType: String = "spark_python_code"
}
