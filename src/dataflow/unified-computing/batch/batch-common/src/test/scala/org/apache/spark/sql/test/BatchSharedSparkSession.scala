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

package org.apache.spark.sql.test

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.scalatest.concurrent.Eventually
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
 */
trait BatchSharedSparkSession
  extends SQLTestUtilsBase
    with BeforeAndAfterEach
    with Eventually { self: Suite =>
  private val wareHousePath = Utils.createTempDir(namePrefix = "warehouse")
  System.setProperty("derby.system.home", wareHousePath.getCanonicalPath)

  private val bkTempPath = Utils.createTempDir(namePrefix = "bk-temp-dir")

  protected def sparkConf = {
    new SparkConf()
      // .set("spark.sql.catalogImplementation", "hive")
      .set("spark.bkdata.scheduletime", s"1622476800000")
      .set("spark.sql.warehouse.dir", s"${wareHousePath.getCanonicalPath}/warehouse")
      .set("spark.bkdata.warehouse.dir", wareHousePath.getCanonicalPath)
      .set("spark.bkdata.tmp.dir", bkTempPath.getCanonicalPath)
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
  }

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _spark: TestSparkSession = null

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def spark: SparkSession = _spark

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(sparkConf)
  }

  /**
   * Initialize the [[TestSparkSession]].  Generally, this is just called from
   * beforeAll; however, in test using styles other than FunSuite, there is
   * often code that relies on the session between test group constructs and
   * the actual tests, which may need this session.  It is purely a semantic
   * difference, but semantically, it makes more sense to call
   * 'initializeSession' between a 'describe' and an 'it' call than it does to
   * call 'beforeAll'.
   */
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSparkSession
    }
  }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  protected override def beforeAll(): Unit = {
    initializeSession()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        Utils.deleteRecursively(wareHousePath)
      }
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Clear all persistent datasets after each test
    spark.sharedState.cacheManager.clearCache()
    // files can be closed from other threads, so wait a bit
    // normally this doesn't take more than 1s
    eventually(timeout(10.seconds), interval(2.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }
}
