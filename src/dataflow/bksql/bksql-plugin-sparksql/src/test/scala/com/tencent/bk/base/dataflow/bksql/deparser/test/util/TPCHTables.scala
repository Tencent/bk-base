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

package com.tencent.bk.base.dataflow.bksql.deparser.test.util

import com.tencent.blueking.bksql.table.ColumnMetadata
import CreateTableSqlToBkTable.sql

object TPCHTables {


  def getTables(): java.util.Map[String, java.util.List[ColumnMetadata]] = {
    val tables = new scala.collection.mutable.HashMap[String, java.util.List[ColumnMetadata]]
    tables += sql(
      """
        |CREATE TABLE `orders` (
        |`o_orderkey` BIGINT, `o_custkey` BIGINT, `o_orderstatus` STRING,
        |`o_totalprice` DECIMAL(10,0), `o_orderdate` DATE, `o_orderpriority` STRING,
        |`o_clerk` STRING, `o_shippriority` INT, `o_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `nation` (
        |`n_nationkey` BIGINT, `n_name` STRING, `n_regionkey` BIGINT, `n_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `region` (
        |`r_regionkey` BIGINT, `r_name` STRING, `r_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `part` (`p_partkey` BIGINT, `p_name` STRING, `p_mfgr` STRING,
        |`p_brand` STRING, `p_type` STRING, `p_size` INT, `p_container` STRING,
        |`p_retailprice` DECIMAL(10,0), `p_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `partsupp` (`ps_partkey` BIGINT, `ps_suppkey` BIGINT,
        |`ps_availqty` INT, `ps_supplycost` DECIMAL(10,0), `ps_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `customer` (`c_custkey` BIGINT, `c_name` STRING, `c_address` STRING,
        |`c_nationkey` STRING, `c_phone` STRING, `c_acctbal` DECIMAL(10,0),
        |`c_mktsegment` STRING, `c_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `supplier` (`s_suppkey` BIGINT, `s_name` STRING, `s_address` STRING,
        |`s_nationkey` BIGINT, `s_phone` STRING, `s_acctbal` DECIMAL(10,0), `s_comment` STRING)
        |USING parquet
      """.stripMargin)

    tables += sql(
      """
        |CREATE TABLE `lineitem` (`l_orderkey` BIGINT, `l_partkey` BIGINT, `l_suppkey` BIGINT,
        |`l_linenumber` INT, `l_quantity` DECIMAL(10,0), `l_extendedprice` DECIMAL(10,0),
        |`l_discount` DECIMAL(10,0), `l_tax` DECIMAL(10,0), `l_returnflag` STRING,
        |`l_linestatus` STRING, `l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE,
        |`l_shipinstruct` STRING, `l_shipmode` STRING, `l_comment` STRING)
        |USING parquet
      """.stripMargin)
    import scala.collection.JavaConverters._
    tables.toMap.asJava
  }

}
