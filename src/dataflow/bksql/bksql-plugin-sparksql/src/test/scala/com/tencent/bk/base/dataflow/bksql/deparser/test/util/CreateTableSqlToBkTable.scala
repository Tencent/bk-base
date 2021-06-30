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

import com.tencent.blueking.bksql.table.{ColumnMetadata, ColumnMetadataImpl}
import com.tencent.blueking.bksql.util.{DataType => bkDataType}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object CreateTableSqlToBkTable {

  def sql(sql: String): (String, java.util.List[ColumnMetadata]) ={
    val conf = new SQLConf()
    val sqlParser = new SparkSqlParser(conf)
    val tableDesc = sqlParser.parsePlan(sql).asInstanceOf[CreateTable].tableDesc
    val columns = new ListBuffer[ColumnMetadata]()
    tableDesc.schema.foreach(f => {
      columns.append(new ColumnMetadataImpl(f.name, dataType2bkDataType(f.dataType), ""))
    })
    import scala.collection.JavaConverters._
    (tableDesc.identifier.table, columns.toList.asJava)
  }

  private def dataType2bkDataType(dateType: DataType): bkDataType = {
    dateType match {
      // String类型
      case StringType => bkDataType.STRING
      //数值类型
      case ByteType => bkDataType.INTEGER
      case ShortType | IntegerType => bkDataType.INTEGER
      case LongType => bkDataType.LONG
      case FloatType => bkDataType.FLOAT
      case DoubleType => bkDataType.DOUBLE
      // 代表任意精度的10进制数据。通过内部的java.math.BigDecimal支持。
      //TODO:BigDecimal暂时没有支持
      case v: DecimalType => bkDataType.DOUBLE

      case TimestampType => bkDataType.LONG // 时间戳
      case BooleanType  => bkDataType.BOOLEAN // 布尔
      case BinaryType => bkDataType.STRING // 二进制
      case _ => bkDataType.STRING
    }
  }


}
