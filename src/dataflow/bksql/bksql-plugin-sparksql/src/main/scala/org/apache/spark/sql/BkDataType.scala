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

package org.apache.spark.sql

import org.apache.spark.sql.types._

object BkDataType {

  def str2DataType(typeStr: String): DataType = {
    typeStr.toLowerCase match {
      // String类型
      case "varchar" | "string" => StringType
      //数值类型
      case "byte" => ByteType
      case "short" => ShortType
      case "int" | "integer" => IntegerType
      case "bigint" | "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType

      case "timestamp" => TimestampType // 时间戳
      case "boolean" => BooleanType // 布尔
      case "binary" => BinaryType // 二进制
      case "null" => NullType // 空值
      case _ => StringType
    }
  }

  def dataType2String(dateType: DataType, columnName: String): String = {

    dateType match {
      // String类型
      case StringType => "string"
      //数值类型
      case ByteType => "int"
      case ShortType | IntegerType => "int"
      case LongType => "long"
      case FloatType => "float"
      case DoubleType => "double"
      // DecimalType(有一些类型映射)
      case v: DecimalType if (v.scale == 0 && v.precision <= 20)  => "long"
      case v: DecimalType if (v.scale > 0  && v.precision <= 30) => "double"
      case v: DecimalType if (v.scale == 0 && v.precision > 20)  => {
        // "biginteger"
        throw new AnalysisException(
          s"""The column "${columnName}" unsupported return ${v.typeName} type,
             |requiring cast to target type.""".stripMargin)
        }
      case v: DecimalType if (v.scale > 0  && v.precision > 30) => {
        // "bigdecimal"
        throw new AnalysisException(
          s"""The column "${columnName}" unsupported return ${v.typeName} type,
             |requiring cast to target type.""".stripMargin)
      }
      case TimestampType => throw new AnalysisException(
        s"""The column "${columnName}" unsupported return timestamp type,
           |requiring cast to target type.""".stripMargin)
      case BooleanType  => throw new AnalysisException(
        s"""The column "${columnName}" unsupported return boolean type,
           |requiring cast to target type.""".stripMargin)
      case BinaryType => throw new AnalysisException(
        s"""The column "${columnName}" unsupported return binary type,
           |requiring cast to target type.""".stripMargin)
      case ArrayType(_, _) => throw new AnalysisException(
        s"""The column "${columnName}" unsupported return array type""")
      case MapType(_, _, _) => throw new AnalysisException(
        s"""The column "${columnName}" unsupported return map type""")
      case _ => "string"
    }
  }

}
