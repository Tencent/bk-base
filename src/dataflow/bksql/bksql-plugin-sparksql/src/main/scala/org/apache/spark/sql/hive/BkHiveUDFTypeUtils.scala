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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException
import org.apache.hadoop.hive.serde2.objectinspector.{ConstantObjectInspector, ObjectInspector}

object BkHiveUDFTypeUtils {

  def getReturnType(r: String): String = {
    r.toLowerCase match {
      case "int" | "integer" => "PrimitiveObjectInspectorFactory.writableIntObjectInspector"
      case "long" => "PrimitiveObjectInspectorFactory.writableLongObjectInspector"
      case "float" => "PrimitiveObjectInspectorFactory.writableFloatObjectInspector"
      case "double" => "PrimitiveObjectInspectorFactory.writableDoubleObjectInspector"
      case "string" => "PrimitiveObjectInspectorFactory.writableStringObjectInspector"
      case _ => "PrimitiveObjectInspectorFactory.writableStringObjectInspector"
    }
  }

  def getInputType(s: String): String = {
    s.toLowerCase match {
      case "smallint" | "tinyint"| "int" | "integer" => "int"
      case "long" | "bigint" => "bigint"
      case "float" => "float"
      case "double" => "double"
      case "string" | "varchar" => "string"
      case "char(255)" => "string"
      case "varchar(65535)" => "string"
      case "timestamp" => "string"
      case "binary" => "string"
      case "decimal(38,18)" => "decimal(38,18)"
      case "boolean" => "boolean"
      case _ => "string"
    }
  }

  @throws[UDFArgumentTypeException]
  def checkArgument(arg: ObjectInspector, definedType: String, index: Int): Unit = {
    val category = arg.getCategory
    if (category eq ObjectInspector.Category.PRIMITIVE) {
      if (!BkHiveUDFTypeUtils.checkInputArgumentType(arg, definedType)) {
        throw new UDFArgumentTypeException(index, "Only " + definedType + " are accepted but "
          + arg.getTypeName + " is passed.")
      }
    }
  }


  private def checkInputArgumentType(argType: ObjectInspector, definedType: String): Boolean = {
    if (argType.isInstanceOf[ConstantObjectInspector]) getInputType(argType.getTypeName) match {
      case "int" =>
        return Seq("int").contains(definedType)
      case "bigint" =>
        return Seq("bigint").contains(definedType)
      case "float" =>
        return Seq("bigint").contains(definedType)
      case "double" =>
        return Seq("double").contains(definedType)
      case "string" =>
        return Seq("string").contains(definedType)
      case "decimal(38,18)" =>
        return Seq("int", "bigint", "float", "double").contains(definedType)
      case "boolean" =>
        return Seq("boolean").contains(definedType)
      case _ =>
        return false
    }
    getInputType(argType.getTypeName) match {
      case "int" =>
        return Seq("int").contains(definedType)
      case "bigint" =>
        return Seq("bigint").contains(definedType)
      case "float" =>
        return Seq("float").contains(definedType)
      case "double" =>
        return Seq("double").contains(definedType)
      case "string" =>
        return Seq("string").contains(definedType)
      case "boolean" =>
        return Seq("boolean").contains(definedType)
      case _ =>
        return false
    }
  }

}
