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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource}
import org.codehaus.janino.SimpleCompiler

object BkHiveUdfCodeGenerator {

  def generateUdf(functionName: String,
                  inputs: Seq[String],
                  returnType: String,
                  requiredArgNum: Int): CatalogFunction = {
    val pkg = "com.tencent.bk.base.dataflow.bksql.spark.sql.udf"
    val className = s"BK_UDF_${functionName}"
    val checkArgumentsStr = inputs.zipWithIndex.map {
      case (str, i) =>
        s"""
          |    if (arguments.length > ${i}) {
          |      UDFTypeUtils.checkArgument(arguments[${i}], "${BkHiveUDFTypeUtils.getInputType(str)}", ${i});
          |    }
        """.stripMargin
    }.mkString(";")
    val clazz =
      s"""
         |package ${pkg};
         |import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
         |import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
         |import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
         |
         |import org.apache.hadoop.hive.ql.metadata.HiveException;
         |import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
         |import org.apache.hadoop.hive.serde2.objectinspector.*;
         |import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
         |import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
         |import org.apache.spark.sql.UDFTypeUtils;
         |
         |public class ${className} extends GenericUDF {
         |  @Override
         |  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
         |    if (${requiredArgNum == inputs.size} && arguments.length != ${inputs.size}) {
         |        throw new UDFArgumentLengthException(
         |                    "The function ${functionName} accepts ${inputs.size} arguments.");
         |    }
         |    if (arguments.length < ${requiredArgNum}) {
         |        throw new UDFArgumentLengthException(
         |                    "The function ${functionName} accepts at least ${inputs.size} arguments.");
         |    }
         |    if (arguments.length > ${inputs.size}) {
         |        throw new UDFArgumentLengthException(
         |                    "The function ${functionName} accepts at most ${inputs.size} arguments.");
         |    }
         |
         |    ${checkArgumentsStr}
         |
         |    return ${BkHiveUDFTypeUtils.getReturnType(returnType)};
         |  }
         |  @Override
         |  public Object evaluate(DeferredObject[] arguments) throws HiveException {
         |    return null;
         |  }
         |  @Override
         |  public String getDisplayString(String[] children) {
         |    return null;
         |  }
         |}
      """.stripMargin
    println(clazz)
    compiler(clazz)
    CatalogFunction(FunctionIdentifier(functionName, None), s"${pkg}.${className}", Seq[FunctionResource]())
  }

  private def compiler(clazz: String): Unit = {
    val compiler = new SimpleCompiler
    compiler.setParentClassLoader(Thread.currentThread().getContextClassLoader())
    compiler.cook(clazz)
    Thread.currentThread().setContextClassLoader(compiler.getClassLoader)
  }
}
