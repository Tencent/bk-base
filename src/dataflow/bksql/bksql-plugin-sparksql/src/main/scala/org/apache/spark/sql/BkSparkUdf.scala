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

import com.tencent.bk.base.dataflow.bksql.deparser.SparkSqlUdfEntity
import SparkSqlUdfEntity.BkFuncType
import com.tencent.bk.base.dataflow.bksql.udaf.BkSparkSqlEmptyUDAF
import com.tencent.bk.base.dataflow.bksql.udf.BkSparkSqlEmptyUDF
import com.tencent.bk.base.dataflow.bksql.udtf.AbstractBaseBkSparkSqlEmptyUDTF
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.codehaus.janino.util.ClassFile

import collection.JavaConverters._
import scala.collection.Seq
import scala.collection.mutable.ListBuffer

object BkSparkUdf {

  def register(
                udfList:
                java.util.List[SparkSqlUdfEntity]): (
    FunctionRegistry, Seq[CatalogFunction], Array[ClassFile]) = {
    val udfMap = udfList.asScala.map(x => (x.getFunctionName.toLowerCase, x)).toMap

    val functionRegistry =BkSparkSqlAnalyzer.getFunctionRegistry(udfMap);
    val hiveUdfFunctions = new ListBuffer[CatalogFunction];
    val hiveUdfClassFiles = new ListBuffer[ClassFile];

    udfList.asScala.foreach(entity => {
      entity.getFunctionType match {
        case BkFuncType.UDF => {
          registerUdf(functionRegistry,
            entity.getFunctionName,
            entity.getInputTypes.size,
            toDataType(entity.getReturnType))
        }
        case BkFuncType.UDAF => {
          val udaf = new BkSparkSqlEmptyUDAF(entity.getInputTypes, entity.getReturnType);
          registerJavaUDAF(functionRegistry,
            entity.getFunctionName,
            udaf)
        }
        case BkFuncType.UDTF => {
          val className = "BKDATA_UDTF_" + entity.getFunctionName;
          hiveUdfFunctions.append(
            CatalogFunction(FunctionIdentifier(
              entity.getFunctionName, None),
              className,
              Seq[FunctionResource]()
            )
          )
          hiveUdfClassFiles.append(UdtfCodeGen.genCode(className, entity.getReturnTypes): _*)
        }
      }
    })
    (functionRegistry, hiveUdfFunctions.toSeq, hiveUdfClassFiles.toArray)
  }

  private def registerUdf(functionRegistry: FunctionRegistry, functionName: String, argNum: Int, returnType: DataType): Unit = {
    val u = new UDFRegistration(functionRegistry)
    val udf = new BkSparkSqlEmptyUDF()
    argNum match {
      case 0 => u.register(functionName, udf.asInstanceOf[UDF0[_]], returnType)
      case 1 => u.register(functionName, udf.asInstanceOf[UDF1[_, _]], returnType)
      case 2 => u.register(functionName, udf.asInstanceOf[UDF2[_, _, _]], returnType)
      case 3 => u.register(functionName, udf.asInstanceOf[UDF3[_, _, _, _]], returnType)
      case 4 => u.register(functionName, udf.asInstanceOf[UDF4[_, _, _, _, _]], returnType)
      case 5 => u.register(functionName, udf.asInstanceOf[UDF5[_, _, _, _, _, _]], returnType)
      case 6 => u.register(functionName, udf.asInstanceOf[UDF6[_, _, _, _, _, _, _]], returnType)
      case 7 => u.register(functionName, udf.asInstanceOf[UDF7[_, _, _, _, _, _, _, _]], returnType)
      case 8 => u.register(functionName, udf.asInstanceOf[UDF8[_, _, _, _, _, _, _, _, _]], returnType)
      case 9 => u.register(functionName, udf.asInstanceOf[UDF9[_, _, _, _, _, _, _, _, _, _]], returnType)
      case 10 => u.register(functionName, udf.asInstanceOf[UDF10[_, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 11 => u.register(functionName, udf.asInstanceOf[UDF11[_, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 12 => u.register(functionName, udf.asInstanceOf[UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 13 => u.register(functionName, udf.asInstanceOf[UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 14 => u.register(functionName, udf.asInstanceOf[UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 15 => u.register(functionName, udf.asInstanceOf[UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 16 => u.register(functionName, udf.asInstanceOf[UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 17 => u.register(functionName, udf.asInstanceOf[UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 18 => u.register(functionName, udf.asInstanceOf[UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 19 => u.register(functionName, udf.asInstanceOf[UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 20 => u.register(functionName, udf.asInstanceOf[UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 21 => u.register(functionName, udf.asInstanceOf[UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
      case 22 => u.register(functionName, udf.asInstanceOf[UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
    }
  }

  private[sql] def registerJavaUDAF(functionRegistry: FunctionRegistry, name: String,
                                    udaf: UserDefinedAggregateFunction): Unit = {
    def builder(children: Seq[Expression]) = ScalaUDAF(children, udaf)
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  def toDataType(typeStr: String): DataType = {
    typeStr.toLowerCase match {
      case "int" | "integer" => IntegerType
      case "long" | "bigint" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "string" => StringType
      case "array(int)" | "array(integer)" => ArrayType(IntegerType, false)
      case "array(long)" | "array(bigint)" => ArrayType(LongType, false)
      case "array(float)" => ArrayType(FloatType, false)
      case "array(double)" => ArrayType(DoubleType, false)
      case "array(string)" => ArrayType(StringType, false)
      case "array(any)" => ArrayType(StringType, false)
      case "array" => ArrayType(StringType, false)
      case _ => StringType
    }
  }

  def dataTypeToStr(dataType: DataType): String = {
    dataType match {
      case BooleanType => "boolean"
      case ByteType | ShortType | IntegerType => "int"
      case LongType => "bigint"
      case FloatType => "float"
      case DoubleType => "double"
      case StringType => "string"
      case BinaryType => "string"
      case ArrayType(_, _) => "array"
      case _ => dataType.simpleString.toLowerCase
    }
  }
/*
castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType)
 */
}
