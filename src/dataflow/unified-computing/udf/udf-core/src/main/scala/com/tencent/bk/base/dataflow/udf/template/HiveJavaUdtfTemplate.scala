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

package com.tencent.bk.base.dataflow.udf.template

import com.tencent.bk.base.dataflow.udf.common.FunctionContext

class HiveJavaUdtfTemplate extends UdfTemplate {

  override def generateFuncCode(context: FunctionContext)
  : String = {
    val funcCode = s"""
       |package com.tencent.bk.base.dataflow.udf.codegen.hive;
       |
       |${context.getImports}
       |import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
       |import org.apache.hadoop.hive.ql.metadata.HiveException;
       |import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
       |import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
       |import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
       |
       |import java.util.ArrayList;
       |import java.util.Arrays;
       |import java.util.List;
       |
       |/**
       | * Automatic code generation.
       | */
       |public class ${context.getUdfClassName} extends GenericUDTF {
       |
       |  private ${context.getUserClass} outerFunction;
       |
       |  @Override
       |  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
       |    ArrayList<String> fieldNames = new ArrayList<>();
       |    ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
       |    List<String> returnTypes = Arrays.asList("${context.getReturnDataTypes}".split(", "));
       |    for (int i = 0; i < returnTypes.size(); i++) {
       |      fieldNames.add("col" + i);
       |      AbstractPrimitiveJavaObjectInspector inspector;
       |      switch (returnTypes.get(i)) {
       |        case "String":
       |          inspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
       |          break;
       |        case "Integer":
       |          inspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
       |          break;
       |        case "Long":
       |          inspector = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
       |          break;
       |        case "Float":
       |          inspector = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
       |          break;
       |        case "Double":
       |          inspector = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
       |          break;
       |        default:
       |          throw new RuntimeException("Not support output type " + returnTypes.get(i));
       |      }
       |      fieldOIs.add(inspector);
       |    }
       |    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
       |  }
       |
       |  @Override
       |  public void process(Object[] args) throws HiveException {
       |    this.outerFunction = new ${context.getUserClass}();
       |    this.outerFunction.call(${context.getTypeParameters});
       |    for (Object[] row : this.outerFunction.getBkDataOutputs()) {
       |      Object[] result = new Object[row.length];
       |      for (int i = 0; i < row.length; i++) {
       |        result[i] = row[i];
       |      }
       |      forward(result);
       |    }
       |  }
       |
       |  @Override
       |  public void close() {
       |
       |  }
       |}
       |
       |
       |
       |
       |
       |
       |
       |
       |
       |
       |
       |
       |
       |""".stripMargin

    funcCode
  }
}
