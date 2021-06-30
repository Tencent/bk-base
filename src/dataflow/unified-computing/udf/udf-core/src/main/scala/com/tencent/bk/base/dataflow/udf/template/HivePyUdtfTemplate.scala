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

class HivePyUdtfTemplate extends UdfTemplate {

  override def generateFuncCode(context: FunctionContext)
  : String = {
    val funcCode =s"""
       |package com.tencent.bk.base.dataflow.udf.codegen.hive;
       |
       |import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
       |import org.apache.hadoop.hive.ql.metadata.HiveException;
       |import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
       |import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
       |import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
       |import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
       |import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
       |import com.tencent.bk.base.dataflow.udf.util.JepSingleton;
       |import com.tencent.bk.base.dataflow.udf.fs.Path;
       |
       |import java.util.ArrayList;
       |import java.util.Arrays;
       |import java.util.List;
       |import java.io.File;
       |
       |/**
       | * Automatic code generation.
       | */
       |public class ${context.getUdfClassName} extends GenericUDTF {
       |
       |  private transient JepSingleton jepSingleton;
       |  private transient List<String> returnTypes;
       |
       |  private void initJep() {
       |    String id = UdfUtils.getPidAndThreadId();
       |    jepSingleton = JepSingleton.getInstance("${context.getJepPath}", "${context.getPythonScriptPath}/udf/" + id + "/udf", "${context.getCpythonPackages}");
       |    if (null == returnTypes) {
       |      returnTypes = Arrays.asList("${context.getReturnDataTypes}".split(", "));
       |    }
       |    if (! jepSingleton.getFlag("${context.getFunctionName}")) {
       |      for (int retryNum = 2; retryNum >= 0; retryNum--) {
       |        try {
       |          UdfUtils.unzipPythonLibrary("${context.getFunctionName}", new Path("${context.getPythonScriptPath}/udf/" + id));
       |          jepSingleton.getJep().eval("from ${context.getUserFuncFileName} import call as ${context.getUserFuncFileName}_call, get_result as ${context.getUserFuncFileName}_get_result");
       |          jepSingleton.setFlag("${context.getFunctionName}", true);
       |        } catch (Exception e) {
       |          if (retryNum == 0) {
       |            throw new RuntimeException("Jep init failed.", e);
       |          }
       |          try {
       |            Thread.sleep(1000L);
       |          } catch (InterruptedException ignored) {
       |
       |          }
       |        } finally {
       |          UdfUtils.deleteQuietlyUdfFiles("${context.getPythonScriptPath}/udf/" + id, "${context.getFunctionName}");
       |        }
       |      }
       |    }
       |  }
       |
       |  @Override
       |  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
       |    ArrayList<String> fieldNames = new ArrayList<>();
       |    ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
       |    if (null == returnTypes) {
       |      returnTypes = Arrays.asList("${context.getReturnDataTypes}".split(", "));
       |    }
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
       |    initJep();
       |    try {
       |      jepSingleton.getJep().invoke("${context.getUserFuncFileName}_call", ${context.getTypeParameters});
       |      List<List> pyResult = (List<List>) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_get_result");
       |      for (List row : pyResult) {
       |        Object[] result = new Object[row.size()];
       |        for (int i = 0; i < row.size(); i++) {
       |          if (returnTypes.get(i).trim().equalsIgnoreCase("string")) {
       |            result[i] = String.valueOf(row.get(i));
       |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("integer")) {
       |            result[i] = Integer.valueOf(String.valueOf(row.get(i)));
       |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("long")) {
       |            result[i] = Long.valueOf(String.valueOf(row.get(i)));
       |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("float")) {
       |            result[i] = Float.valueOf(String.valueOf(row.get(i)));
       |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("double")) {
       |            result[i] = Double.valueOf(String.valueOf(row.get(i)));
       |          } else {
       |            result[i] = row.get(i);
       |          }
       |        }
       |        forward(result);
       |      }
       |    } catch (Exception e) {
       |      throw new RuntimeException("Jep invoke method failed.", e);
       |    }
       |  }
       |
       |  @Override
       |  public void close() {
       |
       |  }
       |}
       |""".stripMargin

    funcCode
  }
}
