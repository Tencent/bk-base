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

class FlinkPyUdtfTemplate extends UdfTemplate {

  override def generateFuncCode(context: FunctionContext)
  : String = {
    val funcCode = s"""
      |package com.tencent.bk.base.dataflow.udf.codegen.flink;
      |
      |import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
      |import org.apache.flink.api.common.typeinfo.TypeInformation;
      |import org.apache.flink.api.java.typeutils.RowTypeInfo;
      |import org.apache.flink.table.functions.TableFunction;
      |import org.apache.flink.types.Row;
      |import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
      |import com.tencent.bk.base.dataflow.udf.util.JepSingleton;
      |import com.tencent.bk.base.dataflow.udf.fs.Path;
      |
      |import java.util.Arrays;
      |import java.util.List;
      |import java.io.File;
      |
      |/**
      | * Automatic code generation.
      | */
      |public class ${context.getUdfClassName} extends TableFunction<Row> {
      |
      |  private transient JepSingleton jepSingleton;
      |  private List<String> returnTypes = Arrays.asList("${context.getReturnDataTypes}".split(","));
      |
      |  public void eval(${context.getInputParams}) {
      |    String id = UdfUtils.getPidAndThreadId();
      |    jepSingleton = JepSingleton.getInstance("${context.getJepPath}", "${context.getPythonScriptPath}/udf/" + id + "/udf", "${context.getCpythonPackages}");
      |
      |    try {
      |      if (! jepSingleton.getFlag("${context.getFunctionName}")) {
      |        for (int retryNum = 2; retryNum >= 0; retryNum--) {
      |          try {
      |            UdfUtils.unzipPythonLibrary("${context.getFunctionName}", new Path("${context.getPythonScriptPath}/udf/" + id));
      |            jepSingleton.getJep().eval("from ${context.getUserFuncFileName} import call as ${context.getUserFuncFileName}_call, get_result as ${context.getUserFuncFileName}_get_result");
      |            jepSingleton.setFlag("${context.getFunctionName}",true);
      |          } catch (Exception e) {
      |            if (retryNum == 0) {
      |              throw new RuntimeException("Jep init failed.", e);
      |            }
      |            try {
      |              Thread.sleep(1000L);
      |            } catch (InterruptedException ignored) {
      |
      |            }
      |          } finally {
      |            UdfUtils.deleteQuietlyUdfFiles("${context.getPythonScriptPath}/udf/" + id, "${context.getFunctionName}");
      |          }
      |        }
      |      }
      |      jepSingleton.getJep().invoke("${context.getUserFuncFileName}_call", ${context.getInputData});
      |      List<List> result = (List<List>) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_get_result");
      |      for (List row : result) {
      |        Row output = new Row(${context.getReturnTypeSize});
      |        for (int i = 0; i < row.size(); i++) {
      |          if (returnTypes.get(i).trim().equalsIgnoreCase("string")) {
      |            output.setField(i, String.valueOf(row.get(i)));
      |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("integer")) {
      |            output.setField(i, Integer.valueOf(String.valueOf(row.get(i))));
      |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("long")) {
      |            output.setField(i, Long.valueOf(String.valueOf(row.get(i))));
      |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("float")) {
      |            output.setField(i, Float.valueOf(String.valueOf(row.get(i))));
      |          } else if (returnTypes.get(i).trim().equalsIgnoreCase("double")) {
      |            output.setField(i, Double.valueOf(String.valueOf(row.get(i))));
      |          } else {
      |            output.setField(i, row.get(i));
      |          }
      |        }
      |        collect(output);
      |      }
      |    } catch (Exception e) {
      |      throw new RuntimeException("Jep invoke method failed.", e);
      |    }
      |  }
      |
      |  @Override
      |  public TypeInformation<Row> getResultType() {
      |    return new RowTypeInfo(${context.getResultType});
      |  }
      |}
      |""".stripMargin

    funcCode
  }
}
