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

class FlinkPyUdafTemplate extends UdfTemplate {
  override def generateFuncCode(context: FunctionContext)
  : String = {
    val funcCode = s"""
       |package com.tencent.bk.base.dataflow.udf.codegen.flink;
       |
       |import jep.JepException;
       |import org.apache.flink.table.functions.AggregateFunction;
       |import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
       |import com.tencent.bk.base.dataflow.udf.util.JepSingleton;
       |import com.tencent.bk.base.dataflow.udf.fs.Path;
       |
       |import java.util.HashMap;
       |import java.util.Iterator;
       |import java.util.Map;
       |import java.io.File;
       |
       |/**
       |* Automatic code generation.
       |*/
       |public class ${context.getUdfClassName} extends AggregateFunction<${context.getReturnType}, ${context.getContainerState}> {
       |  private transient JepSingleton jepSingleton;
       |
       |  private void jepInit() {
       |    String id = UdfUtils.getPidAndThreadId();
       |    jepSingleton = JepSingleton.getInstance("${context.getJepPath}", "${context.getPythonScriptPath}/udf/" + id + "/udf", "${context.getCpythonPackages}");
       |
       |    if (!jepSingleton.getFlag("${context.getFunctionName}")) {
       |      for (int retryNum = 2; retryNum >= 0; retryNum--) {
       |        try {
       |          UdfUtils.unzipPythonLibrary("${context.getFunctionName}", new Path("${context.getPythonScriptPath}/udf/" + id));
       |          jepSingleton.getJep().eval("from ${context.getUserFuncFileName} import init as ${context.getUserFuncFileName}_init, get_value as ${context.getUserFuncFileName}_get_value, accumulate as ${context.getUserFuncFileName}_accumulate, reset_accumulator as ${context.getUserFuncFileName}_reset_accumulator, merge as ${context.getUserFuncFileName}_merge");
       |          jepSingleton.setFlag("${context.getFunctionName}",true);
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
       |  public ${context.getContainerState} createAccumulator() {
       |    jepInit();
       |    try {
       |      ${context.getContainerState} initMap = new HashMap<>();
       |      ((Map<Object, Object>) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_init"))
       |          .forEach((k, v) -> initMap.put(${context.getCastStateMapType}));
       |      return initMap;
       |    } catch (JepException e) {
       |      e.printStackTrace();
       |      throw new RuntimeException("jep invoke failed.", e);
       |    }
       |  }
       |
       |  @Override
       |  public ${context.getReturnType} getValue(${context.getContainerState} accumulator) {
       |    jepInit();
       |    try {
       |      return ${context.getReturnType}.valueOf(jepSingleton.getJep().invoke("${context.getUserFuncFileName}_get_value", accumulator).toString());
       |    } catch (JepException e) {
       |      throw new RuntimeException("jep invoke failed. ", e);
       |    }
       |  }
       |
       |  public void accumulate(${context.getContainerState} map, ${context.getInputParams}) {
       |    jepInit();
       |    try {
       |      ${context.getContainerState} rMap = new HashMap<>((${context.getContainerState}) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_accumulate", map, ${context.getInputData}));
       |      map.clear();
       |      map.putAll(rMap);
       |    } catch (JepException e) {
       |      throw new RuntimeException("jep invoke failed. ", e);
       |    }
       |  }
       |
       |  public void resetAccumulator(${context.getContainerState} map) {
       |    jepInit();
       |    try {
       |      ${context.getContainerState} rMap = new HashMap<>((${context.getContainerState}) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_reset_accumulator", map));
       |      map.clear();
       |      map.putAll(rMap);
       |    } catch (JepException e) {
       |      throw new RuntimeException("jep invoke failed. ", e);
       |    }
       |  }
       |
       |  public void merge(${context.getContainerState} map, Iterable<${context.getContainerState}> it) {
       |    jepInit();
       |    Iterator<${context.getContainerState}> iter = it.iterator();
       |    while (iter.hasNext()) {
       |      try {
       |        ${context.getContainerState} data = iter.next();
       |        ${context.getContainerState} rMap = new HashMap<>((${context.getContainerState}) jepSingleton.getJep().invoke("${context.getUserFuncFileName}_merge", map, data));
       |        map.clear();
       |        map.putAll(rMap);
       |      } catch (JepException e) {
       |        throw new RuntimeException("jep invoke failed. ", e);
       |      }
       |    }
       |  }
       |}
       |
       |""".stripMargin

    funcCode
  }
}
