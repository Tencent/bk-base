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

class HivePyUdfTemplate extends UdfTemplate{

  override def generateFuncCode(context: FunctionContext)
  : String = {
    val funcCode = s"""
      |package com.tencent.bk.base.dataflow.udf.codegen.hive;
      |
      |import org.apache.hadoop.hive.ql.exec.UDF;
      |import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
      |import com.tencent.bk.base.dataflow.udf.util.JepSingleton;
      |import com.tencent.bk.base.dataflow.udf.fs.Path;
      |
      |import java.io.File;
      |
      |/**
      | * Automatic code generation.
      | */
      |public class ${context.getUdfClassName} extends UDF {
      |
      | private transient JepSingleton jepSingleton;
      |
      | public ${context.getReturnType} evaluate(${context.getInputParams}) {
      |   String id = UdfUtils.getPidAndThreadId();
      |   jepSingleton = JepSingleton.getInstance("${context.getJepPath}", "${context.getPythonScriptPath}/udf/" + id + "/udf", "${context.getCpythonPackages}");
      |
      |   try {
      |     if (!jepSingleton.getFlag("${context.getFunctionName}")) {
      |       for (int retryNum = 2; retryNum >= 0; retryNum--) {
      |         try {
      |           UdfUtils.unzipPythonLibrary("${context.getFunctionName}", new Path("${context.getPythonScriptPath}/udf/" + id));
      |           jepSingleton.getJep().eval("from ${context.getUserFuncFileName} import call as ${context.getUserFuncFileName}_call");
      |           jepSingleton.setFlag("${context.getFunctionName}",true);
      |         } catch (Exception e) {
      |           if (retryNum == 0) {
      |             throw new RuntimeException("Jep init failed.", e);
      |           }
      |           try {
      |             Thread.sleep(1000L);
      |           } catch (InterruptedException ignored) {
      |           
      |           }
      |         } finally {
      |           UdfUtils.deleteQuietlyUdfFiles("${context.getPythonScriptPath}/udf/" + id, "${context.getFunctionName}");
      |         }
      |       }
      |     }
      |     return ${context.getReturnType}.valueOf(jepSingleton.getJep().invoke("${context.getUserFuncFileName}_call", ${context.getInputData}).toString());
      |   } catch (Exception e) {
      |     throw new RuntimeException("Jep invoke method failed.", e);
      |   }
      | }
      |}
      |""".stripMargin

    funcCode
  }
}
