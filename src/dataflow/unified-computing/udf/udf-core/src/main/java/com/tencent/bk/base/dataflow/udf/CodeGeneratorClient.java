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

package com.tencent.bk.base.dataflow.udf;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdafForJavaGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdafForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdfForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.FunctionGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdafForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdfForJavaGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdfForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdtfForJavaGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdtfForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdfForJavaGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdtfForJavaGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.FlinkUdtfForPyGeneratorFactory;
import com.tencent.bk.base.dataflow.udf.factories.HiveUdafForJavaGeneratorFactory;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CodeGeneratorClient {

    private static Map<String, FunctionGeneratorFactory> factoryMap;

    private static void init() {
        factoryMap = new HashMap<>();

        // flink java udf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udf.toString()),
                new FlinkUdfForJavaGeneratorFactory());
        // flink python udf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udf.toString()),
                new FlinkUdfForPyGeneratorFactory());
        // hive java udf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udf.toString()),
                new HiveUdfForJavaGeneratorFactory());
        // hive python udf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udf.toString()),
                new HiveUdfForPyGeneratorFactory());

        // flink java udtf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udtf.toString()),
                new FlinkUdtfForJavaGeneratorFactory());

        // flink python udtf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udtf.toString()),
                new FlinkUdtfForPyGeneratorFactory());

        // hive java udtf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udtf.toString()),
                new HiveUdtfForJavaGeneratorFactory());

        // hive python udtf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udtf.toString()),
                new HiveUdtfForPyGeneratorFactory());

        // hive python udaf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udaf.toString()),
                new HiveUdafForPyGeneratorFactory());

        // hive java udaf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.hive.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udaf.toString()),
                new HiveUdafForJavaGeneratorFactory());

        // flink java udaf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.java.toString(),
                        ConstantVar.UserFunction.udaf.toString()),
                new FlinkUdafForJavaGeneratorFactory());

        // flink python udaf
        factoryMap.put(
                getFactoryMapKey(ConstantVar.Component.flink.toString(),
                        ConstantVar.CodeLanguage.python.toString(),
                        ConstantVar.UserFunction.udaf.toString()),
                new FlinkUdafForPyGeneratorFactory());
    }

    /**
     * Get generator factory key.
     *
     * @param componentType flink/batch/all, all contains flink and batch
     * @param codeLanguage java/python
     * @param functionType udf/udtf/udaf
     * @return the key of generator factory
     */
    private static String getFactoryMapKey(String componentType, String codeLanguage, String functionType) {
        return MessageFormat.format("{0}_{1}_{2}", componentType, codeLanguage, functionType);
    }

    /**
     * generate code main method.
     * maven 构建生命周期为 validate -> compile -> test ->  package -> verify -> install -> deploy
     * 在 install 阶段触发调用此 main 方法，进行自定义函数的 codegen 过程
     * 
     *  eg1. java udf
     *  builder.codeLanguage("java");
     *  builder.type("udf");
     *  builder.name("test_udf");
     *  builder.inputTypes(
     *  Arrays.stream("String,String".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.outputTypes(
     *  Arrays.stream("String".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.roleTypes(Arrays.asList("stream,batch".split(",")));
     *  builder.userFuncFileName("TestUdf");
     *  builder.jepPath("/usr/local/py_env/one_udf/default");
     *  builder.pythonScriptPath("/usr/local/py_env/one_udf/default");
     *
     *  eg2. python udf
     *  builder.codeLanguage("python");
     *  builder.type("udf");
     *  builder.name("test_udf");
     *  builder.inputTypes(
     *  Arrays.stream("String,String".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.outputTypes(
     *  Arrays.stream("String".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.roleTypes(Arrays.asList("stream,batch".split(",")));
     *  builder.userFuncFileName("test_udf");
     *
     *  eg3 flink-java-udtf.  codeLanguage=>java, type=>udtf, name=>test_udtf, inputTypes=>[STRING],
     *  outputTypes=>[STRING, INTEGER],
     *  roleType=stream, userFuncFileName=TestUdtf
     *
     *  eg4 hive-java-udf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("java");
     *  builder.type("udf");
     *  builder.name("test_udtf");
     *  builder.inputTypes(Arrays.asList("String,String".split(",")));
     *  builder.outputTypes(Arrays.asList("String".split(",")));
     *  builder.roleType("batch");
     *  builder.userFuncFileName("TestUdf");
     *
     *  eg5 hive-python-udf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("python");
     *  builder.type("udf");
     *  builder.name("test_udtf");
     *  builder.inputTypes(Arrays.asList("String,String".split(",")));
     *  builder.outputTypes(Arrays.asList("String".split(",")));
     *  builder.roleType("batch");
     *  builder.userFuncFileName("test_udf");
     *
     *  eg6 flink-python-udtf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("python");
     *  builder.type("udtf");
     *  builder.name("test_udtf");
     *  builder.inputTypes(Arrays.asList("String,String".split(",")));
     *  builder.outputTypes(Arrays.asList("String,Integer".split(",")));
     *  builder.roleType("stream");
     *  builder.userFuncFileName("test_udtf");
     *
     *  eg7 hive-java-udtf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("java");
     *  builder.type("udtf");
     *  builder.name("test_udtf");
     *  builder.inputTypes(Arrays.asList("String,String".split(",")));
     *  builder.outputTypes(Arrays.asList("String,Integer".split(",")));
     *  builder.roleType("batch");
     *  builder.userFuncFileName("TestUdtf");
     *
     *  eg8 hive-python-udtf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("python");
     *  builder.type("udtf");
     *  builder.name("test_udtf");
     *  builder.inputTypes(Arrays.asList("String,String".split(",")));
     *  builder.outputTypes(Arrays.asList("String,Integer".split(",")));
     *  builder.roleType("batch");
     *  builder.userFuncFileName("test_udtf");
     *
     *  eg9 hive-java-udaf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("java");
     *  builder.type("udaf");
     *  builder.name("test_udaf");
     *  builder.inputTypes(Arrays.asList("Long,Integer".split(",")));
     *  builder.outputTypes(Arrays.asList("Double".split(",")));
     *  builder.roleType("batch");
     *  builder.userFuncFileName("TestUdaf");
     *
     *  eg10 flink-java-udaf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("java");
     *  builder.type("udaf");
     *  builder.name("test_udaf");
     *  builder.inputTypes(Arrays.asList("Long,Integer".split(",")));
     *  builder.outputTypes(Arrays.asList("Double".split(",")));
     *  builder.roleType("stream");
     *  builder.userFuncFileName("TestUdaf");
     *
     *  eg11 flink-python-udaf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("python");
     *  builder.type("udaf");
     *  builder.name("test_udaf");
     *  builder.inputTypes(
     *  Arrays.stream("Long,Integer".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.outputTypes(
     *  Arrays.stream("Double".split(","))
     *  .map(CodeGeneratorClient::convertJavaType)
     *  .collect(Collectors.toList()));
     *  builder.roleTypes(Arrays.asList("stream,batch".split(",")));
     *  builder.userFuncFileName("test_udaf");
     *  builder.jepPath("/usr/local/py_env/one_udf/default");
     *  builder.pythonScriptPath("/usr/local/py_env/one_udf/default");
     *
     *  eg12 hive-python-udaf.
     *  FunctionInfo.Builder builder = FunctionInfo.builder();
     *  builder.codeLanguage("python");
     *  builder.type("udaf");
     *  builder.name("test_udaf");
     *  builder.inputTypes(Arrays.asList("Long,Integer".split(",")));
     *  builder.outputTypes(Arrays.asList("Double".split(",")));
     *  builder.roleType("stream");
     *  builder.userFuncFileName("test_udaf");
     *
     * @param args args
     */
    public static void main(String[] args) {
        String codeLanguage = args[0];
        String udfType = args[1];
        String udfName = args[2];
        String inputTypesStringArr = args[3];
        String outputTypesStringArr = args[4];
        String roleTypesStringArr = args[5];
        String userFuncFileName = args[6];
        String jepPath = args[7];
        String pythonScriptPath = args[8];
        String cpythonPackages = args[9];
        // 代码覆盖率检查也会经过 install 阶段，当 udfName 为 null 时，不需要进行自定义函数的 codegen 过程
        if (udfName == null) {
            return;
        }

        FunctionInfo functionInfo = FunctionInfo.builder()
                .codeLanguage(codeLanguage)
                .type(udfType)
                .name(udfName)
                .inputTypes(
                        Arrays.stream(inputTypesStringArr.split(","))
                                .map(CodeGeneratorClient::convertJavaType)
                                .collect(Collectors.toList()))
                .outputTypes(
                        Arrays.stream(outputTypesStringArr.split(","))
                                .map(CodeGeneratorClient::convertJavaType)
                                .collect(Collectors.toList()))
                .roleTypes(Arrays.asList(roleTypesStringArr.split(",")))
                .userFuncFileName(userFuncFileName)
                .jepPath(jepPath)
                .pythonScriptPath(pythonScriptPath)
                .cpythonPackages(cpythonPackages)
                .create();

        // Initialize factory map.
        init();
        generateFunctionClass(functionInfo);
    }

    private static String getComponentTypes(String roleType) {
        switch (roleType.toLowerCase().trim()) {
            case "stream":
                return "flink";
            case "batch":
                return "hive";
            default:
                throw new RuntimeException("Don't support role type: " + roleType);
        }
    }

    private static void generateFunctionClass(FunctionInfo functionInfo) {
        List<String> componentTypes = functionInfo.getRoleTypes().stream()
                .map(CodeGeneratorClient::getComponentTypes)
                .collect(Collectors.toList());
        for (String componentType : componentTypes) {
            FunctionGeneratorFactory factory = factoryMap.get(
                    getFactoryMapKey(componentType, functionInfo.getCodeLanguage(), functionInfo.getType()));
            if (factory == null) {
                throw new RuntimeException(MessageFormat.format("Not support {0} {1} {2} function.",
                        componentType, functionInfo.getCodeLanguage(), functionInfo.getType()));
            }
            factory.createFunctionGenerator().generate(functionInfo); // generate function file.
        }
    }

    private static String convertJavaType(String type) {
        switch (type.toLowerCase().trim()) {
            case "string":
                return "String";
            case "int":
            case "integer":
                return "Integer";
            case "long":
                return "Long";
            case "float":
                return "Float";
            case "double":
                return "Double";
            default:
                throw new RuntimeException("Not support type " + type);
        }
    }
}
