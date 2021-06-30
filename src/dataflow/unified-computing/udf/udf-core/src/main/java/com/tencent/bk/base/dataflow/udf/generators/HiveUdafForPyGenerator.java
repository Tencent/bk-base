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

package com.tencent.bk.base.dataflow.udf.generators;

import com.google.common.base.CaseFormat;
import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.util.JepSingleton;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import com.tencent.bk.base.dataflow.udf.template.HivePyUdafTemplate;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jep.JepException;

public class HiveUdafForPyGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        FunctionContext context = createFunctionContext(functionInfo);
        String code = new HivePyUdafTemplate().generateFuncCode(context);
        String className = getClassName(functionInfo.getUserFuncFileName());

        String javaFile = String.format("%s/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.HIVE_UDF_DIRECTORY, className).replace(".", "/")
                + ".java";
        UdfUtils.createUdfFile(javaFile, code);
    }

    private FunctionContext createFunctionContext(FunctionInfo functionInfo) {
        // check return type
        if (functionInfo.getOutputTypes().size() != 1) {
            throw new RuntimeException("Udaf must have only one return value.");
        }

        FunctionContext.Builder builder = FunctionContext.builder();
        builder.setUdfClassName(getClassName(functionInfo.getUserFuncFileName()));
        // set return type
        builder.setReturnType(
                functionInfo.getOutputTypes()
                        .stream()
                        .collect(Collectors.joining(", ")));

        // set type parameters
        builder.setTypeParameters(
                IntStream.range(0, functionInfo.getInputTypes().size())
                        .mapToObj(i -> MessageFormat.format("args[{0}]", i))
                        .collect(Collectors.joining(", ")));

        // set cast state map

        // set input params
        builder.setInputParams(
                IntStream.range(0, functionInfo.getInputTypes().size())
                        .mapToObj(i -> MessageFormat.format("{0} {1}", functionInfo.getInputTypes().get(i), "args" + i))
                        .collect(Collectors.joining(", ")));

        builder.setUserFuncFileName(functionInfo.getUserFuncFileName());

        // get return type
        if (functionInfo.getOutputTypes().size() > 25) {
            throw new RuntimeException("The user defined function output parameters support up to 25.");
        }

        // set state map
        getStateMapType(functionInfo, builder);

        // get return data types
        builder.setReturnDataTypes(functionInfo.getOutputTypes().stream().collect(Collectors.joining(", ")));
        builder.setFunctionName(functionInfo.getName());
        builder.setJepPath(functionInfo.getJepPath());
        builder.pythonScriptPath(functionInfo.getPythonScriptPath());
        builder.setCpythonPackages(functionInfo.getCpythonPackages());
        return builder.create();
    }

    private void getStateMapType(FunctionInfo functionInfo, FunctionContext.Builder builder) {
        try {
            JepSingleton jepSingleton = JepSingleton.getInstance(
                    functionInfo.getJepPath(),
                    "src/main/python/com/tencent/bk/base/dataflow/udf/python/functions",
                    functionInfo.getCpythonPackages());

            jepSingleton.getJep()
                    .eval(MessageFormat.format("from {0} import init", functionInfo.getUserFuncFileName()));

            Map<Object, Object> init = (Map) jepSingleton.getJep().invoke("init");

            List<String> types = new ArrayList<>();
            for (Map.Entry<Object, Object> entry : init.entrySet()) {
                types.add(entry.getKey().getClass().getTypeName().replace("java.lang.", ""));
                types.add(entry.getValue().getClass().getTypeName().replace("java.lang.", ""));
                break;
            }

            // set cast state map type
            // key
            List<String> castTypes = new ArrayList<>();
            switch (types.get(0)) {
                case "String":
                    castTypes.add("k.toString()");
                    break;
                case "Integer":
                    castTypes.add("Integer.valueOf(k.toString())");
                    break;
                case "Long":
                    castTypes.add("Long.valueOf(k.toString())");
                    break;
                case "Float":
                    castTypes.add("Float.valueOf(k.toString())");
                    break;
                case "Double":
                    castTypes.add("Double.valueOf(k.toString())");
                    break;
                default:
                    throw new RuntimeException("In udaf state data not support " + types.get(0));
            }
            // value
            switch (types.get(1)) {
                case "String":
                    castTypes.add("v.toString()");
                    break;
                case "Integer":
                    castTypes.add("Integer.valueOf(v.toString())");
                    break;
                case "Long":
                    castTypes.add("Long.valueOf(v.toString())");
                    break;
                case "Float":
                    castTypes.add("Float.valueOf(v.toString())");
                    break;
                case "Double":
                    castTypes.add("Double.valueOf(v.toString())");
                    break;
                default:
                    throw new RuntimeException("In udaf state data not support " + types.get(1));
            }

            builder.setContainerState(
                    MessageFormat.format("Map<{0}>", types.stream().collect(Collectors.joining(", "))));
            builder.setCastStateMapType(castTypes.stream().collect(Collectors.joining(", ")));
        } catch (JepException e) {
            throw new RuntimeException("Jep get state map type failed.", e);
        }

    }

    /**
     * format java class name
     *
     * @param name eg. test_udf
     * @return eg. FlinkPyTestUdf
     */
    private String getClassName(String name) {
        String standardizedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name);
        return "HivePy" + standardizedName;
    }
}
