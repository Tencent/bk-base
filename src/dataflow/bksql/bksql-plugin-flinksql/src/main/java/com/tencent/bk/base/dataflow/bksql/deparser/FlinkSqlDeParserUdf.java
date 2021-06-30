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

package com.tencent.bk.base.dataflow.bksql.deparser;

import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_ENV;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_FUNCTION_NAME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_PRODUCT_ENV;

import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.function.udf.UdfArgs;
import com.tencent.blueking.bksql.function.udf.UdfMetadata;
import com.tencent.blueking.bksql.function.udf.UdfParameterMetadata;
import com.tencent.bk.base.dataflow.bksql.udf.FlinkUtcToLocalUdf;
import com.tencent.bk.base.dataflow.bksql.util.FlinkUdf;
import com.tencent.bk.base.dataflow.bksql.util.FlinkUdfUsage;
import com.tencent.blueking.bksql.util.UdfType;
import com.typesafe.config.Config;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.codehaus.janino.SimpleCompiler;

public class FlinkSqlDeParserUdf {
    private Map<String, FlinkUdf> udfs;

    public static String getUDTFAliasName(String aliasName) {
        Pattern regexPattern = Pattern.compile("T\\((.*)\\)");
        Matcher matcher = regexPattern.matcher(aliasName);
        if (matcher.find()) {
            String group = matcher.group(1);
            String[] split = group.split(",");
            return Arrays.stream(split)
                    .map(identifier -> escape(identifier))
                    .collect(Collectors.joining(", "));
        }
        throw new RuntimeException("Not support alias name " + aliasName + ", must be T(...)");
    }

    public static String escape(String identifier) {
        // no escape for now
        if (identifier.endsWith("`") && identifier.startsWith("`")) {
            return identifier;
        }
        return String.format("`%s`", identifier);
    }

    /**
     * 初始化UDF,并更新了aggregationCallSet
     *
     * @param config
     * @param aggregationCallSet
     * @param udfMetadataConnector
     * @return
     */
    public FlinkSqlDeParserUdf initUdfInfo(Config config,
                                           Set<String> aggregationCallSet,
                                           BkdataUdfMetadataConnector udfMetadataConnector) {
        String env = config.hasPath(PROPERTY_KEY_ENV) ? config.getString(PROPERTY_KEY_ENV) : PROPERTY_KEY_PRODUCT_ENV;
        String functionName = config.hasPath(PROPERTY_KEY_FUNCTION_NAME)
                ? config.getString(PROPERTY_KEY_FUNCTION_NAME) : null;
        List<UdfMetadata> udfMetadataList = new ArrayList<>();
        if (functionName != null) {
            udfMetadataList = fetchUdfMetadata(new UdfArgs(env, functionName), udfMetadataConnector);
        }

        this.udfs = udfMetadataList.stream()
                .map(udfMetadata -> {
                    FlinkUdf.Builder udfBuilder = new FlinkUdf.Builder();
                    udfBuilder.type(udfMetadata.getUdfType());
                    if (udfMetadata.getUdfType() == UdfType.UDAF) {
                        aggregationCallSet.add(udfMetadata.getFunctionName());
                    }
                    udfBuilder.name(udfMetadata.getFunctionName());
                    udfBuilder.usages(buildFlinkUdfUsage(udfMetadata.getParameterMetadataList()));
                    return udfBuilder.create();
                })
                .collect(Collectors.toMap(FlinkUdf::getName, flinkUdf -> flinkUdf));
        return this;
    }

    /**
     * 注册UDF到flink运行环境
     *
     * @param tEnv
     * @param functions
     */
    public void registerUdf(StreamTableEnvironment tEnv,
                            Set<String> functions) {
        functions.forEach(func -> {
            if (udfs.containsKey(func)) {
                try {
                    if (udfs.get(func).getType() == UdfType.UDF) {
                        String code = generateUdfCode(formatClassName(func), udfs.get(func).getUsages());
                        ScalarFunction scalarFunction = (ScalarFunction) getUdfClass(
                                formatClassName(func), code, ScalarFunction.class.getClassLoader()).newInstance();
                        tEnv.registerFunction(func, scalarFunction);
                    } else if (udfs.get(func).getType() == UdfType.UDTF) {
                        String code = generateUdtfCode(formatClassName(func), udfs.get(func).getUsages());
                        TableFunction tableFunction = (TableFunction) getUdfClass(
                                formatClassName(func), code, TableFunction.class.getClassLoader())
                                .getConstructor(List.class)
                                .newInstance(udfs.get(func).getUsages().get(0).getOutputTypes());
                        tEnv.registerFunction(func, tableFunction);
                    } else if (udfs.get(func).getType() == UdfType.UDAF) {
                        String code = generateUdafCode(formatClassName(func), udfs.get(func).getUsages());
                        AggregateFunction aggregateFunction = (AggregateFunction) getUdfClass(
                                formatClassName(func), code, AggregateFunction.class.getClassLoader())
                                .getConstructor(List.class)
                                .newInstance(udfs.get(func).getUsages().get(0).getOutputTypes());
                        tEnv.registerFunction(func, aggregateFunction);
                    }
                } catch (NoSuchMethodException
                        | InstantiationException
                        | IllegalAccessException
                        | InvocationTargetException e) {
                    throw new RuntimeException("Failed to register udf function " + func, e);
                }
            }
        });
        // 解决Flink UTC时区问题
        tEnv.registerFunction("utc_to_local", new FlinkUtcToLocalUdf());
    }

    private List<UdfMetadata> fetchUdfMetadata(UdfArgs udfArgs, BkdataUdfMetadataConnector udfMetadataConnector) {
        try {
            return udfMetadataConnector.fetchUdfMetaData(udfArgs);
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.udf.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }

    private List<FlinkUdfUsage> buildFlinkUdfUsage(List<UdfParameterMetadata> parameterMetadata) {
        return parameterMetadata
                .stream()
                .map(udfParameterMetadata -> {
                    FlinkUdfUsage.Builder usageBuilder = new FlinkUdfUsage.Builder();
                    usageBuilder.inputTypes(udfParameterMetadata.getInputTypes());
                    usageBuilder.outputTypes(udfParameterMetadata.getOutputTypes());
                    return usageBuilder.create();
                })
                .collect(Collectors.toList());
    }

    private String formatClassName(String name) {
        return "Flink" + name;
    }

    private Class<?> getUdfClass(String name, String code, ClassLoader classLoader) {
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(classLoader);
        try {
            compiler.cook(code);
            return compiler.getClassLoader().loadClass(name);
        } catch (Exception e) {
            throw new RuntimeException("The code cannot be compiled. " + e);
        }
    }

    private String generateUdtfCode(String functionClassName, List<FlinkUdfUsage> oneUdtf) {
        if (oneUdtf.size() != 1) {
            throw new RuntimeException("Udtf not support more than one usage." + functionClassName);
        }

        String outputSize = String.valueOf(oneUdtf.get(0).getOutputTypes().size());
        String beginCode = ("public final class ${functionClassName} "
                + "extends com.tencent.bk.base.dataflow.bksql.udf.AbstractFlinkUdtf {\n")
                .replace("${functionClassName}", functionClassName);

        String constructorCode = "public ${functionClassName}(List<String> outputTypes) { super(outputTypes); } \n"
                .replace("${functionClassName}", functionClassName);

        String evalCode = "public void eval(${inputTypes}) { collect(new Row(${outputSize})); } \n"
                .replace("${inputTypes}", getUdfInputArgs(oneUdtf.get(0).getInputTypes()))
                .replace("${outputSize}", outputSize);

        String endCode = "} \n";

        String importCode = "import org.apache.flink.types.Row; \n"
                + "import java.util.List; \n";
        return importCode + beginCode + constructorCode + evalCode + endCode;
    }

    private String generateUdfCode(String functionClassName, List<FlinkUdfUsage> oneUdf) {
        String beginCode = ("public final class ${functionClassName} "
                + "extends org.apache.flink.table.functions.ScalarFunction {\n")
                .replace("${functionClassName}", functionClassName);
        StringBuilder evalCode = new StringBuilder();

        oneUdf.forEach(udf -> evalCode.append("public ${returnType} eval(${inputTypes}) { return null;} \n"
                .replace("${returnType}", getUdfOutputType(udf.getOutputTypes()))
                .replace("${inputTypes}", getUdfInputArgs(udf.getInputTypes()))));

        String endCode = "} \n";
        return beginCode + evalCode.toString() + endCode;
    }

    private String generateUdafCode(String functionClassName, List<FlinkUdfUsage> oneUdaf) {
        if (oneUdaf.size() != 1) {
            throw new RuntimeException("Udaf not support more than one usage. " + functionClassName);
        }

        String beginCode = ("public final class ${functionClassName} "
                + "extends com.tencent.bk.base.dataflow.bksql.udf.AbstractFlinkUdaf {\n")
                .replace("${functionClassName}", functionClassName);

        String accumulateCode = ("public void accumulate("
                + "com.tencent.bk.base.dataflow.bksql.udf.AbstractFlinkUdaf.DummyAccum acc, ${inputTypes}) { } \n")
                .replace("${inputTypes}", getUdfInputArgs(oneUdaf.get(0).getInputTypes()));

        String constructorCode = "public ${functionClassName}(List<String> outputTypes) {super(outputTypes); }\n"
                .replace("${functionClassName}", functionClassName);

        String endCode = "} \n";

        String importCode = "import java.util.List; \n";

        return importCode + beginCode + constructorCode + accumulateCode + endCode;
    }

    private String getUdfOutputType(List<String> ouputTypes) {
        return convertType(ouputTypes.get(0));
    }

    private String getUdfInputArgs(List<String> inputTypes) {
        List<String> oneArg = new ArrayList<>();
        IntStream.range(0, inputTypes.size())
                .forEach(i -> oneArg.add(MessageFormat.format("{0} {1}",
                        convertType(inputTypes.get(i)),
                        "arg" + i)));
        return oneArg.stream().collect(Collectors.joining(","));
    }

    private String convertType(String type) {
        switch (type.toLowerCase()) {
            case "string":
                return "String";
            case "boolean":
                return "Boolean";
            case "int":
            case "integer":
                return "Integer";
            case "long":
                return "Long";
            case "double":
                return "Double";
            case "string...":
                return "String...";
            default:
                throw new RuntimeException("Udf not support the type " + type);
        }
    }
}
