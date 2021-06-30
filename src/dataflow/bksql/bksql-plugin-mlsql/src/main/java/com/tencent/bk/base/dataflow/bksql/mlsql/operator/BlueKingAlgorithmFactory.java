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

package com.tencent.bk.base.dataflow.bksql.mlsql.operator;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.MLSqlLocalizedException;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.params.MLSqlFunctionInputParams;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.params.MLSqlFunctionOutputParams;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTypeFactory;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlueKingAlgorithmFactory {

    private static final Logger logger = LoggerFactory.getLogger(BlueKingAlgorithmFactory.class);
    private static String baseUrl;
    private static MLSqlTypeFactory typeFactory = new MLSqlTypeFactory(RelDataTypeSystem.DEFAULT);

    public static void init(String esbUrl) {
        if (StringUtils.isBlank(esbUrl)) {
            throw new MLSqlLocalizedException("miss.config.error",
                    new Object[]{"algorithmMetaUrlPattern"},
                    MLSqlDeParser.class,
                    ErrorCode.MISS_CONFIG_ERROR);
        }
        if (!StringUtils.isBlank(esbUrl)) {
            baseUrl = esbUrl;
        }
    }

    /**
     * 根据算法名称获取对应的算法对象
     *
     * @param name 算法名称
     * @return 根据名称获取得到的算法信息
     */
    public static MLSqlModelFunction getAlgorithmFunction(String name) {
        JSONObject args = new JSONObject();
        if (!name.contains(".")) {
            //算法名称内不含有框架，默认为spark
            name = "spark_" + name;
        } else {
            name = name.replace(".", "_");
        }
        args.put("?:filter", "algorithm_name='" + name + "'");
        args.put("?:typed", "AlgorithmVersion");
        args.put("config", true);
        JSONArray array = new JSONArray();
        array.put(args);
        JSONObject paramObject = new JSONObject();
        paramObject.put("retrieve_args", array);
        try {
            logger.info("alg url:" + baseUrl);
            HttpResponse<JsonNode> response = Unirest.post(baseUrl)
                    .header("Content-Type", "application/json")
                    .body(paramObject)
                    .asJson();
            JSONObject tableObject = response.getBody().getObject();
            if (tableObject.getBoolean("result")) {
                JSONObject dataObject = tableObject.getJSONObject("data");
                JSONArray querySetArray = dataObject.getJSONArray("AlgorithmVersion");
                if (querySetArray.length() > 0) {
                    String config = querySetArray.getJSONObject(0).getString("config");
                    config = config.substring(1, config.length() - 1);
                    config = config.replace("\\", "");
                    JSONObject configObject = new JSONObject(config);
                    return generateFunction(name, configObject);
                }
            }
        } catch (Throwable e) {
            logger.error("Get algorithm error", e);
        }
        return null;
    }

    public static SqlTypeName getSqlTypeName(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "string":
                return SqlTypeName.VARCHAR;
            case "int":
                return SqlTypeName.INTEGER;
            case "long":
                return SqlTypeName.BIGINT;
            case "float":
                return SqlTypeName.FLOAT;
            case "double":
                return SqlTypeName.DOUBLE;
            default:
                throw new RuntimeException("Don't support the type " + fieldType.toLowerCase());
        }
    }

    /**
     * 根据从算法库中获取的信息，初始化算法对象的各个属性
     *
     * @param fieldObject 算法库中获取的输入参数信息
     * @param nameList 算法对象的参数列表
     * @param typeList 算法对象的参数类型列表
     * @param needInterpreterCols 算法对象中需要特征翻译处理的参数列表
     * @param mustInputCols 算法对象中必须输入的参数列表
     */
    public static void initAlgParameters(JSONObject fieldObject, List<String> nameList, List<RelDataType> typeList,
            List<String> needInterpreterCols, List<String> mustInputCols) {
        String fieldType = fieldObject.getString("field_type");
        SqlTypeName type = getSqlTypeName(fieldType);
        RelDataType relDataType = null;
        String compositeType = null;
        if (fieldObject.has("composite_type")) {
            compositeType = fieldObject.getString("composite_type");
        }
        if (!StringUtils.isBlank(compositeType)) {
            if (RelParser.PARAM_TYPE_VECTOR.equalsIgnoreCase(compositeType)) {
                // 向量
                relDataType = typeFactory.createJavaType(Vector.class);
            } else {
                // 数组
                relDataType = typeFactory.createArrayType(typeFactory.createSqlType(type), -1);
            }
        } else {
            //非复合类型，即不是vector，也不是array,则此字段为原始类型
            relDataType = typeFactory.createSqlType(type);
        }
        boolean required = false;
        if (fieldObject.has("required")) {
            required = fieldObject.getBoolean("required");
        }
        boolean needInterpreter = false;
        if (fieldObject.has("need_interprete")) {
            needInterpreter = fieldObject.getBoolean("need_interprete");
        }
        String fieldName = fieldObject.getString("real_name");
        nameList.add(fieldName);
        typeList.add(relDataType);
        if (required) {
            mustInputCols.add(fieldName);
        }
        if (needInterpreter) {
            needInterpreterCols.add(fieldName);
        }

    }

    /**
     * 根据算法的名称，获取算法的类型，如分类算法，特征转换算法，聚类算法等
     *
     * @param algorithmName 算法名称
     * @return 算法的分类，如分类，特征转换，聚类等
     */
    public static String getAlgorithmType(String algorithmName) {
        JSONObject args = new JSONObject();
        args.put("?:filter", "algorithm_name='" + algorithmName + "'");
        args.put("?:typed", "Algorithm");
        args.put("algorithm_type", true);
        JSONArray array = new JSONArray();
        array.put(args);
        JSONObject paramObject = new JSONObject();
        paramObject.put("retrieve_args", array);
        try {
            logger.info("alg url:" + baseUrl);
            HttpResponse<JsonNode> response = Unirest.post(baseUrl)
                    .header("Content-Type", "application/json")
                    .body(paramObject)
                    .asJson();
            JSONObject tableObject = response.getBody().getObject();
            if (tableObject.getBoolean("result")) {
                JSONObject dataObject = tableObject.getJSONObject("data");
                JSONArray querySetArray = dataObject.getJSONArray("Algorithm");
                if (querySetArray.length() > 0) {
                    return querySetArray.getJSONObject(0).getString("algorithm_type");
                }
            }
            return null;
        } catch (Exception e) {
            logger.info("Get algorithm type error", e);
            return null;
        }
    }

    /**
     * 解析输入配置中的参数信息，生成函数对象
     *
     * @param name 算法名称
     * @param configObject 配置对象
     * @return 算法对象
     */
    public static MLSqlModelFunction generateFunction(String name, JSONObject configObject) {

        List<String> inputNames = new ArrayList<>();
        List<RelDataType> inputTypes = new ArrayList<>();
        List<String> outputNames = new ArrayList<>();
        List<RelDataType> outputTypes = new ArrayList<>();
        List<String> needInterpreterCols = new ArrayList<>();
        List<String> mustInputCols = new ArrayList<>();
        //初始化输入参数
        initInputParams(configObject, inputNames, inputTypes, needInterpreterCols, mustInputCols);
        //初始化输出参数
        JSONArray predictOutputArgs = configObject.getJSONArray("predict_output");
        for (int ind = 0; ind < predictOutputArgs.length(); ind++) {
            initAlgParameters(predictOutputArgs.getJSONObject(ind), outputNames, outputTypes,
                    needInterpreterCols, mustInputCols);
        }
        String outputCol = predictOutputArgs.getJSONObject(0).getString("real_name");
        MLSqlFunctionInputParams inputParams = new MLSqlFunctionInputParams(inputTypes, inputNames,
                needInterpreterCols, mustInputCols);
        MLSqlFunctionOutputParams outputParams = new MLSqlFunctionOutputParams(outputTypes, outputNames,
                outputCol);
        MLSqlModelFunction function = new MLSqlModelFunction(name, inputParams, outputParams);
        String algorithmType = getAlgorithmType(name);
        if (!StringUtils.isBlank(algorithmType)) {
            function.setAlgorithmType(algorithmType);
        }
        return function;
    }

    /**
     * 初始化算法所有的输入参数参数
     *
     * @param configObject 配置信息
     * @param inputNames 输入参数列表
     * @param inputTypes 输入参数类型列表
     * @param needInterpreterCols 需要特征翻译处理的参数列表
     * @param mustInputCols 用户必须输入的参数列表
     */
    public static void initInputParams(JSONObject configObject,
            List<String> inputNames, List<RelDataType> inputTypes,
            List<String> needInterpreterCols, List<String> mustInputCols) {
        JSONObject featureMappingObject = configObject.getJSONObject("feature_columns_mapping")
                .getJSONObject("spark");
        JSONArray multiFeatureColums = new JSONArray();
        JSONArray singleFeatureColums = new JSONArray();
        if (featureMappingObject.has("multi")) {
            multiFeatureColums = featureMappingObject.getJSONArray("multi");
        }
        if (featureMappingObject.has("single")) {
            singleFeatureColums = featureMappingObject.getJSONArray("single");
        }
        //初始化label信息
        JSONArray labelColumns = configObject.getJSONArray("label_columns");
        for (int ind = 0; ind < labelColumns.length(); ind++) {
            initAlgParameters(labelColumns.getJSONObject(ind), inputNames, inputTypes, needInterpreterCols,
                    mustInputCols);
        }
        //初始化用户参数
        JSONArray trainArgs = configObject.getJSONArray("training_args");
        for (int ind = 0; ind < trainArgs.length(); ind++) {
            initAlgParameters(trainArgs.getJSONObject(ind), inputNames, inputTypes, needInterpreterCols,
                    mustInputCols);
        }
        //初始化特征列
        initFeatureColumns(multiFeatureColums, singleFeatureColums, inputNames,
                inputTypes, needInterpreterCols, mustInputCols);

    }

    /**
     * 初始化特征列
     *
     * @param multiFeatureColums 多输入参数名称
     * @param singleFeatureColums 单输入参数名称
     * @param inputNames 输入参数列表
     * @param inputTypes 输入参数类型列表
     * @param needInterpreterCols 需要特征翻译处理的参数列表
     * @param mustInputCols 用户必须输入的参数列表
     */
    public static void initFeatureColumns(JSONArray multiFeatureColums, JSONArray singleFeatureColums,
            List<String> inputNames, List<RelDataType> inputTypes,
            List<String> needInterpreterCols, List<String> mustInputCols) {
        for (int ind = 0; ind < multiFeatureColums.length(); ind++) {
            initAlgParameters(multiFeatureColums.getJSONObject(ind), inputNames, inputTypes,
                    needInterpreterCols, mustInputCols);
        }
        for (int ind = 0; ind < singleFeatureColums.length(); ind++) {
            initAlgParameters(singleFeatureColums.getJSONObject(ind), inputNames, inputTypes,
                    needInterpreterCols, mustInputCols);
        }
    }
}
