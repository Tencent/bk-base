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

package com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc;

import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.params.MLSqlFunctionInputParams;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.params.MLSqlFunctionOutputParams;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTypeFactory;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

public class MLSqlModelFunction extends SqlFunction {

    protected static MLSqlTypeFactory typeFactory = new MLSqlTypeFactory(RelDataTypeSystem.DEFAULT);
    protected List<String> needInterpreterCols;
    protected String outputColName;
    protected List<String> mustInputs = new ArrayList<>();
    protected String algorithmType = "transform";
    private Map<String, RelDataType> inputs;
    private Map<String, RelDataType> outputs;

    public MLSqlModelFunction() {
        super("MODEL_FUNC",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createSqlType(SqlTypeName.ANY)),
                null,
                OperandTypes.repeat(SqlOperandCountRanges.from(1), OperandTypes.ANY),
                SqlFunctionCategory.SYSTEM);
    }

    /**
     * 使用基本信息构造MLSqlModelFunction
     *
     * @param name 名称
     * @param outputTypeList 输出参数类型
     * @param outputNameList 输出参数列表
     * @param inputNameList 输入参数列表
     * @param inputTypeList 输入参数类型
     * @param needInterpreterCols 需要特殊翻译处理的列
     * @param outputColName 唯一输出列名
     */
    public MLSqlModelFunction(String name,
            List<RelDataType> outputTypeList,
            List<String> outputNameList,
            List<RelDataType> inputTypeList,
            List<String> inputNameList,
            List<String> needInterpreterCols,
            String outputColName) {
        super(name.toUpperCase(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createStructType(
                                outputTypeList,
                                outputNameList)),
                null,
                OperandTypes.repeat(SqlOperandCountRanges.from(inputNameList.size()), OperandTypes.ANY),
                SqlFunctionCategory.SYSTEM);
        inputs = new LinkedHashMap<>();
        outputs = new LinkedHashMap<>();
        for (int ind = 0; ind < inputNameList.size(); ind++) {
            inputs.put(inputNameList.get(ind), inputTypeList.get(ind));
        }
        for (int ind = 0; ind < outputNameList.size(); ind++) {
            outputs.put(outputNameList.get(ind), outputTypeList.get(ind));
        }
        this.needInterpreterCols = needInterpreterCols;
        if (StringUtils.isBlank(outputColName)) {
            this.outputColName = outputNameList.get(0);
        } else {
            this.outputColName = outputColName;
        }
    }

    /**
     * 使用基本信息构造MLSqlModelFunction
     *
     * @param name 名称
     * @param inputParams 输入参数信息
     * @param outputParams 输出参数信息
     */
    public MLSqlModelFunction(String name, MLSqlFunctionInputParams inputParams,
            MLSqlFunctionOutputParams outputParams) {
        super(name.toUpperCase(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createStructType(
                                outputParams.getOutputTypeList(),
                                outputParams.getOutputNameList())),
                null,
                OperandTypes
                        .repeat(SqlOperandCountRanges.from(inputParams.getInputNameList().size()), OperandTypes.ANY),
                SqlFunctionCategory.SYSTEM);
        inputs = new LinkedHashMap<>();
        outputs = new LinkedHashMap<>();
        for (int ind = 0; ind < inputParams.getInputNameList().size(); ind++) {
            inputs.put(inputParams.getInputNameList().get(ind), inputParams.getInputTypeList().get(ind));
        }
        for (int ind = 0; ind < outputParams.getOutputNameList().size(); ind++) {
            outputs.put(outputParams.getOutputNameList().get(ind), outputParams.getOutputTypeList().get(ind));
        }
        this.needInterpreterCols = inputParams.getNeedInterpreterCols();
        if (StringUtils.isBlank(outputParams.getOutputColName())) {
            this.outputColName = outputParams.getOutputNameList().get(0);
        } else {
            this.outputColName = outputParams.getOutputColName();
        }
        this.mustInputs = inputParams.getMustInputs();
    }

    public List<String> getNeedInterpreterCols() {
        return this.needInterpreterCols;
    }

    public String getOutputColName() {
        return this.outputColName;
    }

    public Map<String, RelDataType> getInputs() {
        return this.inputs;
    }

    public Map<String, RelDataType> getOutputs() {
        return outputs;
    }

    public RelDataType getReturnType() {
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
    }

    /**
     * 检查用户输入参数里是否包含必填参数
     *
     * @param userInputKeys 用户输入参数集合
     * @return True 或 False
     */
    public boolean checkMustInput(Set<String> userInputKeys) {
        for (String item : this.mustInputs) {
            boolean itemExists = false;
            if (item.contains("|")) {
                String[] subItems = item.split("\\|");
                for (String subItem : subItems) {
                    if (userInputKeys.contains(subItem)) {
                        itemExists = true;
                    }
                }
            } else if (userInputKeys.contains(item)) {
                itemExists = true;
            }
            if (!itemExists) {
                // 属性不存在
                throw new MessageLocalizedExceptionV1("param.not.input.error",
                        new Object[]{item},
                        MLSqlDeParser.class,
                        ErrorCode.PARAM_NOT_INPUT_ERROR);
            }
        }
        return true;
    }

    /**
     * 检查模型的输入与输出是否合法
     */
    public boolean checkParameters(Map<String, Object> args,
            List<RelDataTypeField> fieldList) throws Exception {
        Map<String, RelDataType> paramTypeMap = this.getInputs();
        Map<String, RelDataType> fieldTypeMap = new HashMap<>();
        for (RelDataTypeField field : fieldList) {
            fieldTypeMap.put(field.getName(), field.getType());
        }
        for (Map.Entry<String, Object> entry : args.entrySet()) {
            String paramName = entry.getKey();
            Object value = entry.getValue();
            if (fieldTypeMap.containsKey(value) && paramTypeMap.containsKey(paramName)) {
                //参数值为源表列
                String fieldType = fieldTypeMap.get(value).getSqlTypeName().getName();
                String paramType = paramTypeMap.get(paramName).getSqlTypeName().getName();
                if (!"ANY".equals(paramType) && !fieldType.equalsIgnoreCase(paramType)) {
                    throw new MessageLocalizedExceptionV1("param.type.not.match.error",
                            new Object[]{paramName, paramTypeMap.get(paramName)},
                            MLSqlDeParser.class,
                            ErrorCode.PARAM_TYPE_NOT_MATCH_ERROR);
                }
            } else {
                //值不是源表列，则为

            }

        }
        return true;
    }

    public String getAlgorithmType() {
        return this.algorithmType;
    }

    public void setAlgorithmType(String algorithmType) {
        this.algorithmType = algorithmType;
    }

}
