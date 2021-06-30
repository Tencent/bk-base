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

import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTypeFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class MLSqlModelLateralFunciton extends SqlFunction {

    protected static MLSqlTypeFactory typeFactory = new MLSqlTypeFactory(RelDataTypeSystem.DEFAULT);
    protected List<String> needInterpreterCols;
    protected String outputColName;
    private Map<String, RelDataType> inputs;
    private Map<String, RelDataType> outputs;

    public MLSqlModelLateralFunciton() {
        super("LATERAL_MODEL_FUNC",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createStructType(
                                ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY),
                                        typeFactory.createSqlType(SqlTypeName.ANY)),
                                ImmutableList.of("os", "test"))),
                null,
                OperandTypes.repeat(SqlOperandCountRanges.from(1), OperandTypes.ANY),
                SqlFunctionCategory.SYSTEM);
    }

    public MLSqlModelLateralFunciton(String functionName, List<RelDataType> outputTypeList,
            List<String> outputNameList) {
        super(functionName.toUpperCase(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createStructType(outputTypeList, outputNameList)),
                null,
                OperandTypes.repeat(SqlOperandCountRanges.from(3), OperandTypes.ANY),
                SqlFunctionCategory.SYSTEM);
    }

    public MLSqlModelLateralFunciton(List<RelDataType> typeList, List<String> nameList) {
        super("LATERAL_MODEL_FUNC_" + typeList.size(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(
                        typeFactory -> typeFactory.createStructType(
                                typeList,
                                nameList)),
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
    public MLSqlModelLateralFunciton(String name,
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
        inputs = new HashMap<>();
        outputs = new HashMap<>();
        for (int ind = 0; ind < inputNameList.size(); ind++) {
            inputs.put(inputNameList.get(ind), inputTypeList.get(ind));
        }
        for (int ind = 0; ind < outputNameList.size(); ind++) {
            outputs.put(outputNameList.get(ind), outputTypeList.get(ind));
        }
        this.needInterpreterCols = needInterpreterCols;
        this.outputColName = outputColName;
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

}
