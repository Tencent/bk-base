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

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * ref:http://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression
 */
public class MLSqlLogisticRegressionFunction extends MLSqlModelFunction {

    public MLSqlLogisticRegressionFunction(String modelName,
            List<RelDataType> outputTypeList,
            List<String> outputNameList,
            List<RelDataType> inputTypeList,
            List<String> inputNameList,
            List<String> needInterpreterCols,
            String outputColName) {
        super(modelName, outputTypeList, outputNameList, inputTypeList, inputNameList, needInterpreterCols,
                outputColName);
        this.algorithmType = "classify";
    }

    /**
     * 创建LogisticRegression算法对象
     */
    public static MLSqlLogisticRegressionFunction createFunction() {
        List<String> inputNames = new ArrayList<>();
        inputNames.add("features_col");
        inputNames.add("label_col");
        inputNames.add("max_iter");
        inputNames.add("reg_param");
        inputNames.add("elastic_net_param");
        inputNames.add("family");
        List<RelDataType> inputTypes = new ArrayList<>();
        inputTypes.add(typeFactory.createJavaType(Vector.class));
        inputTypes.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
        inputTypes.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
        inputTypes.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
        inputTypes.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
        inputTypes.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        List<String> outputNames = new ArrayList<>();
        //这类模型训练的输出就是一个hdfs的路径即可，没有output_col这类字段
        outputNames.add("prediction_col");
        List<RelDataType> outputTypes = new ArrayList<>();
        outputTypes.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
        List<String> needInterpreterCols = new ArrayList<>();
        needInterpreterCols.add("features_col");
        String modelName = "LogisticRegression";
        return new MLSqlLogisticRegressionFunction(modelName, outputTypes, outputNames, inputTypes, inputNames,
                needInterpreterCols, null);
    }
}
