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

package com.tencent.bk.base.dataflow.bksql.mlsql.parser;

import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlProcessorType;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;

public class FunctionParserParams {

    private MLSqlProcessorType type = MLSqlProcessorType.trained_run;
    private String modelName = null;
    private String algorithmName = null;
    private String evaluateFunction = null;
    private String evaluateLabel = null;
    private MLSqlModelFunction function = null;
    private MLSqlModel model = null;


    public FunctionParserParams() {

    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public void setAlgorithmName(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    public String getEvaluateFunction() {
        return evaluateFunction;
    }

    public void setEvaluateFunction(String evaluateFunction) {
        this.evaluateFunction = evaluateFunction;
    }

    public String getEvaluateLabel() {
        return evaluateLabel;
    }

    public void setEvaluateLabel(String evaluateLabel) {
        this.evaluateLabel = evaluateLabel;
    }

    public MLSqlModelFunction getFunction() {
        return function;
    }

    public void setFunction(MLSqlModelFunction function) {
        this.function = function;
    }

    public MLSqlModel getModel() {
        return model;
    }

    public void setModel(MLSqlModel model) {
        this.model = model;
    }

    public MLSqlProcessorType getType() {
        return type;
    }

    public void setType(MLSqlProcessorType type) {
        this.type = type;
    }
}
