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

package com.tencent.bk.base.dataflow.bksql.udaf;

import org.apache.spark.sql.BkSparkUdf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class BkSparkSqlEmptyUDAF extends UserDefinedAggregateFunction {

    private final String[] inputs;
    private final String returnType;

    public BkSparkSqlEmptyUDAF(String[] inputs, String returnType) {
        this.inputs = inputs;
        this.returnType = returnType;
    }

    @Override
    public StructType inputSchema() {
        List<StructField> fields = new ArrayList();
        for (int i = 0; i < inputs.length; i++) {
            String type = inputs[i];
            fields.add(DataTypes.createStructField(
                    "arg_" + i,
                    BkSparkUdf.toDataType(type),
                    true));
        }
        return DataTypes.createStructType(fields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> fields = new ArrayList();
        fields.add(DataTypes.createStructField(
                "buffer",
                BkSparkUdf.toDataType(returnType),
                true));
        return DataTypes.createStructType(fields);
    }

    @Override
    public DataType dataType() {
        return BkSparkUdf.toDataType(returnType);
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    }

    @Override
    public Object evaluate(Row buffer) {
        return null;
    }
}
