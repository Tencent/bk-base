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

package com.tencent.bk.base.dataflow.ml.spark.sink;

import com.tencent.bk.base.datahub.iceberg.SparkUtils;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergOutput;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

public class MLIcebergQuerySetSink extends MLQuerySetSink {

    private DataFrameWrapper dataset;
    private ModelIceBergQuerySetSinkNode node;

    public MLIcebergQuerySetSink(ModelIceBergQuerySetSinkNode node, DataFrameWrapper dataset) {
        this.dataset = dataset;
        this.node = node;
    }

    @Override
    public void createNode() {
        BatchIcebergOutput batchIcebergOutput = this.node.getBatchIcebergOutput();
        String mode = batchIcebergOutput.getMode();
        SaveMode saveMode = null;
        if (mode.equalsIgnoreCase(SaveMode.Overwrite.name())) {
            saveMode = SaveMode.Overwrite;
        } else {
            saveMode = SaveMode.Append;
        }
        Dataset<Row> innerData = dataset.getDataFrame();
        innerData.show();
        Map<String, DataType> dataSchemas = Arrays.stream(innerData.schema().fields())
                .collect(Collectors.toMap(StructField::name, StructField::dataType));
        String[] columns =
                this.node.getFields().stream().map(field -> this.castFieldExpr(field, dataSchemas))
                        .toArray(String[]::new);

        Dataset<Row> select = innerData.selectExpr(columns);
        SparkUtils.writeTable(batchIcebergOutput.getOutputInfo(), this.node.getIcebergConf(), select, saveMode);
    }
}
