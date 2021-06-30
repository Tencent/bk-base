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

package com.tencent.bk.base.dataflow.ml.node;

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.AbstractBatchSinkNode;
import java.util.Map;

public class ModelIceBergQuerySetSinkNode extends AbstractBatchSinkNode {

    protected BatchIcebergOutput batchIcebergOutput;
    protected Map<String, Object> icebergConf;

    public ModelIceBergQuerySetSinkNode(ModelIceBergQuerySetSinkNodeBuilder builder) {
        super(builder);
        this.batchIcebergOutput = builder.batchIcebergOutput;
        this.icebergConf = builder.icebergConf;
    }

    public BatchIcebergOutput getBatchIcebergOutput() {
        return batchIcebergOutput;
    }

    public Map<String, Object> getIcebergConf() {
        return this.icebergConf;
    }

    public static class ModelIceBergQuerySetSinkNodeBuilder extends AbstractBatchNodeBuilder {

        protected BatchIcebergOutput batchIcebergOutput;
        protected Map<String, Object> icebergConf;

        public ModelIceBergQuerySetSinkNodeBuilder(Map<String, Object> info) {
            super(info);
        }

        @Override
        protected void initBuilder(Map<String, Object> info) {
            String icebergTableName = this.retrieveRequiredString(info, "iceberg_table_name").toString();
            String outputMode = this.retrieveRequiredString(info, "output_mode").toString();
            this.batchIcebergOutput = new BatchIcebergOutput(icebergTableName, outputMode);
            this.icebergConf = (Map<String, Object>) this.retrieveRequiredString(info, "iceberg_conf");
        }

        @Override
        public ModelIceBergQuerySetSinkNode build() {
            return new ModelIceBergQuerySetSinkNode(this);
        }
    }

}
