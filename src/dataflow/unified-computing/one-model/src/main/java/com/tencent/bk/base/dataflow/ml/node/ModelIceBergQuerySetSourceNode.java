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

import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode;
import java.util.Map;

public class ModelIceBergQuerySetSourceNode extends AbstractBatchSourceNode {

    protected Map<String, Object> icebergConf;
    protected BatchIcebergInput batchIcebergInput;

    public ModelIceBergQuerySetSourceNode(ModelIceBergQuerySetSourceNodeBuilder builder) {
        super(builder);
        this.icebergConf = builder.icebergConf;
        this.batchIcebergInput = builder.batchIcebergInput;
    }

    public BatchIcebergInput getBatchIcebergInput() {
        return batchIcebergInput;
    }

    public Map<String, Object> getIcebergConf() {
        return this.icebergConf;
    }

    public static class ModelIceBergQuerySetSourceNodeBuilder extends AbstractBatchNodeBuilder {

        protected Map<String, Object> icebergConf;
        protected BatchIcebergInput batchIcebergInput;

        public ModelIceBergQuerySetSourceNodeBuilder(Map<String, Object> info) {
            super(info);
        }

        @Override

        protected void initBuilder(Map<String, Object> info) {
            String icebergTableName = this.retrieveRequiredString(info, "iceberg_table_name").toString();
            this.icebergConf = (Map<String, Object>) this.retrieveRequiredString(info, "iceberg_conf");
            this.batchIcebergInput = new BatchIcebergInput(icebergTableName);
        }

        @Override
        public ModelIceBergQuerySetSourceNode build() {
            return new ModelIceBergQuerySetSourceNode(this);
        }
    }
}
