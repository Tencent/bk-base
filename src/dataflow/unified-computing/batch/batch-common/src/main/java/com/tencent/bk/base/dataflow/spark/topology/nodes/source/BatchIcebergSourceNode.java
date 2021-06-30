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

package com.tencent.bk.base.dataflow.spark.topology.nodes.source;

import com.tencent.bk.base.dataflow.spark.topology.BkFieldConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergInput;

import java.util.Map;

public class BatchIcebergSourceNode extends AbstractBatchSourceNode {

    protected Map<String, Object> icebergConf;
    protected BatchIcebergInput batchIcebergInput;
    protected boolean isAccumulateNotInRange;

    public BatchIcebergSourceNode(BatchIcebergSourceNodeBuilder builder) {
        super(builder);
        this.icebergConf = builder.icebergConf;
        this.inputTimeRangeString = builder.inputTimeRangeString;
        this.batchIcebergInput = builder.batchIcebergInput;
        this.isAccumulateNotInRange = builder.isAccumulateNotInRange;
    }

    public BatchIcebergInput getBatchIcebergInput() {
        return batchIcebergInput;
    }

    public Map<String, Object> getIcebergConf() {
        return this.icebergConf;
    }

    public boolean isAccumulateNotInRange() {
        return this.isAccumulateNotInRange;
    }

    public static class BatchIcebergSourceNodeBuilder extends AbstractBatchNodeBuilder {

        protected Map<String, Object> icebergConf;
        protected String inputTimeRangeString;
        protected BatchIcebergInput batchIcebergInput;
        protected BkFieldConstructor fieldConstructor;

        protected boolean isAccumulateNotInRange;

        public BatchIcebergSourceNodeBuilder(Map<String, Object> info) {
            super(info);
            this.fieldConstructor = new BkFieldConstructor();
        }

        @Override
        public BatchIcebergSourceNode build() {
            this.buildParams();
            return new BatchIcebergSourceNode(this);
        }

        protected void buildParams() {
            String icebergTableName = this.retrieveRequiredString(this.info,"iceberg_table_name").toString();
            long timeRangeStart = (long)this.retrieveRequiredString(this.info,"start_time");
            long timeRangeEnd = (long)this.retrieveRequiredString(this.info,"end_time");

            this.batchIcebergInput = new BatchIcebergInput(icebergTableName, timeRangeStart, timeRangeEnd);
            this.icebergConf = (Map<String, Object>)this.retrieveRequiredString(this.info,"iceberg_conf");
            this.fields = fieldConstructor.getRtField(this.nodeId);
            this.inputTimeRangeString = String.format("%d_%d", timeRangeStart / 1000, timeRangeEnd / 1000);
            this.isAccumulateNotInRange = false;
        }

        public void setFieldConstructor(BkFieldConstructor constructor) {
            this.fieldConstructor = constructor;
        }
    }
}
