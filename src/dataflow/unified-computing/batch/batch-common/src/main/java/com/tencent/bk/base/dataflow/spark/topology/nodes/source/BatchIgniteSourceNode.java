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
import com.tencent.bk.base.dataflow.spark.topology.FieldConstructor;
import com.tencent.bk.base.dataflow.spark.topology.IgniteStorageConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;

import java.util.Map;

public class BatchIgniteSourceNode extends AbstractBatchSourceNode {

    protected String physicalName;

    protected Map<String, Object> connectionInfo;

    public BatchIgniteSourceNode(BatchIgniteSourceNodeBuilder builder) {
        super(builder);
        this.physicalName = builder.physicalName;
        this.connectionInfo = builder.connectionInfo;
    }

    public Map<String, Object> getConnectionInfo() {
        return connectionInfo;
    }

    public String getPhysicalName() {
        return physicalName;
    }

    public static class BatchIgniteSourceNodeBuilder extends AbstractBatchNodeBuilder {

        protected String physicalName;

        protected Map<String, Object> connectionInfo;

        protected FieldConstructor fieldConstructor;

        protected IgniteStorageConstructor igniteStorageConstructor;

        public BatchIgniteSourceNodeBuilder(Map<String, Object> info) {
            super(info);
            this.igniteStorageConstructor = new IgniteStorageConstructor(this.nodeId);
            this.fieldConstructor = new BkFieldConstructor();
        }

        @Override
        public BatchIgniteSourceNode build() {
            this.buildParams();
            return new BatchIgniteSourceNode(this);
        }

        protected void buildParams() {
            this.physicalName = this.igniteStorageConstructor.getPhysicalName();
            this.connectionInfo = this.igniteStorageConstructor.getConnectionInfoAsJava();
            this.fields = this.fieldConstructor.getRtField(this.nodeId);
        }

        public void setIgniteStorageConstructor(IgniteStorageConstructor constructor) {
            this.igniteStorageConstructor = constructor;
        }

        public void setFieldConstructor(BkFieldConstructor constructor) {
            this.fieldConstructor = constructor;
        }
    }
}
