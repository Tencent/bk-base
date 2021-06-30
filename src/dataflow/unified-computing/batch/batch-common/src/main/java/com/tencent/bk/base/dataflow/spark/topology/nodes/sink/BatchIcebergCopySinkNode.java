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

package com.tencent.bk.base.dataflow.spark.topology.nodes.sink;

import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;

import java.util.Map;

public class BatchIcebergCopySinkNode extends BatchIcebergSinkNode {

    protected BatchSinglePathOutput hdfsCopyOutput;
    protected boolean enableMoveToOrigin;

    public BatchIcebergCopySinkNode(BatchIcebergCopySinkNodeBuilder builder) {
        super(builder);
        this.hdfsCopyOutput = builder.hdfsCopyOutput;
        this.enableMoveToOrigin = builder.enableMoveToOrigin;
    }

    public boolean isEnableMoveToOrigin() {
        return enableMoveToOrigin;
    }

    public BatchSinglePathOutput getHdfsCopyOutput() {
        return hdfsCopyOutput;
    }

    public static class BatchIcebergCopySinkNodeBuilder extends BatchIcebergSinkNode.BatchIcebergSinkNodeBuilder {

        protected BatchSinglePathOutput hdfsCopyOutput;
        protected boolean enableMoveToOrigin;
        protected HDFSPathConstructor pathConstructor;

        public BatchIcebergCopySinkNodeBuilder(Map<String, Object> info) {
            super(info);
            this.pathConstructor = new HDFSPathConstructor();
        }

        @Override
        public BatchIcebergCopySinkNode build() {
            this.buildParams();
            BatchIcebergCopySinkNode sinkNode = new BatchIcebergCopySinkNode(this);
            return sinkNode;
        }

        @Override
        protected void buildParams() {
            super.buildParams();
            String copyRoot = this.pathConstructor.getHDFSCopyRoot(this.nodeId);
            String outputCopyPath = this.pathConstructor.timeToPath(copyRoot, this.dtEventTimeStamp);
            String copyFormat = this.retrieveRequiredString(this.info, "copy_format").toString();
            this.hdfsCopyOutput =
                new BatchSinglePathOutput(outputCopyPath, copyFormat, this.batchIcebergOutput.getMode());
            this.enableMoveToOrigin = Boolean.parseBoolean(this.info.get("dispatch_to_storage").toString());
            this.enableMaintainInterface = this.enableMoveToOrigin;
        }

        public void setPathConstructor(HDFSPathConstructor constructor) {
            this.pathConstructor = constructor;
        }

    }
}
