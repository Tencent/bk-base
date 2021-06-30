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

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;

import java.util.Map;

public class BatchHDFSCopySinkNode extends BatchHDFSSinkNode {

    protected BatchSinglePathOutput hdfsCopyOutput;
    protected boolean enableMoveToOrigin;

    public BatchHDFSCopySinkNode(BatchHDFSCopySinkNodeBuilder builder) {
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

    public static class BatchHDFSCopySinkNodeBuilder extends BatchHDFSSinkNode.BatchHDFSSinkNodeBuilder {

        protected BatchSinglePathOutput hdfsCopyOutput;
        protected boolean enableMoveToOrigin;

        public BatchHDFSCopySinkNodeBuilder(Map<String, Object> info) {
            super(info);
        }

        @Override
        public BatchHDFSCopySinkNode build() {
            this.buildParams();
            BatchHDFSCopySinkNode sinkNode = new BatchHDFSCopySinkNode(this);
            return sinkNode;
        }

        @Override
        protected void buildParams() {
            super.buildParams();
            String copyRoot = this.pathConstructor.getHDFSCopyRoot(this.nodeId);
            String outputCopyPath = this.pathConstructor.timeToPath(copyRoot, this.dtEventTimeStamp);
            this.hdfsCopyOutput = new BatchSinglePathOutput(outputCopyPath, this.hdfsOutput.getFormat(),
                this.hdfsOutput.getMode());
            this.enableMoveToOrigin = Boolean.parseBoolean(this.info.get("dispatch_to_storage").toString());
            this.enableMaintainInterface = this.enableMoveToOrigin;
        }
    }
}
