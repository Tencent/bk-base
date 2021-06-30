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

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchPathListInput;

import java.util.List;
import java.util.Map;

public class BatchHDFSCopySourceNode extends BatchHDFSSourceNode {
    protected BatchPathListInput hdfsCopyInput;

    public BatchHDFSCopySourceNode(BatchHDFSCopySourceNodeBuilder builder) {
        super(builder);
        this.hdfsCopyInput = builder.hdfsCopyInput;
    }

    public BatchPathListInput getHdfsCopyInput() {
        return hdfsCopyInput;
    }

    public static class BatchHDFSCopySourceNodeBuilder extends BatchHDFSSourceNodeBuilder {

        protected BatchPathListInput hdfsCopyInput;

        public BatchHDFSCopySourceNodeBuilder(Map<String, Object> info) {
            super(info);
        }

        @Override
        public BatchHDFSCopySourceNode build() {
            this.buildParams();
            return new BatchHDFSCopySourceNode(this);
        }

        @Override
        protected void buildParams() {
            super.buildParams();
            String copyRootPath = this.pathConstructor.getHDFSCopyRoot(this.nodeId);

            long startTimeMillis = (long)this.retrieveRequiredString(this.info,"start_time");
            long endTimeMillis = (long)this.retrieveRequiredString(this.info,"end_time");

            List<String> copyPaths =
                pathConstructor.getAllTimePerHourAsJava(copyRootPath, startTimeMillis, endTimeMillis, 1);

            this.hdfsCopyInput = new BatchPathListInput(copyPaths);
        }
    }
}
