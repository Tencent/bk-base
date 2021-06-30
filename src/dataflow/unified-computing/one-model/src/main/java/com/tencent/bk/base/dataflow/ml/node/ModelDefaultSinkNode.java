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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TableType;
import com.tencent.bk.base.dataflow.core.topo.AbstractOutputChannel;
import com.tencent.bk.base.dataflow.core.topo.HDFSOutput;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import java.util.Map;

public class ModelDefaultSinkNode extends SinkNode {

    private AbstractOutputChannel<String> output;
    private TableType tableType;
    private long dtEventTimeStamp = 0L;

    public ModelDefaultSinkNode(ModelDefaultSinkNodeBuilder builder) {
        super(builder);
        this.output = builder.output;
        this.tableType = builder.tableType;
        this.dtEventTimeStamp = builder.dtEventTimeStamp;
    }

    public AbstractOutputChannel<String> getOutput() {
        return output;
    }

    public static class ModelDefaultSinkNodeBuilder extends AbstractBuilder {

        private AbstractOutputChannel<String> output;
        private String outputPath;
        private TableType tableType;
        private long dtEventTimeStamp = 0L;

        public ModelDefaultSinkNodeBuilder(Map<String, Object> info) {
            super(info);
            Map<String, Object> outputInfo = (Map<String, Object>) info.get("output");
            this.tableType = ConstantVar.TableType.valueOf(outputInfo
                    .getOrDefault("table_type", TableType.other.toString()).toString());
            this.outputPath = outputInfo.get("path").toString();
            String outputType = outputInfo.get("type").toString();
            switch (ConstantVar.ChannelType.valueOf(outputType)) {
                case hdfs:
                    this.output = new HDFSOutput(this.outputPath,
                            outputInfo.get("format").toString(),
                            outputInfo.get("mode").toString());
                    break;
                default:
                    throw new RuntimeException("Not support output type " + outputType);
            }
        }

        @Override
        public SinkNode build() {
            return new ModelDefaultSinkNode(this);
        }
    }
}
