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
import com.tencent.bk.base.dataflow.core.topo.HDFSInput;
import com.tencent.bk.base.dataflow.core.topo.AbstractInputChannel;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelDefaultSourceNode extends SourceNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelDefaultSourceNode.class);
    private AbstractInputChannel<String> input;
    private ConstantVar.TableType tableType;

    public ModelDefaultSourceNode(ModelDefaultSourceNodeBuilder builder) {
        super(builder);
        this.input = builder.input;
        this.tableType = builder.tableType;
    }

    public AbstractInputChannel<String> getInput() {
        return input;
    }

    public static class ModelDefaultSourceNodeBuilder extends AbstractBuilder {

        private AbstractInputChannel<String> input;
        private ConstantVar.TableType tableType;

        public ModelDefaultSourceNodeBuilder(Map<String, Object> info) {
            super(info);
            Map<String, Object> inputInfo = (Map<String, Object>) info.get("input");
            String root = inputInfo.get("path").toString();
            this.tableType = ConstantVar.TableType.valueOf(inputInfo
                    .getOrDefault("table_type", TableType.other.toString()).toString());
            String inputType = inputInfo.get("type").toString();
            switch (ConstantVar.ChannelType.valueOf(inputType)) {
                case hdfs:
                    this.input = new HDFSInput(root, inputInfo.get("format").toString());
                    break;
                default:
                    throw new RuntimeException("Not support input type " + inputType);
            }
            LOGGER.info("Read source path:" + root);
        }

        @Override
        public SourceNode build() {
            return new ModelDefaultSourceNode(this);
        }
    }
}
