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

import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ModelTransformNode extends TransformNode {

    private ModelProcessor processor;

    private Map<String, Interpreter> interpreter;

    public ModelTransformNode(ModelTransformNodeBuilder builder) {
        super(builder);
        this.processor = builder.processor;
        this.interpreter = builder.interpreter;
    }

    public ModelProcessor getProcessor() {
        return processor;
    }

    public Map<String, Interpreter> getInterpreter() {
        return interpreter;
    }

    public static class ModelTransformNodeBuilder extends AbstractBuilder {

        private ModelProcessor processor;
        private Map<String, Interpreter> interpreter;

        /**
         * 构建transform节点的builder
         *
         * @param info 节点信息
         */
        public ModelTransformNodeBuilder(Map<String, Object> info) {
            super(info);
            Map<String, Object> processorInfo = (Map<String, Object>) info.get("processor");
            String name = processorInfo.get("name").toString();
            String type = processorInfo.get("type").toString();
            Map<String, Object> args = new HashMap<>();
            if (null != processorInfo.get("args")) {
                args = (Map<String, Object>) processorInfo.get("args");
            }
            this.processor = new ModelProcessor(name, type, args);
            // interpreter
            if (null != info.get("interpreter")) {
                Map<String, Map<String, Object>> interpreterInfo = (LinkedHashMap<String, Map<String, Object>>) info
                        .get("interpreter");
                this.interpreter = interpreterInfo.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Entry::getKey,
                                value -> new Interpreter(
                                        value.getValue().get("value"),
                                        value.getValue().get("implement").toString()),
                                (u, v) -> {
                                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                                },
                                LinkedHashMap::new));
            }
        }

        @Override
        public TransformNode build() {
            return new ModelTransformNode(this);
        }
    }
}
