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

package com.tencent.bk.base.dataflow.core.topo;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SourceNode extends Node {

    private WindowConfig windowConfig;
    private List<NodeField> fields = new ArrayList<>();
    private AbstractInputChannel input;

    public SourceNode() {

    }

    public SourceNode(AbstractBuilder builder) {
        super(builder);
    }

    @Override
    public void map(Map<String, Object> info, Component jobType) {
        super.map(info, jobType);
        // 解析 window
        if (info.get("window") != null) {
            windowConfig = MapNodeUtil.mapWindowConfig(((Map<String, Object>) info.get("window")));
        }
        Map<String, Object> inputInfo = (Map<String, Object>) info.get("input");
        String type = inputInfo.get("type").toString();
        switch (ConstantVar.ChannelType.valueOf(type)) {
            case kafka:
                input = new KafkaInput(
                        inputInfo.get("cluster_domain").toString(),
                        Integer.parseInt(inputInfo.get("cluster_port").toString()));
                break;
            case hdfs:
                break;
            case string:
                break;
            default:
                throw new RuntimeException();
        }
    }

    public WindowConfig getWindowConfig() {
        return windowConfig;
    }

    public AbstractInputChannel getInput() {
        return input;
    }
}
