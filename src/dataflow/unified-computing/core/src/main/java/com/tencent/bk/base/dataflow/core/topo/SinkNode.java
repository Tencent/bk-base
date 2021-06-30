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

import com.tencent.bk.base.dataflow.core.configuration.ConfigurationUtils;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SinkNode extends Node {

    /**
     * storage args
     */
    private List<String> storages = new ArrayList<String>();
    private String storagesArgs;

    private AbstractOutputChannel output;

    public SinkNode() {
    }

    public SinkNode(AbstractBuilder builder) {
        super(builder);
    }

    @Override
    public void map(Map<String, Object> info, Component jobType) {
        super.map(info, jobType);
        if (info.get("storage") != null) {
            storages = ((List<String>) ((Map<String, Object>) info.get("storage")).get("storages"));
            Object storagesArgs = ((Map<String, Object>) info.get("storage")).get("storages_args");
            if (storagesArgs != null) {
                this.storagesArgs = storagesArgs.toString();
            }
        }
        Map<String, Object> outputInfo = (Map<String, Object>) info.get("output");
        String type = outputInfo.get("type").toString();
        switch (ConstantVar.ChannelType.valueOf(type)) {
            case kafka:
                output = new KafkaOutput(
                        outputInfo.get("cluster_domain").toString(),
                        Integer.parseInt(outputInfo.get("cluster_port").toString()));
                // conf 允许为 Null
                output.setConf(ConfigurationUtils.createConfiguration(outputInfo.get("conf")));
                break;
            default:
                throw new RuntimeException();
        }
    }

    public List<String> getStorages() {
        return storages;
    }

    public void setStorages(List<String> storages) {
        this.storages = storages;
    }

    public String getStoragesArgs() {
        return storagesArgs;
    }

    public void setStoragesArgs(String storagesArgs) {
        this.storagesArgs = storagesArgs;
    }

    public AbstractOutputChannel getOutput() {
        return output;
    }

}
