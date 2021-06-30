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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2;

import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PeriodicSQLv2IgniteSourceNodeBuilder extends BatchIgniteSourceNode.BatchIgniteSourceNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLv2IgniteSourceNodeBuilder.class);

    public PeriodicSQLv2IgniteSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.fields = this.parseV2Field((List<Map<String, String>>)info.get("fields"));

        String fullPhysicalName = this.getV2PhysicalTableName(info);
        if (fullPhysicalName.contains(".")) {
            this.physicalName = fullPhysicalName.substring(fullPhysicalName.lastIndexOf(".") + 1);
        } else {
            this.physicalName = fullPhysicalName;
        }
        this.connectionInfo = this.getIgniteConnectionInfo(info);
    }

    private Map<String, Object> getIgniteConnectionInfo(Map<String, Object> info) {
        Map<String, Object> storageMap = (Map<String, Object>)info.get("storage_conf");
        Map<String, Object> connectionMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : storageMap.entrySet()) {
            connectionMap.put(entry.getKey(), this.formatString(entry.getValue()));
        }
        return connectionMap;
    }

    private String formatString(Object value) {
        if (value instanceof Double) {
            return value.toString().substring(0, value.toString().indexOf("."));
        } else {
            return value.toString();
        }
    }

    @Override
    public BatchIgniteSourceNode build() {
        this.buildParams();
        return new BatchIgniteSourceNode(this);
    }
}
