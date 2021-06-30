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

package com.tencent.bk.base.dataflow.spark.topology.nodes;

import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.spark.sql.BatchJavaEnumerations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractBatchNodeBuilder extends AbstractBuilder {
    protected Map<String, Object> info;

    public AbstractBatchNodeBuilder(Map<String, Object> info) {
        super(info);
        this.info = info;
        this.initBuilder(info);
    }

    protected void initBuilder(Map<String, Object> info) {
    }

    protected Object retrieveRequiredString(Map<String, Object> info, String key) {
        if (info.containsKey(key)) {
            return info.get(key);
        } else {
            throw new IllegalArgumentException(String.format("Can't find valid value for %s", key));
        }
    }


    protected List<NodeField> parseV2Field(List<Map<String, String>> fields) {
        List<NodeField> nodeFields = new ArrayList<>();
        for (Map<String, String> field : fields) {
            if (!field.get("field").equals("timestamp") || !field.get("field").equals("offset")) {
                NodeField nodeField = new NodeField();
                nodeField.setField(field.get("field"));
                nodeField.setType(field.get("type"));
                nodeField.setOrigin(field.get("origin"));
                nodeField.setDescription(field.get("description"));
                nodeFields.add(nodeField);
            }
        }
        return nodeFields;
    }

    protected String getV2PhysicalTableName(Map<String, Object> info) {
        Map<String, Object> storageMap = (Map<String, Object>)info.get("storage_conf");
        return storageMap.get("physical_table_name").toString();
    }

    protected Map<String, Object> getV2IcebergConf(Map<String, Object> info) {
        Map<String, Object> storageMap = (Map<String, Object>)info.get("storage_conf");
        Map<String, Object> storekitHdfsConf = (Map<String, Object>)storageMap.get("storekit_hdfs_conf");
        Map<String, Object> tmpMap = new HashMap<>();
        tmpMap.put("hive.metastore.uris", storekitHdfsConf.get("hive.metastore.uris").toString());
        return tmpMap;
    }

    protected boolean isFieldContainStartEndTime() {
        boolean isStartTime = false;
        boolean isEndTime = false;
        for (NodeField field : this.fields) {
            if (field.getField().toLowerCase().equals(BatchJavaEnumerations.StartEndTimeField._starttime_.name())) {
                isStartTime = true;
            }
            if (field.getField().toLowerCase().equals(BatchJavaEnumerations.StartEndTimeField._endtime_.name())) {
                isEndTime = true;
            }
        }

        if (isEndTime ^ isStartTime) {
            throw new UnsupportedOperationException("starttime endtime must exist at the same time");
        }

        return isEndTime && isStartTime;
    }
}
