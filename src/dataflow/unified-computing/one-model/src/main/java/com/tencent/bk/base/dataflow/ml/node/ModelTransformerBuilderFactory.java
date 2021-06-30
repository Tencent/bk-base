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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataNodeType;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.HdfsTableFormat;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TableType;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TransformType;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctionConfig;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions.Builder;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode.ModelTransformNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelTransformerBuilderFactory extends AbstractModelBuilderFactory {

    private List<String> udfNameList = new ArrayList<>();
    private Builder builder = UserDefinedFunctions.builder();
    private Map<String, Object> sourceNodes;

    public ModelTransformerBuilderFactory(String jobType, long scheduleTime, Map<String, Object> sourceNodes) {
        super(jobType, scheduleTime);
        this.sourceNodes = sourceNodes;
    }

    @Override
    public AbstractBuilder getBuilder(Map<String, Object> nodeInfo) {
        TransformType transformType = TransformType.mlsql_query;
        if (nodeInfo.containsKey("task_type")) {
            transformType = TransformType.valueOf(nodeInfo.get("task_type").toString());
        }
        switch (transformType) {
            case mlsql_query:
                return new ModelTransformNodeBuilder(nodeInfo);
            case sub_query:
                Map<String, Object> transformInfo = new HashMap<>();
                Map<String, Object> processor = (Map<String, Object>) nodeInfo.get("processor");
                Map<String, Object> processorArgs = (Map<String, Object>) processor.get("args");
                transformInfo.put("sql", processorArgs.get("sql").toString());
                transformInfo.put("id", nodeInfo.get("id").toString());
                transformInfo.put("name", nodeInfo.get("name").toString());
                transformInfo.put("type", nodeInfo.get("type").toString());
                if (nodeInfo.containsKey("udfs")) {
                    List<Map<String, String>> udfs = (List<Map<String, String>>) nodeInfo.get("udfs");
                    udfs.forEach(item -> {
                        if (!udfNameList.contains(item.get("name"))) {
                            builder.addFunction(
                                    new UserDefinedFunctionConfig(item.get("language"), item.get("name"),
                                            item.get("type"), item.get("hdfs_path"), item.get("local_path")));
                            udfNameList.add(item.get("name"));
                        }
                    });
                }
                return new BatchSQLTransformNode.BatchSQLTransformNodeBuilder(transformInfo);
            default:
                throw new RuntimeException("Unsupported transform type:" + transformType);
        }
    }

    @Override
    public Map<String, Object> getConfigMap(TableType tableType, HdfsTableFormat hdfsTableFormat,
            DataNodeType sourceNodeType) {
        return this.sourceNodes;
    }

    @Override
    public UserDefinedFunctions getUserDefinedFunctions() {
        return this.builder.create();
    }
}
