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

package com.tencent.bk.base.dataflow.ml.spark.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode.ModelTransformNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformerBuilderFactory;
import com.tencent.bk.base.dataflow.ml.spark.utils.FileUtil;
import com.tencent.bk.base.dataflow.spark.topology.nodes.transform.BatchSQLTransformNode.BatchSQLTransformNodeBuilder;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ModelTransformerBuilderFactoryTest {

    private ModelTransformerBuilderFactory factory;
    private Map<String, Object> transformNode;

    private void init(String file) {
        try {
            String content = FileUtil.read(file);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parameters = objectMapper.readValue(content, HashMap.class);
            Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) parameters.get("nodes");
            Map<String, Object> sourceNodes = nodes.get("source");
            Map<String, Object> transformNodes = nodes.get("transform");
            transformNode = (Map<String, Object>) transformNodes.get("591_string_indexer_result_204");
            long scheduleTime = (long) parameters.get("schedule_time");
            this.factory = new ModelTransformerBuilderFactory("spark_mllib", scheduleTime, sourceNodes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetBuilder() {
        this.init("/common_node_topology/test_periodic_topology.conf");
        transformNode.put("task_type", "mlsql_query");
        AbstractBuilder builder = factory.getBuilder(transformNode);
        assert builder instanceof ModelTransformNodeBuilder;

        transformNode.put("task_type", "sub_query");
        builder = factory.getBuilder(transformNode);
        assert builder instanceof BatchSQLTransformNodeBuilder;

    }
}
