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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

public class TestIgniteStaticJoin {

    @Test
    public void initStreamKeyIndexs() {
        TransformNode spyNode = spyNode();
        // test case1: key order
        Mockito.when(spyNode.getProcessorArgs()).thenReturn(
                "{\"storage_info\": {\"query_mode\": \"single\", \"ignite_cluster\": \"ignite-join-test\", "
                        + "\"redis_type\": \"private\", \"data_id\": \"591_test_ignite_write_node\", "
                        + "\"ignite_user\": \"bkdata_admin\", \"key_separator\": \"\", "
                        + "\"key_order\": [\"ip\", \"report_time\"], \"host\": \"127.0.0.1\", "
                        + "\"kv_source_type\": \"ignite\", \"password\": \"\", "
                        + "\"port\": 10800, \"cache_name\": \"test_ignite_write_node_591\"}, "
                        + "\"async_io_concurrency\": 3, \"table_name\": \"test_ignite_write_node\", "
                        + "\"biz_id\": \"591\", \"type\": \"left\", \"join_keys\": [{\"static\": \"ip\", "
                        + "\"stream\": \"ip\"}, {\"static\": \"report_time\", \"stream\": \"report_time\"}]}");
        IgniteStaticJoin igniteStaticJoin1 = new IgniteStaticJoin(spyNode, null, null);
        assertEquals("[1, 2]", igniteStaticJoin1.getStreamKeyIndexs().toString());

        // test case2: key order reverse
        Mockito.when(spyNode.getProcessorArgs()).thenReturn(
                "{\"storage_info\": {\"query_mode\": \"single\", \"ignite_cluster\": \"ignite-join-test\", "
                        + "\"redis_type\": \"private\", \"data_id\": \"591_test_ignite_write_node\", "
                        + "\"ignite_user\": \"bkdata_admin\", \"key_separator\": \"\", "
                        + "\"key_order\": [\"report_time\", \"ip\"], \"host\": \"127.0.0.1\", "
                        + "\"kv_source_type\": \"ignite\", \"password\": \"\", "
                        + "\"port\": 10800, \"cache_name\": \"test_ignite_write_node_591\"}, "
                        + "\"async_io_concurrency\": 3, \"table_name\": \"test_ignite_write_node\", "
                        + "\"biz_id\": \"591\", \"type\": \"left\", \"join_keys\": [{\"static\": \"ip\", "
                        + "\"stream\": \"ip\"}, {\"static\": \"report_time\", \"stream\": \"report_time\"}]}");
        IgniteStaticJoin igniteStaticJoin2 = new IgniteStaticJoin(spyNode, null, null);
        assertEquals("[2, 1]", igniteStaticJoin2.getStreamKeyIndexs().toString());
    }

    /**
     * spy transformNode for static join test
     *
     * @return spy transformNode
     */
    public static TransformNode spyNode() {
        //prepare parent node
        NodeField nodeField1 = new NodeField();
        NodeField nodeField2 = new NodeField();
        NodeField nodeField3 = new NodeField();
        nodeField1.setField("time");
        nodeField2.setField("ip");
        nodeField3.setField("report_time");
        NodeField[] nodeFieldArray = {nodeField1, nodeField2, nodeField3};
        TransformNode parentNode = new TransformNode();
        parentNode.setFields(Arrays.asList(nodeFieldArray));
        List<Node> parents = Lists.newArrayList();
        parents.add(parentNode);
        TransformNode node = new TransformNode();
        node.setParents(parents);
        //prepare current node
        NodeField curNodeField1 = new NodeField();
        curNodeField1.setOrigin("test_upstream:time");
        curNodeField1.setField("time");
        curNodeField1.setType("long");

        NodeField curNodeField2 = new NodeField();
        curNodeField2.setOrigin("test_upstream:ip");
        curNodeField2.setField("ip");
        curNodeField2.setType("string");

        NodeField curNodeField3 = new NodeField();
        curNodeField3.setOrigin("test_upstream:report_time");
        curNodeField3.setField("report_time");
        curNodeField3.setType("string");

        NodeField curNodeField4 = new NodeField();
        curNodeField4.setOrigin("static_join_transform:province");
        curNodeField4.setField("province");
        curNodeField4.setType("string");

        NodeField curNodeField5 = new NodeField();
        curNodeField5.setOrigin("static_join_transform:city");
        curNodeField5.setField("city");
        curNodeField5.setType("string");

        NodeField[] curNodeFieldArray = {curNodeField1, curNodeField2, curNodeField3, curNodeField4, curNodeField5};
        node.setFields(Arrays.asList(curNodeFieldArray));
        //spy node for test
        TransformNode spyNode = Mockito.mock(TransformNode.class, Mockito.withSettings()
                .spiedInstance(node)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .serializable());
        return spyNode;
    }
}
