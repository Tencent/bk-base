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

package com.tencent.bk.base.dataflow.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNodeUtils {

    @Test
    public void testGetNodeType() {
        SourceNode sourceNode = Mockito.mock(SourceNode.class);
        TransformNode transformNode = Mockito.mock(TransformNode.class);
        SinkNode sinkNode = Mockito.mock(SinkNode.class);

        assertEquals("source", NodeUtils.getNodeType(sourceNode));
        assertEquals("transform", NodeUtils.getNodeType(transformNode));
        assertEquals("sink", NodeUtils.getNodeType(sinkNode));
    }

    @Test
    public void testParseOrigin() {
        List<String> strings = NodeUtils.parseOrigin("stream:abc");
        assertEquals("[stream, abc]", strings.toString());
    }

    @Test
    public void testGenerateKafkaTopic() {
        String testNode = NodeUtils.generateKafkaTopic("test_node");
        assertEquals("table_test_node", testNode);
    }

    @Test
    public void testHaveOffsetField() {
        // test have offset field
        List<NodeField> fields = new ArrayList<>();
        NodeField field = new NodeField();
        NodeField spyField = spy(field);
        spyField.setField(ConstantVar.BKDATA_PARTITION_OFFSET);
        fields.add(spyField);
        assertTrue(NodeUtils.haveOffsetField(fields));

        // test have not offset field
        fields.clear();
        NodeField spyField1 = spy(field);
        spyField1.setField("abc_field");
        fields.add(spyField1);
        assertFalse(NodeUtils.haveOffsetField(fields));
    }

    @Test
    public void testCountFreq() {
        TransformNode node = new TransformNode();
        TransformNode spyNode = spy(node);
        Map<String, Object> info = ImmutableMap
                .of("window", ImmutableMap.of("count_freq", 30, "waiting_time", 60),
                        "id", "test_node_id",
                        "name", "test_node_name");
        spyNode.map(info, Component.flink);
        int waitingTime = NodeUtils.getCountFreq(spyNode);
        assertEquals(30, waitingTime);

        // test window config is null
        spyNode = spy(node);
        Map<String, Object> info1 = ImmutableMap
                .of("id", "test_node_id", "name", "test_node_name");
        spyNode.map(info1, Component.flink);
        assertEquals(0, NodeUtils.getCountFreq(spyNode));
    }

    @Test
    public void testGetWaitingTime() {
        TransformNode node = new TransformNode();
        TransformNode spyNode = spy(node);
        Map<String, Object> info = ImmutableMap
                .of("window", ImmutableMap.of("count_freq", 30, "waiting_time", 60),
                        "id", "test_node_id",
                        "name", "test_node_name");
        spyNode.map(info, Component.flink);
        int waitingTime = NodeUtils.getWaitingTime(spyNode);
        assertEquals(60, waitingTime);

        // test window config is null
        spyNode = spy(node);
        Map<String, Object> info1 = ImmutableMap
                .of("id", "test_node_id", "name", "test_node_name");
        spyNode.map(info1, Component.flink);
        assertEquals(0, NodeUtils.getWaitingTime(spyNode));
    }

}
