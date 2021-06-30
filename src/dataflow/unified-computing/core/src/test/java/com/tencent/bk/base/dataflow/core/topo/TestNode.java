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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class TestNode {

    private Node node;
    private Map<String, Object> buildInfo;

    /**
     * 测试 {@link Node} 的准备工作
     */
    @Before
    public void setUp() {
        this.buildInfo = ImmutableMap.<String, Object>builder()
                .put("id", "1_test_node_id")
                .put("name", "test_node_id")
                .put("type", "data")
                .put("description", "test node")
                .put("fields", ImmutableList.<Map<String, Object>>builder()
                        .add(ImmutableMap.of("field", "dtEventTime",
                                "type", "string",
                                "origin", "",
                                "event_time", false,
                                "description", ""))
                        .add(ImmutableMap.of("field", "double_field",
                                "type", "double",
                                "origin", "",
                                "event_time", false,
                                "description", ""))
                        .add(ImmutableMap.of("field", "int_field",
                                "type", "int", "origin", "",
                                "event_time", false,
                                "description", ""))
                        .add(ImmutableMap.of("field", "string_field",
                                "type", "string",
                                "origin", "",
                                "event_time", false,
                                "description", ""))
                        .add(ImmutableMap.of("field", "long_field",
                                "type", "long",
                                "origin", "",
                                "event_time", true,
                                "description", ""))
                        .add(ImmutableMap.of("field", "float_field",
                                "type", "float",
                                "origin", "",
                                "event_time", false,
                                "description", ""))
                        .build())
                .build();
        AbstractBuilder builder = new AbstractBuilder(buildInfo) {
            @Override
            public Node build() {
                return null;
            }
        };

        this.node = new Node(builder);
    }

    @Test
    public void testGetFieldNames() {
        String[] fieldNames = this.node.getFieldNames();
        assertEquals("[dtEventTime, double_field, int_field, string_field, long_field, float_field]",
                Arrays.toString(fieldNames));
    }

    @Test
    public void testGetCommonFields() {
        String commonFields = this.node.getCommonFields();
        assertEquals("dtEventTime, double_field, int_field, string_field, long_field, float_field",
                commonFields);
    }

    @Test
    public void testGetFieldIndex() {
        assertEquals(3, this.node.getFieldIndex("string_field"));
    }

    @Test
    public void testMap() {
        this.node.map(this.buildInfo, Component.flink);
        assertEquals("data", this.node.getType());
        assertEquals(6, this.node.getFieldsSize());
    }
}
