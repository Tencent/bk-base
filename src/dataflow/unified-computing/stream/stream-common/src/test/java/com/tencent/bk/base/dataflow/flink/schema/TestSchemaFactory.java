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

package com.tencent.bk.base.dataflow.flink.schema;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSchemaFactory {

    @Test
    public void testGetFieldsTypes() {

        List<NodeField> fields = new ArrayList<>();
        NodeField nodeField = new NodeField();
        NodeField spyIntField = spy(nodeField);
        spyIntField.setField("int_field");
        spyIntField.setType("int");
        fields.add(spyIntField);

        NodeField spyDoubleField = spy(nodeField);
        spyDoubleField.setField("double_field");
        spyDoubleField.setType("double");
        fields.add(spyDoubleField);

        NodeField spyFloatField = spy(nodeField);
        spyFloatField.setField("float_field");
        spyFloatField.setType("float");
        fields.add(spyFloatField);

        NodeField spyLongField = spy(nodeField);
        spyLongField.setField("long_field");
        spyLongField.setType("long");
        fields.add(spyLongField);

        NodeField spyStringField = spy(nodeField);
        spyStringField.setField("string_field");
        spyStringField.setType("string");
        fields.add(spyStringField);

        Node node = Mockito.mock(Node.class);
        Mockito.when(node.getFields()).thenReturn(fields);
        TypeInformation<?>[] fieldsTypes = new SchemaFactory().getFieldsTypes(node);
        assertEquals("Integer", fieldsTypes[0].toString());
        assertEquals("Double", fieldsTypes[1].toString());
        assertEquals("Float", fieldsTypes[2].toString());
        assertEquals("Long", fieldsTypes[3].toString());
        assertEquals("String", fieldsTypes[4].toString());
    }
}
