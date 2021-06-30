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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.assign.AssignValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NodeFactoryTest {

    /**
     * 测试genNode
     */
    @Test
    public void testGenNode() {
        NodeFactory nodeFactory = new NodeFactory();
        Map<String, Object> map = new HashMap<>();
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        Assert.assertNull(NodeFactory.genNode(new Context(), null));
        map.put("type", "xxx");
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        map.put("type", null);
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        map.put("type", "\0fun");
        map.put("method", "xxx");
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        map.put("type", "\0access");
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        map.put("type", "\0assign");
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
        map.put("type", "\0branch");
        Assert.assertNull(NodeFactory.genNode(new Context(), map));
    }

    /**
     * 测试generateNodeLabel
     */
    @Test(expected = NullPointerException.class)
    public void testGenerateNodeLabel1() {
        new AssignValue(new Context(), null);
    }

    /**
     * 测试generateNodeLabel
     */
    @Test(expected = NullPointerException.class)
    public void testGenerateNodeLabel2() {
        Map<String, Object> map = new HashMap<>();
        new AssignValue(new Context(), map);
    }

    /**
     * 测试generateNodeLabel
     */
    @Test(expected = NullPointerException.class)
    public void testGenerateNodeLabel3() {
        Map<String, Object> map = new HashMap<>();
        map.put("label", null);
        new AssignValue(new Context(), map);
    }

    /**
     * 测试verifyEtlConf
     */
    @Test
    public void testVerifyEtlConf() {
        new VerifyEtlConf();
        new Config();
    }

    /**
     * 测试Branch
     */
    @Test
    public void testBranch() {
        Map<String, Object> conf = new HashMap<>();
        List<Map<String, Object>> list = new LinkedList<>();
        conf.put("next", list);
        Node node = new Branch(new Context(), conf);
        Assert.assertFalse(node.validateNext());
    }
}
