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

package com.tencent.bk.base.datahub.databus.pipe.fun;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.exception.EmptyEtlResultError;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Items Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
public class ItemsTest {

    /**
     * 测试ValidateNext方法
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertFalse(parser.validateNext());
    }

    /**
     * 测试正常分支
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Map<String, String> map = Maps.newHashMap();
        map.put("k1", "k1_value");
        map.put("k2", "k2_value");
        map.put("k3", "k3_value");
        parser.execute(ctx, map);
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals("k1_value", values.get(0).get(0));
        Assert.assertEquals("k2_value", values.get(1).get(0));
        Assert.assertEquals("k3_value", values.get(2).get(0));
    }

    /**
     * 测试新流程正常分支
     */
    @Test
    public void testExecuteNewSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-new-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Map<String, String> map = Maps.newHashMap();
        map.put("k1", "k1_value");
        map.put("k2", "k2_value");
        map.put("k3", "k3_value");
        parser.execute(ctx, map);
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals("[[k1, k1_value], [k2, k2_value], [k3, k3_value]]", values.toString());
    }

    /**
     * 测试所有下游节点处理失败的分支
     */
    @Test(expected = EmptyEtlResultError.class)
    public void testExecuteCase0() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-case-0.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Map<String, String> map = Maps.newHashMap();
        map.put("k1", "k1_value");
        map.put("k2", "k2_value");
        map.put("k3", "k3_value");
        parser.execute(ctx, map);
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals("k1_value", values.get(0).get(0));
        Assert.assertEquals("k2_value", values.get(1).get(0));
        Assert.assertEquals("k3_value", values.get(2).get(0));
    }

    /**
     * 测试没有next节点的分支
     */
    @Test
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Map<String, String> map = Maps.newHashMap();
        map.put("k1", "k1_value");
        map.put("k2", "k2_value");
        map.put("k3", "k3_value");
        Object result = parser.execute(ctx, map);
        Assert.assertNull(result);
    }

    /**
     * 测试输入不为Map的分支
     */
    @Test
    public void testExecuteInputInvalid() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "teststr");
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试输入为null的分支
     */
    @Test
    public void testExecuteInputNull() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Items/items-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, null);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

} 
