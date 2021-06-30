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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Zip Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
public class ZipTest {

    /**
     * 测试ValidateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertFalse(parser.validateNext());
    }

    /**
     * 测试正常分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        List<String> list1 = Lists.newArrayList("k1", "k2", "k3", "k11");
        List<String> list2 = Lists.newArrayList("k4", "k5", "k6");
        Map listMap = Maps.newHashMap();
        listMap.put("list1", list1);
        listMap.put("list2", list2);
        parser.execute(ctx, listMap);
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "k1");
        Assert.assertEquals(values.get(0).get(1), "k4");

    }

    /**
     * 测试没有next节点的分支
     *
     * @throws Exception
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        List<String> list1 = Lists.newArrayList("k1", "k2", "k3");
        List<String> list2 = Lists.newArrayList("k4", "k5", "k6");
        Map listMap = Maps.newHashMap();
        listMap.put("list1", list1);
        listMap.put("list2", list2);
        Object result =  parser.execute(ctx, listMap);
        Assert.assertNull(result);
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "k1");
        Assert.assertEquals(values.get(0).get(1), "k4");
    }

    /**
     * 测试输入不为map的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputInvalid() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("aa", "bb"));
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试输入为null的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputNull() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, null);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试正常分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteCase0() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        List<String> list1 = Lists.newArrayList("k1", "k2", "k3");
        String list2 = new String("test");
        Map listMap = Maps.newHashMap();
        listMap.put("list1", list1);
        listMap.put("list2", list2);
        parser.execute(ctx, listMap);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);

    }

    /**
     * 测试正常分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteCase1() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/Zip/zip-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        List<String> list2 = Lists.newArrayList("k1", "k2", "k3");
        String list1 = new String("test");
        Map listMap = Maps.newHashMap();
        listMap.put("list1", list1);
        listMap.put("list2", list2);
        parser.execute(ctx, listMap);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);

    }

} 
