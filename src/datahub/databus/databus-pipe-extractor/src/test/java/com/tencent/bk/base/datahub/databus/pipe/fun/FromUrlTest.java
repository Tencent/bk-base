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
import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FromUrl Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/28/2018</pre>
 */
public class FromUrlTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: validateNext()
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
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
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
        ETL etl = new ETLImpl(confStr);
        String data = "key1=123&key2=test_def";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname = ret.getValByName(ret.getValues().get(0).get(0), "varname");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "varname2");
        Assert.assertEquals(123, varname);
        Assert.assertEquals("test_def", varname2);
    }

    /**
     * 测试编码为空
     *
     * @throws Exception
     */
    @Test
    public void testReadMapCase0() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-case-0.json");
        Context ctx = new Context();
        ctx.etl = new ETLImpl(confStr);
        ctx.etl.encoding = null;
        String data = "key1=123&key2=test_def";
        FromUrl parser = (FromUrl) Config.parse(ctx, confStr);
        Optional<Method> method = ReflectionUtils.findMethod(parser.getClass(), "readMap", Context.class, String.class);
        Map<String, String> result = (Map<String, String>) ReflectionUtils
                .invokeMethod(method.get(), parser, ctx, data);
        Assert.assertEquals("123", result.get("key1"));
        Assert.assertEquals("test_def", result.get("key2"));
    }

    /**
     * 测试错误编码
     *
     * @throws Exception
     */
    @Test
    public void testReadMapCase1() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-case-0.json");
        Context ctx = new Context();
        ctx.etl = new ETLImpl(confStr);
        ctx.etl.encoding = "";
        String data = "key1=123&key2=test_def";
        FromUrl parser = (FromUrl) Config.parse(ctx, confStr);
        Optional<Method> method = ReflectionUtils.findMethod(parser.getClass(), "readMap", Context.class, String.class);
        Map<String, String> result = (Map<String, String>) ReflectionUtils
                .invokeMethod(method.get(), parser, ctx, data);
        Assert.assertEquals(Maps.newHashMap(), result);
    }

    /**
     * 测试没有next节点的分支
     *
     * @throws Exception
     */
    @Test(expected = NullPointerException.class)
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-failed.json");
        ETL etl = new ETLImpl(confStr);
        String data = "key1=123&key2=test_def";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname = ret.getValByName(ret.getValues().get(0).get(0), "varname");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "varname2");
        Assert.assertEquals(123, varname);
        Assert.assertEquals("test_def", varname2);
    }

    /**
     * 测试输入不为string的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputInvalid() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("2016-03-24 04:13:30", "xxx_openid", "xxx_worldid"));
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试输入不为json的分支
     * @throws Exception
     *//*
  @Test
  public void testExecuteInputNotJson() throws Exception {
    String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
    ETL etl = new ETLImpl(confStr);
    String data = "test not json";
    ETLResult ret = etl.handle(data.getBytes());
    Assert.assertEquals(Lists.newArrayList(), ret.getValues().get(0));
  }*/

    /**
     * 测试输入为null的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputNull() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, null);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试错误编码
     *
     * @throws Exception
     */
    @Test
    public void testIllegalArgumentException() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromUrl/from_url-success.json");
        ETL etl = new ETLImpl(confStr);
        String data = "key1=111&key2%=222";
        ETLResult ret = etl.handle(data.getBytes());
        Assert.assertEquals(1, ret.getFailedValues().size());
    }
}
