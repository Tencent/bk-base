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
import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.exception.BadCsvDataError;
import org.junit.Assert;
import org.junit.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

/**
 * CsvLine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/28/2018</pre>
 */
@RunWith(PowerMockRunner.class)
public class CsvLineTest {

    /**
     * 测试validateNext
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertFalse(parser.validateNext());
    }

    /**
     * 测试正常分支
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "2016-03-24 04:13:30,xxx_openid,xxx_worldid");
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "2016-03-24 04:13:30");
        Assert.assertEquals(values.get(0).get(1), "xxx_openid");
        Assert.assertEquals(values.get(0).get(2), "xxx_worldid");
    }

    /**
     * 测试没有next节点的分支
     */
    @Test
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Object result = parser.execute(ctx, "2016-03-24 04:13:30|xxx_openid|xxx_worldid");
        Assert.assertNull(result);
    }

    /**
     * 测试输入不为string的分支
     */
    @Test
    public void testExecuteInputInvalid() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("2016-03-24 04:13:30", "xxx_openid", "xxx_worldid"));
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试输入为null的分支
     */
    @Test
    public void testExecuteInputNull() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, null);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    @Test(expected = BadCsvDataError.class)
    @PrepareForTest(CsvLine.class)
    public void testCsvLineCase0() throws Exception {
        String argument = "2016-03-24 04:13:30,xxx_openid,xxx_worldid";
        PowerMockito.whenNew(StringReader.class).withArguments(argument).thenReturn(null);
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        CsvLine csvLine = (CsvLine) Config.parse(ctx, confStr);
        Optional<Method> method = ReflectionUtils.findMethod(csvLine.getClass(), "csvLine", String.class);
        ReflectionUtils.invokeMethod(method.get(), csvLine, argument);

    }

    @Test
    public void testCsvLineCase1() throws Exception {
        String argument = "";
        String confStr = TestUtils.getFileContent("/fun/CsvLine/csvline-success.json");
        Context ctx = new Context();
        CsvLine csvLine = (CsvLine) Config.parse(ctx, confStr);
        Optional<Method> method = ReflectionUtils.findMethod(csvLine.getClass(), "csvLine", String.class);
        List<String> result = (List<String>) ReflectionUtils.invokeMethod(method.get(), csvLine, argument);
        Assert.assertEquals(Lists.newArrayList(), result);
    }
} 
