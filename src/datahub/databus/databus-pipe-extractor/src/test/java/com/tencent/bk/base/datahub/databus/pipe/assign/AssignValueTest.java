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

package com.tencent.bk.base.datahub.databus.pipe.assign;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

/**
 * AssignValue Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
public class AssignValueTest {

    /**
     * 测试ValidateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignValue/assign_value-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertTrue(parser.validateNext());
    }

    /**
     * 测试正常情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignValue/assign_value-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("openid", "worldid", "timestamp"));
        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "[openid, worldid, timestamp]");
    }

    /**
     * 测试失败情况
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExecuteFailed() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignValue/assign_value-success.json");
        Context ctx = new Context();
        AssignValue assignValue = (AssignValue) Config.parse(ctx, confStr);

        // 通过反射修改Field中的isJson为true
        Field f = assignValue.getClass().getDeclaredField("field");
        f.setAccessible(true);
        Object field_val = f.get(assignValue);

        Field isJson = f.getType().getDeclaredField("isJson");
        isJson.setAccessible(true);
        isJson.set(field_val, Boolean.TRUE);

        f.set(assignValue, field_val);

        assignValue.execute(ctx, Lists.newArrayList("openid", "worldid", "timestamp"));
        Assert.assertNotEquals(ctx.badValues.size(), 0);
    }

    /**
     * 测试Conf中type为int的情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteTypeIsInt() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignValue/assign_value.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("openid", "worldid", "timestamp"));
        Assert.assertNull(ctx.getValues().get(0));
    }

    /**
     * 测试Conf中Subtype为null的情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSubtypeIsNull() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignValue/assign_subtype_null.json");
        Context ctx = new Context();
        Config.parse(ctx, confStr);
    }
} 
