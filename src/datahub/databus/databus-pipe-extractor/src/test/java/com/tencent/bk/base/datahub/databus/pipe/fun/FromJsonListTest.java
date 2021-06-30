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
import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.List;

/**
 * FromJsonList Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/28/2018</pre>
 */
public class FromJsonListTest {

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
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-success.json");
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
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-success.json");
        ETL etl = new ETLImpl(confStr);
        String data = "[{\"k1\":123, \"k2\":\"test_def\"},{\"k1\":234, \"k2\":\"test2_def\"}]";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname1 = ret.getValByName(ret.getValues().get(0).get(0), "var1");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "var2");
        Assert.assertEquals("123", varname1);
        Assert.assertEquals("test_def", varname2);

        Object varname3 = ret.getValByName(ret.getValues().get(0).get(1), "var1");
        Object varname4 = ret.getValByName(ret.getValues().get(0).get(1), "var2");
        Assert.assertEquals("234", varname3);
        Assert.assertEquals("test2_def", varname4);


    }

    /**
     * 测试没有next节点的分支
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-failed.json");
        ETL etl = new ETLImpl(confStr);
        String data = "[{\"k1\":123, \"k2\":\"test_def\"},{\"k1\":234, \"k2\":\"test2_def\"}]";
        ETLResult ret = etl.handle(data.getBytes());

        Object varname1 = ret.getValByName(ret.getValues().get(0).get(0), "var1");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "var2");
        Assert.assertEquals("123", varname1);
        Assert.assertEquals("test_def", varname2);

        Object varname3 = ret.getValByName(ret.getValues().get(0).get(1), "var1");
        Object varname4 = ret.getValByName(ret.getValues().get(0).get(1), "var2");
        Assert.assertEquals("234", varname3);
        Assert.assertEquals("test2_def", varname4);
    }

    /**
     * 测试输入不为string的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputInvalid() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList("2016-03-24 04:13:30", "xxx_openid", "xxx_worldid"));
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

    /**
     * 测试输入不为json的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputNotJson() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-success.json");
        ETL etl = new ETLImpl(confStr);
        String data = "test not json";
        ETLResult ret = etl.handle(data.getBytes());
        Assert.assertEquals(Lists.newArrayList(), ret.getValues().get(0));
    }

    /**
     * 测试输入为null的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteInputNull() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/FromJsonList/from_json_list-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, null);
        List<Object> values = ctx.getValues();
        Assert.assertEquals(Lists.newArrayList(), values);
    }

}
