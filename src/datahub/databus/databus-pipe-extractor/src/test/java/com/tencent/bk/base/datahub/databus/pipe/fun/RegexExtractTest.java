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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

/**
 * RegexExtract Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
@RunWith(Parameterized.class)
public class RegexExtractTest {

    public RegexExtractTest(Object input, Object output) {
    }

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"param", Lists.newArrayList("a123b", "data")},
        };
        return Arrays.asList(data);
    }

    /**
     * 测试validateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/RegexExtract/regex_extract-success.json");
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
        String confStr = TestUtils.getFileContent("/fun/RegexExtract/regex_extract-success.json");
        ETL etl = new ETLImpl(confStr);
        String data = "a123b";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname = ret.getValByName(ret.getValues().get(0).get(0), "data");
        Assert.assertEquals("123", varname);
    }

    /**
     * 测试匹配不到的分支
     *
     * @throws Exception
     */
    @Test
    public void testExecuteCase0() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/RegexExtract/regex_extract-case-0.json");
        ETL etl = new ETLImpl(confStr);
        String data = "a123b";
        ETLResult ret = etl.handle(data.getBytes());
        Object varName = ret.getValByName(ret.getValues().get(0).get(0), "data");
        Assert.assertNull(varName);
    }

    /**
     * 测试解析正则失败的分支
     *
     * @throws Exception
     */
    @Test(expected = java.util.regex.PatternSyntaxException.class)
    public void testExecuteNoNext() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/RegexExtract/regex_extract-failed.json");
        ETL etl = new ETLImpl(confStr);
        String data = "a123b";
        etl.handle(data.getBytes());
    }

}
