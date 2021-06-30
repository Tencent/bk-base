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

import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ContextTest {

    /**
     * 测试flattenValues
     */
    @Test
    public void testFlattenValues1() throws Exception {
        Fields fields = new Fields();
        fields.append(new Fields());
        List<Object> values = new ArrayList<>();
        values.add(null);
        Context ctx = new Context();
        ctx.setContextToVerifyConf();
        Method method = ctx.getClass().getDeclaredMethod("addErrorMessage", String.class);
        method.setAccessible(true);
        method.invoke(ctx, "errMsg");
        Assert.assertEquals(ctx.flattenValues(fields, values).size(), 0);
    }

    /**
     * 测试flattenValues
     */
    @Test
    public void testFlattenValues2() throws Exception {
        Fields fields = new Fields();
        fields.append(new Fields());
        List<Object> values = new ArrayList<>();
        values.add(null);
        Context ctx = new Context();
        ctx.appendBadValues("data", new RuntimeException(), false);
        java.lang.reflect.Field field = ctx.getClass().getDeclaredField("etl");
        String confStr = TestUtils.getFileContent("/dispatch.json");
        ETL etl = new ETLImpl(confStr);
        field.set(ctx, etl);
        Assert.assertEquals(ctx.flattenValues(fields, values).size(), 0);
    }

    /**
     * 测试ToETLResult
     */
    @Test
    public void testToETLResult() throws Exception {
        String confStr = TestUtils.getFileContent("/dispatch.json");
        ETLImpl etl = new ETLImpl(confStr);
        java.lang.reflect.Field field = etl.getClass().getDeclaredField("timeFieldIdx");
        field.set(etl, 1);
        java.lang.reflect.Field field1 = etl.getClass().getDeclaredField("dispatch");
        field1.set(etl, null);

        Fields schema = new Fields();
        Fields fields = new Fields();
        fields.append(new Field("xx", "xx"));
        schema.append(fields);
        schema.append(new Field("xx", "xx"));

        List<Object> values = new ArrayList<>();
        List<List<Object>> data0 = new ArrayList<>();
        List<Object> list0 = new ArrayList<>();
        List<Object> list1 = new ArrayList<>();
        String value = "2016-03-24 04:13:30";
        String data1 = "2016-03-24 04:13";
        list0.add(value);
        data0.add(list0);
        list1.add(data1);
        data0.add(list1);

        values.add(data0);
        values.add(value);

        Context ctx = new Context(etl);
        java.lang.reflect.Field context1 = ctx.getClass().getDeclaredField("schema");
        context1.setAccessible(true);
        context1.set(ctx, schema);

        java.lang.reflect.Field context2 = ctx.getClass().getDeclaredField("values");
        context2.setAccessible(true);
        context2.set(ctx, values);

        ETLResult result = ctx.toETLResult(etl);
        Assert.assertEquals(result.getFailedMsgNum(), 1);
    }
}
