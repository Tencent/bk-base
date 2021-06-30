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

package com.tencent.bk.base.datahub.databus.pipe.dispatch;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExprTest {

    /**
     * 测试类型为Filed的equals方法
     */
    @Test
    public void testEqualsTypeIsField() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Field val = new Field("varname", "string");
        Expr expr = new Expr(etl, "type", val, 0);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("hello world");
        Assert.assertTrue(expr.equals(oneValue, expr));
    }

    /**
     * 测试类型为数值的equals方法
     */
    @Test
    public void testEqualsTypeIsInt() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Field val = new Field("varname", "int");
        Expr expr = new Expr(etl, "type", val, 0);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add(null);
        Assert.assertFalse(expr.equals(oneValue, expr));
    }

    /**
     * 测试类型不为数值的equals方法异常情况
     */
    @Test(expected = RuntimeException.class)
    public void testEqualsTypeNotNumError() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "xxx", 11);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add(11);
        expr.equals(oneValue, 11);
    }

    /**
     * 测试类型为数值的equals方法
     */
    @Test
    public void testEqualsTypeIsDouble() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "double", 1.11);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add(11);
        Assert.assertTrue(expr.equals(oneValue, 1.11));
    }

    /**
     * 测试类型为数值的equals方法
     */
    @Test
    public void testEqualsTypeIsInt1() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "int", 1);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add(11);
        Assert.assertTrue(expr.equals(oneValue, 1));
    }

    /**
     * 测试类型为数值的equals方法
     */
    @Test
    public void testEqualsTypeIsLong() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "long", 1);
        List<Object> oneValue = new ArrayList<>();
        oneValue.add(11);
        Assert.assertTrue(expr.equals(oneValue, 1));
    }

    /**
     * 测试类型为String的equals方法
     */
    @Test(expected = RuntimeException.class)
    public void testEqualsTypeIsString1() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "int", "value");
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("value");
        expr.equals(oneValue, "value");
    }

    /**
     * 测试类型为String的equals方法
     */
    @Test
    public void testEqualsTypeIsString2() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "string", "value");
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("value");
        Assert.assertTrue(expr.equals(oneValue, "value"));
    }

    /**
     * 测试execute方法
     */
    @Test
    public void testExecute() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Expr expr = new Expr(etl, "string", "value");
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("value");
        Assert.assertFalse(expr.execute(etl, oneValue));
        Assert.assertEquals(expr.getVal(), "value");
        Assert.assertFalse(expr.notEquals(oneValue, expr));
    }
}
