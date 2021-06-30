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

package com.tencent.bk.base.datahub.databus.pipe.record;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

/**
 * Field Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/04/2018</pre>
 */
@RunWith(Parameterized.class)
public class FieldTest {

    private String input;
    private Object output;

    public FieldTest(String input, Object output) {
        this.input = input;
        this.output = output;
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
        Field field = new Field("timestamp", "int");
        String msg = field.toString();
        Assert.assertEquals(
                "(name:timestamp,type:int,is_dimension:false,is_json:false,is_internal:false,is_output:true)",
                msg);
    }

    /**
     * Method: isDimension()
     */
    @Test
    public void testIsDimension() throws Exception {
        Field field = new Field("timestamp", "int", false);
        Assert.assertFalse(field.isDimension());

    }

    /**
     * Method: setIsOutputField(Boolean isOutputField)
     */
    @Test
    public void testSetIsOutputField() throws Exception {
        Field field = new Field("timestamp", "int", false);
        field.setIsOutputField(true);
        Assert.assertTrue(field.isOutputField());
    }


    /**
     * Method: parseNumber(Object data)
     */
    @Test
    public void testParseNumberCase0() throws Exception {
        Field field = new Field("timestamp", "Integer", false);
        Object result = ReflectionUtils
                .invokeMethod(field.getClass().getDeclaredMethod("parseNumber", Object.class), field, 100);
        Assert.assertNull(result);
    }

    /**
     * Method: parseNumber(Object data)
     */
    @Test
    public void testParseNumberCase1() throws Exception {
        Field field = new Field("timestamp", "Integer", false);
        Object result = ReflectionUtils
                .invokeMethod(field.getClass().getDeclaredMethod("parseNumber", Object.class), field, "test");
        Assert.assertNull(result);
    }

    /**
     * Method: parseNumber(Object data)
     */
    @Test
    public void testIsNullString() throws Exception {
        Field field = new Field("timestamp", "Integer", false);
        Boolean result = (Boolean) ReflectionUtils
                .invokeMethod(field.getClass().getDeclaredMethod("isNullString", String.class), field, "null");
        Assert.assertTrue(result);
    }

    @Test
    public void testParseNumberSwitch0() throws Exception {
        Field field = new Field("timestamp", input, false);
        Object result = ReflectionUtils
                .invokeMethod(field.getClass().getDeclaredMethod("parseNumber", Object.class), field, "test");
        Assert.assertNull(result);
    }

    @Test
    public void testParseNumberSwitch1() throws Exception {
        Field field = new Field("timestamp", input, false);
        Object result = ReflectionUtils
                .invokeMethod(field.getClass().getDeclaredMethod("parseNumber", Object.class), field, 111);
        Assert.assertNull(result);
    }

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{{"\0int", null}, {"\0long", null},
                {"\0double", null}, {"\0bigint", null}, {"\0bigdecimal", null}};
        return Arrays.asList(data);
    }


} 
