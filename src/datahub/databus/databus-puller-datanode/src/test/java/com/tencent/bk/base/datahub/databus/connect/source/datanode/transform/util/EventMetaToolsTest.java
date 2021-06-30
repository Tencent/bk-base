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

package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.util;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventMetaToolsTest {

    /**
     * 测试传参为String数组的getColumnValue方法
     */
    @Test
    public void testGetColumnValueArray() {
        EventMetaTools tools = new EventMetaTools("a0|a1|a2|a3");

        String[] vals1 = {"val0", "val1", "val2", "val3"};
        Assert.assertEquals("val3", tools.getColumnValue(vals1, "a3"));
        Assert.assertNull(tools.getColumnValue(vals1, null));
        String[] vals2 = null;
        Assert.assertNull(tools.getColumnValue(vals2, "a3"));
        Assert.assertNull(tools.getColumnValue(vals1, "a4"));
    }

    /**
     * 测试传参为List的getColumnValue方法
     */
    @Test
    public void testGetColumnValueList() {
        EventMetaTools tools = new EventMetaTools("a0|a1");
        List<Object> vals1 = new ArrayList<>();
        vals1.add("val0");
        vals1.add("val1");
        Assert.assertEquals("val1", tools.getColumnValue(vals1, "a1"));
        Assert.assertNull(tools.getColumnValue(vals1, null));
        List<Object> vals2 = null;
        Assert.assertNull(tools.getColumnValue(vals2, "a1"));
        Assert.assertNull(tools.getColumnValue(vals1, "a2"));
    }

    /**
     * 覆盖getColumnIndex中colName为null的情况
     */
    @Test
    public void testGetColumnIndex() {
        EventMetaTools tools = new EventMetaTools("a0|a1");
        Assert.assertEquals(-1, tools.getColumnIdx(null));
    }

    /**
     * 测试传参String型的构造函数传参为null的情况
     *
     * @throws Exception
     */
    @Test
    public void testConstructorStringIsNull() throws Exception {
        String columnParam = null;
        EventMetaTools tools = new EventMetaTools(columnParam);
        Field field = tools.getClass().getDeclaredField("columnMap");
        field.setAccessible(true);
        Map<String, Integer> columnMap = (HashMap) field.get(tools);
        Assert.assertEquals(0, columnMap.size());
    }

    /**
     * 测试传参StringArray型的构造函数传参为null的情况
     *
     * @throws Exception
     */
    @Test
    public void testConstructorStringArrayIsNull() throws Exception {
        String[] columnParam = null;
        EventMetaTools tools = new EventMetaTools(columnParam);
        Field field = tools.getClass().getDeclaredField("columnMap");
        field.setAccessible(true);
        Map<String, Integer> columnMap = (HashMap) field.get(tools);
        Assert.assertEquals(0, columnMap.size());
    }
} 
