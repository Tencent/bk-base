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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Fields Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/04/2018</pre>
 */
public class FieldsTest {

    /**
     * Method: set(int index, Field field)
     */
    @Test
    public void testSet() throws Exception {
        Fields fields = new Fields();
        fields.append(new Field("timestamp", "int"));
        fields.set(0, new Field("datetime", "string"));
        Field f = (Field) fields.get(0);
        Assert.assertNotNull(f);
    }


    /**
     * Method: getSimpleDesc()
     */
    @Test
    public void testSimpleDesc() throws Exception {
        Fields fields = new Fields();
        java.lang.reflect.Field field = fields.getClass().getDeclaredField("fields");
        field.setAccessible(true);
        field.set(fields, Lists.newArrayList("aa", "bb", "cc"));
        List<String> result = fields.getSimpleDesc();
        Assert.assertEquals(Lists.newArrayList(), result);
    }

    /**
     * Method: toList()
     */
    @Test
    public void testToList() throws Exception {
        Fields fields = new Fields();
        Assert.assertEquals(Lists.newArrayList(), fields.toList());
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
        Fields fields = new Fields();
        Assert.assertEquals(Lists.newArrayList().toString(), fields.toString());
    }

} 
