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

package com.tencent.bk.base.datahub.databus.commons.convert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.Charset;

@RunWith(PowerMockRunner.class)
public class JsonConverterTest {

    @Test
    public void getListObjectsCase0() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("k1", "v1".getBytes(Charset.forName("iso_8859_1")), null);
        assertEquals(Lists.newArrayList("BadJsonListError"), result.getErrors());
    }

    @Test
    @PrepareForTest(JsonConverter.class)
    public void getListObjectsCase1() throws Exception {

        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("k1", "v1".getBytes("ISO-8859-1"), null);
        assertNotNull(result);
    }

    @Test
    public void testGetListObjects1Case0() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("k1", "v1", null);
        assertEquals(Lists.newArrayList("BadJsonListError"), result.getErrors());
    }

    @Test
    public void testGetListObjects1Case1() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("DatabusEvent", "v1", null);
        assertEquals(Lists.newArrayList("BadJsonListError"), result.getErrors());
    }


    @Test
    public void testGetListObjects1Case2() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("DatabusEvent", "", null);
        assertTrue(result.getIsDatabusEvent());
    }

    @Test
    public void testGetListObjects1Case3() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getListObjects("k1", "", null);
        assertNotNull(result);
    }

    @Test
    public void testGetJsonListCase0() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getJsonList("k1", "");
        assertEquals(Lists.newArrayList(""), result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase1() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getJsonList("DatabusEvent", "");
        assertEquals(Lists.newArrayList(), result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase2() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getJsonList("DatabusEvent", "v1");
        assertEquals(Lists.newArrayList("v1"), result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase3() {
        JsonConverter obj = new JsonConverter();
        ConvertResult result = obj.getJsonList("", "");
        assertEquals(Lists.newArrayList(""), result.getJsonResult());
    }

    @Test
    public void testConstructor() {
        JsonConverter obj = new JsonConverter();
        assertNotNull(obj);

    }
}