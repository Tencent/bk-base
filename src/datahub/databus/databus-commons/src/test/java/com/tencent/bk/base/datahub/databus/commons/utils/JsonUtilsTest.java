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

package com.tencent.bk.base.datahub.databus.commons.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;

import org.junit.Test;

import java.util.Map;

/**
 * JsonUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/14/2018</pre>
 */
public class JsonUtilsTest {


    @Test
    public void testConstructor() {
        JsonUtils jsonUtils = new JsonUtils();
        assertNotNull(jsonUtils);
    }

    /**
     * Method: toJsonWithoutException(Object obj)
     */
    @Test
    public void testToJsonWithoutExceptionCase0() throws Exception {
        Map<String, String> testMap = Maps.newHashMap();
        testMap.put("k1", "v1");
        testMap.put("k2", "v2");
        String result = JsonUtils.toJsonWithoutException(testMap);
        String expected = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        assertEquals(expected, result);
    }

    /**
     * Method: toJsonWithoutException(Object obj)
     */
    @Test
    public void testToJsonWithoutExceptionCase1() throws Exception {
        String result = JsonUtils.toJsonWithoutException(new TestBean());
        String expected = "{}";
        assertEquals(expected, result);
    }

    /**
     * Method: toJson(Object obj)
     */
    @Test
    public void testToJson() throws Exception {
        Map<String, String> testMap = Maps.newHashMap();
        testMap.put("k1", "v1");
        testMap.put("k2", "v2");
        String result = JsonUtils.toJson(testMap);
        String expected = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        assertEquals(expected, result);
    }

    /**
     * Method: toMapWithoutException(String json)
     */
    @Test
    public void testToMapWithoutExceptionCase0() throws Exception {
        String mapStr = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        Map<?, ?> result = JsonUtils.toMapWithoutException(mapStr);
        Map<String, String> expected = Maps.newHashMap();
        expected.put("k1", "v1");
        expected.put("k2", "v2");

        assertEquals(expected, result);
    }

    /**
     * Method: toMapWithoutException(String json)
     */
    @Test
    public void testToMapWithoutExceptionCase1() throws Exception {
        String mapStr = "k1,k2";
        Map<?, ?> result = JsonUtils.toMapWithoutException(mapStr);
        assertEquals(Maps.newHashMap(), result);
    }

    /**
     * Method: toMap(String json)
     */
    @Test
    public void testToMap() throws Exception {
        String mapStr = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        Map<?, ?> result = JsonUtils.toMap(mapStr);
        Map<String, String> expected = Maps.newHashMap();
        expected.put("k1", "v1");
        expected.put("k2", "v2");

        assertEquals(expected, result);
    }

    class TestBean {

    }


} 
