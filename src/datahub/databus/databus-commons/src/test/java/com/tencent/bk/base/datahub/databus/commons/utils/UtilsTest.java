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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Utils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/13/2018</pre>
 */
public class UtilsTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: getTdwFinishDatabusEvent(String folder, String timestamp)
     */
    @Test
    public void testGetTdwFinishDatabusEvent() throws Exception {
        String result = Utils.getTdwFinishDatabusEvent("/api/xxx/xxx", "2018121216");
        String expected = "DatabusEvent:ds=2018121216&tdwFinishDir=/api/xxx/xxx";
        assertEquals(expected, result);
    }

    /**
     * Method: parseDatabusEvent(String event)
     */
    @Test
    public void testParseDatabusEventCase0() throws Exception {
        String event = "DatabusEvent:collector-ds=2018121216&tdwFinishDir=/api/xxx/xxx";
        Map<String, String> expected = Maps.newHashMap();
        expected.put("collector-ds", "2018121216");
        expected.put("tdwFinishDir", "/api/xxx/xxx");
        expected.put("ds", "2018121216");
        Map<String, String> result = Utils.parseDatabusEvent(event);
        assertEquals(expected, result);
    }

    /**
     * Method: parseDatabusEvent(String event)
     */
    @Test
    public void testParseDatabusEventCase1() throws Exception {
        String event = "testEvent";
        Map<String, String> expected = Maps.newHashMap();
        Map<String, String> result = Utils.parseDatabusEvent(event);
        assertEquals(expected, result);
    }

    /**
     * Method: readUrlTags(String data)
     */
    @Test
    public void testReadUrlTagsCase0() throws Exception {
        String url = "k1=v1&k2=v2&k3=v3";
        Map<String, String> expected = Maps.newHashMap();
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        expected.put("k3", "v3");
        Map<String, String> result = Utils.readUrlTags(url);
        assertEquals(expected, result);
    }

    /**
     * Method: readUrlTags(String data)
     */
    @Test
    public void testReadUrlTagsCase1() throws Exception {
        String url = "%x";
        Map<String, String> expected = Maps.newHashMap();
        Map<String, String> result = Utils.readUrlTags(url);
        assertEquals(expected, result);
    }

    /**
     * Method: splitKeyValue(String urlParams)
     */
    @Test
    public void testSplitKeyValueCase0() throws Exception {
        String url = "k1=v1&k2=v2&k3=v3";
        Map<String, String> expected = Maps.newHashMap();
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        expected.put("k3", "v3");
        Map<String, String> result = Utils.splitKeyValue(url);
        assertEquals(expected, result);
    }

    /**
     * Method: splitKeyValue(String urlParams)
     */
    @Test
    public void testSplitKeyValueCase1() throws Exception {
        String url = "k1=&k2=v2&k3=v3";
        Map<String, String> expected = Maps.newHashMap();
        expected.put("k1", "");
        expected.put("k2", "v2");
        expected.put("k3", "v3");
        Map<String, String> result = Utils.splitKeyValue(url);
        assertEquals(expected, result);
    }

    /**
     * Method: getDsTimeTag(String key)
     */
    @Test
    public void testGetDsTimeTagCase0() throws Exception {
        //collector-ds
        String url = "k1=v1&k2=v2&k3=v3&collector-ds=15454811111";
        Map<String, String> dsTags = Utils.readUrlTags(url);
        long tag = Utils.getDsTimeTag(dsTags);
        assertEquals(15454811111l, tag);
    }

    /**
     * Method: getDsTimeTag(String key)
     */
    @Test
    public void testGetDsTimeTagCase1() throws Exception {
        //collector-ds
        String url = "k1=v1&k2=v2&k3=v3&collector-ds=xxx";
        Map<String, String> dsTags = Utils.readUrlTags(url);
        long tag = Utils.getDsTimeTag(dsTags);
        assertEquals(0, tag);
    }

    /**
     * Method: main(String[] args)
     */
    @Test
    public void testConstructor() throws Exception {
        Utils utils = new Utils();
        assertNotNull(utils);

    }


} 
