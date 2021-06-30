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
package com.tencent.bk.base.datahub.databus.connect.source.datanode;

import static org.junit.Assert.assertNotNull;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

/**
 * DatanodeSourceConfig Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/18/2018</pre>
 */
public class DatanodeSourceConfigTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: get(Object key)
     */
    @Test
    public void testGet() throws Exception {
    }

    @Test
    public void testConstructor() {
        Map<String, String> props = new HashMap<>();
        props.put(DatanodeSourceConfig.SOURCE_RT_LIST, "105_abc,105_aaa");
        props.put(DatanodeSourceConfig.CONFIG, "{\"a\": 105}");
        props.put(BkConfig.GROUP_ID, "a.b.c");
        props.put(BkConfig.RT_ID, "105_merge_a");
        props.put(BkConfig.CONNECTOR_NAME, "puller-merge_105_merge_a");

        DatanodeSourceConfig obj = new DatanodeSourceConfig(props);
        assertNotNull(obj);
    }


} 
