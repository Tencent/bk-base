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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * BizHdfsSinkConnector Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
public class BizHdfsSinkConnectorTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: start(Map<String, String> props)
     */
    @Test
    public void testStart() throws Exception {
        BizHdfsSinkConnector connector = new BizHdfsSinkConnector();
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_hdfs_url");
        props.put("biz.id", "101");
        connector.start(props);
    }

    /**
     * Method: stop()
     */
    @Test
    public void testStop() throws Exception {
        BizHdfsSinkConnector connector = new BizHdfsSinkConnector();
        connector.stop();
        assertNotNull(connector);
    }

    /**
     * Method: taskClass()
     */
    @Test
    public void testTaskClass() throws Exception {
        BizHdfsSinkConnector connector = new BizHdfsSinkConnector();
        assertEquals("com.tencent.bk.base.datahub.databus.connect.hdfs.BizHdfsSinkTask",
                connector.taskClass().getName());
    }

    /**
     * Method: config()
     */
    @Test
    public void testConfig() throws Exception {
        BizHdfsSinkConnector connector = new BizHdfsSinkConnector();
        assertNotNull(connector.config());
    }

    @Test
    public void testConstructor() {
        BizHdfsSinkConnector connector = new BizHdfsSinkConnector();
        assertNotNull(connector);
    }


} 
