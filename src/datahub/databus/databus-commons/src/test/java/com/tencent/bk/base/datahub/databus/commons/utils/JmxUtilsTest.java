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
import static org.junit.Assert.assertNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.naming.*", "javax.security.*"})
public class JmxUtilsTest {

    @BeforeClass
    public static void beforeClass() {
        JmxTestAgentRemote.initAndStart();
    }

    @AfterClass
    public static void afterClass() {
        JmxTestAgentRemote.stop();
    }

    @Test
    public void getJmxConnectorCase0() {
        JMXConnector jmxConnector = JmxUtils.getJmxConnector("localhost", "9999");
        assertNotNull(jmxConnector);
    }

    @Test
    public void getJmxConnectorCase1() {
        JMXConnector jmxConnector = JmxUtils.getJmxConnector("localhost", "8888");
        assertNull(jmxConnector);
    }


    @Test(expected = IOException.class)
    public void closeJmxConnectorCase0() throws Exception {
        JMXConnector jmxConnector = JmxUtils.getJmxConnector("localhost", "9999");
        String conId = jmxConnector.getConnectionId();
        JmxUtils.closeJmxConnector(jmxConnector);
        assertEquals(conId, jmxConnector.getConnectionId());
    }

    @Test
    public void closeJmxConnectorCase1() throws Exception {
        JMXConnector mockConn = PowerMockito.mock(JMXConnector.class);
        PowerMockito.when(mockConn, "close").thenThrow(IOException.class);
        JmxUtils.closeJmxConnector(mockConn);
        assertNotNull(mockConn);
    }

    @Test
    public void closeJmxConnectorCase2() throws Exception {
        JMXConnector conn = null;
        JmxUtils.closeJmxConnector(conn);
        assertNull(conn);
    }

    @Test
    public void getAllMbeanNamesCase0() {
        Set<ObjectName> objectNameSet = JmxUtils.getAllMbeanNames("localhost", "9999");
        assertNotNull(objectNameSet);
    }

    @Test
    public void getAllMbeanNamesCase1() {
        Set<ObjectName> objectNameSet = JmxUtils.getAllMbeanNames("localhost", "8888");
        assertNull(objectNameSet);
    }

    @Test
    @PrepareForTest(JmxUtils.class)
    public void getAllMbeanNamesCase2() throws Exception {
        PowerMockito.whenNew(JMXServiceURL.class).withAnyArguments().thenThrow(MalformedURLException.class);
        Set<ObjectName> objectNameSet = JmxUtils.getAllMbeanNames("localhost", "9999");
        assertNull(objectNameSet);
    }

    @Test
    @PrepareForTest(JmxUtils.class)
    public void getAllMbeanNamesCase3() throws Exception {
        PowerMockito.whenNew(JMXServiceURL.class).withAnyArguments().thenThrow(NullPointerException.class);
        Set<ObjectName> objectNameSet = JmxUtils.getAllMbeanNames("localhost", "9999");
        assertNull(objectNameSet);
    }

    @Test
    public void testConstructor() {
        JmxUtils obj = new JmxUtils();
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(JmxUtils.class)
    public void testCloseConn() throws Exception {
        JMXConnector mockConn = PowerMockito.mock(JMXConnector.class);
        PowerMockito.when(mockConn, "close").thenThrow(IOException.class);
        Whitebox.invokeMethod(JmxUtils.class, "closeConn", mockConn);
        assertNotNull(mockConn);
    }
}