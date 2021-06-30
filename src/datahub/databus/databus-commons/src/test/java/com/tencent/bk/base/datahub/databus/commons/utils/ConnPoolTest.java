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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;

@RunWith(PowerMockRunner.class)
public class ConnPoolTest {

    private static String connUrl;
    private static String badConnUrl = "jdbc:mysql://localhost:3305/mysql?";

    static {
        try {
            Class.forName("org.sqlite.JDBC");
            Path dbPath = Paths.get("ConnPoolTest.db");
            Files.deleteIfExists(dbPath);
            connUrl = "jdbc:sqlite:" + dbPath;
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    public void getConnectionCase0() throws Exception {
        Connection connection = ConnPool.getConnection(connUrl,
                "root",
                "123456");
        ConnPool.returnToPool(connUrl, connection);
        assertNotNull(connection);
    }

    @Test
    public void shutdownCase0() throws Exception {
        Connection connection1 = ConnPool.getConnection(connUrl,
                "root",
                "123456");
        assertNotNull(connection1);
        ConnPool.returnToPool(connUrl, connection1);
        ConnPool.shutdown();
        //Thread.sleep(1100);
    }

    @Test
    @PrepareForTest(ConnUtils.class)
    public void shutdownCase1() throws Exception {
        PowerMockito.spy(ConnUtils.class);
        PowerMockito.when(ConnUtils.class, "closeQuietly", Matchers.any()).thenThrow(InterruptedException.class);
        Connection connection1 = ConnPool.getConnection(connUrl,
                "root",
                "123456");
        ConnPool.returnToPool(connUrl, connection1);
        ConnPool.shutdown();
        assertNotNull(connection1);
        //Thread.sleep(1100);
    }


    @Test
    @PrepareForTest(LogUtils.class)
    public void getConnectionCase1() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Connection connection = ConnPool.getConnection(badConnUrl,
                "root",
                "123456");
        ConnPool.returnToPool(badConnUrl, connection);
        assertNull(connection);
    }

    @Test
    public void returnToPoolCase0() throws Exception {
        Connection conn = ConnPool.getConnection(connUrl,
                "root",
                "123456");
        ConnPool.returnToPool(connUrl, conn);
        Thread.sleep(1000);

    }

    @Test
    public void returnToPoolCase1() throws Exception {
        Connection conn1 = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        Connection conn2 = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        Connection conn3 = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        Connection conn4 = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        ConnPool.returnToPool(connUrl, conn1);
        ConnPool.returnToPool(connUrl, conn2);
        ConnPool.returnToPool(connUrl, conn3);
        ConnPool.returnToPool(connUrl, conn4);
        Thread.sleep(2000);

    }


    @Test
    @PrepareForTest(ConnPool.class)
    public void returnToPoolCase4() throws Exception {
        Connection conn = ConnPool.getConnection(connUrl,
                "root",
                "123456");
        Connection spyConn = PowerMockito.spy(conn);
        PowerMockito.when(spyConn.isValid(1)).thenThrow(SQLException.class);
        ConnPool.returnToPool(connUrl, spyConn);
        Thread.sleep(1000);
    }


    @Test
    public void getConnectionUrlKeyCase0() {
        String urlKey = ConnPool.getConnectionUrlKey(connUrl);
        String expected = "jdbc:sqlite:ConnPoolTest.db";
        assertEquals(expected, urlKey);
    }

    @Test
    public void getConnectionUrlKeyCase1() {
        String urlKey = ConnPool.getConnectionUrlKey("jdbc:mysql");
        String expected = "jdbc:mysql";
        assertEquals(expected, urlKey);
    }

    @Test
    public void testConstructor() {
        ConnPool connPool = new ConnPool();
        assertNotNull(connPool);
    }
}