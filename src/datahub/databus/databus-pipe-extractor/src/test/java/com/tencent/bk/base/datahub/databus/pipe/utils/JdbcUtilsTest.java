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

package com.tencent.bk.base.datahub.databus.pipe.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.sql.*;
import java.util.Map;

/**
 * JdbcUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/04/2018</pre>
 */
public class JdbcUtilsTest {

    /**
     * 测试closeConnection正常情况
     */
    @Test
    public void testCloseConnectionSuccess() throws Exception {
        Path dbPath = Paths.get( JdbcUtilsTest.class.getSimpleName() + ".db");
        String connUrl = "jdbc:sqlite:" + dbPath;
        String connUser = "root";
        String connPass = "123456";
        Class.forName("org.sqlite.JDBC");
        Files.deleteIfExists(dbPath);
        Connection connection = JdbcUtils.newConnection(connUrl, connUser, connPass, 3);
        JdbcUtils.closeConnection(connection);
        Assert.assertNotNull(connection);
    }

    /**
     * 测试closeDbResource失败情况
     */
    @Test
    public void testCloseDbResourceFailed() throws Exception {
        ResultSet rs = PowerMockito.mock(ResultSet.class);
        Statement stat = PowerMockito.mock(Statement.class);
        PowerMockito.doThrow(new SQLException()).when(rs).close();
        PowerMockito.doThrow(new SQLException()).when(stat).close();
        JdbcUtils.closeDbResource(rs, stat, null);
    }

    /**
     * 测试closeConnection失败情况
     */
    @Test
    public void testCloseConnectionFailed() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException()).when(connection).close();
        JdbcUtils.closeConnection(connection);
    }

    /**
     * 测试getCache正常情况
     */
    @Test
    public void testGetCacheSuccess() throws Exception {
        Path dbPath = Paths.get( JdbcUtilsTest.class.getSimpleName() + ".db");
        String connUrl = "jdbc:sqlite:" + dbPath;
        String connUser = "root";
        String connPass = "123456";
        Class.forName("org.sqlite.JDBC");
        Files.deleteIfExists(dbPath);

        String connDb = "mysql";
        String tableName = "user";
        String keyColumns = "Host";
        String mapColumn = "User";
        String separator = "|";
        Map<String, Object> cache = JdbcUtils
                .getCache(connUrl, connUser, connPass, connDb, tableName, mapColumn, keyColumns, separator);
        Assert.assertNotNull(cache);
    }

    /**
     * 测试getCache失败情况
     */
    @Test
    public void testGetCacheFailed() {
        new JdbcUtils();
        String connUrl = "jdbc:mysql://mysql:3306/mysql";
        String connUser = "root";
        String connPass = "123456";
        String connDb = "mysql";
        String tableName = "xxx";
        String keyColumns = "Host";
        String mapColumn = "User";
        String separator = "|";
        Map<String, Object> cache = JdbcUtils
                .getCache(connUrl, connUser, connPass, connDb, tableName, mapColumn, keyColumns, separator);
        Assert.assertEquals(cache.size(), 0);
    }
} 
