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

package com.tencent.bk.base.dataflow.codecheck.util.db;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Connection.class, PreparedStatement.class, DBConnectionUtil.class,
        DBCloseUtil.class, CodeCheckDBUtil.class, ResultSet.class})
public class CodeCheckDBUtilTest {

    private AutoCloseable closeable;

    @Before
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void addParserGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.when(preparedStatement.execute()).thenReturn(true);
        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addParserGroup("a", "b", "c");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addParserGroup("a", "b", "c");
    }

    @Test
    public void addParserGroup_2() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.doThrow(new SQLException()).when(preparedStatement).execute();
        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addParserGroup("a", "b", "c");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addParserGroup("a", "b", "c");
    }

    @Test
    public void getParserGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        ResultSetMetaData resultSetMetaData = PowerMockito.mock(ResultSetMetaData.class);
        PowerMockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        int columnCount = 3;
        PowerMockito.when(resultSetMetaData.getColumnCount()).thenReturn(columnCount);
        PowerMockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
        PowerMockito.when(resultSetMetaData.getColumnName(anyInt())).thenReturn("a").thenReturn("b").thenReturn("c");
        PowerMockito.when(resultSet.getObject(anyInt())).thenReturn(1).thenReturn(2).thenReturn(3);

        List retList = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("a", 1);
                put("b", 2);
                put("c", 3);
            }});
        }};

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);

        List ret = CodeCheckDBUtil.getParserGroup("a");
        Assert.assertEquals(retList, ret);
    }

    @Test
    public void listParserGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        List retList = new ArrayList<Map<String, String>>();
        PowerMockito.doReturn(retList).when(CodeCheckDBUtil.class, "convertToList", resultSet);
        List ret = CodeCheckDBUtil.listParserGroup();
        Assert.assertEquals(retList, ret);
    }

    @Test
    public void deleteParserGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.when(preparedStatement.execute()).thenReturn(true);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteParserGroup("a");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteParserGroup("a");
    }

    @Test
    public void addBlacklistGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.when(preparedStatement.execute()).thenReturn(true);
        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addBlacklistGroup("a", "b");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addBlacklistGroup("a", "b");
    }

    @Test
    public void addBlacklistGroup_2() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.doThrow(new SQLException()).when(preparedStatement).execute();
        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addBlacklistGroup("a", "b");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addBlacklistGroup("a", "b");
    }

    @Test
    public void getBlacklistGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        List retList = new ArrayList<Map<String, String>>();
        PowerMockito.doReturn(retList).when(CodeCheckDBUtil.class, "convertToList", resultSet);
        List ret = CodeCheckDBUtil.getBlacklistGroup("a");
        Assert.assertEquals(retList, ret);
    }

    @Test
    public void listBlacklistGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        List retList = new ArrayList<Map<String, String>>();
        PowerMockito.doReturn(retList).when(CodeCheckDBUtil.class, "convertToList", resultSet);
        List ret = CodeCheckDBUtil.listBlacklistGroup();
        Assert.assertEquals(retList, ret);
    }

    @Test
    public void deleteBlacklistGroup() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.class, "getSingleton").thenReturn(dbConnectionUtil);
        PowerMockito.doReturn(conn).when(dbConnectionUtil).getConnection();

        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(conn.prepareStatement(anyString())).thenReturn(preparedStatement);
        PowerMockito.doNothing().when(preparedStatement).setString(anyInt(), anyString());
        PowerMockito.when(preparedStatement.execute()).thenReturn(true);

        PowerMockito.mockStatic(DBCloseUtil.class);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", conn);
        PowerMockito.doNothing().when(DBCloseUtil.class, "close", preparedStatement);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.spy(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteBlacklistGroup("a");
        verifyStatic(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteBlacklistGroup("a");
    }
}