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

package com.tencent.bk.base.datahub.databus.connect.jdbc.util;

import com.tencent.bk.base.datahub.databus.connect.jdbc.TestConfig;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.ConnectionBrokenException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DataTooLongException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderInstanceDownException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderNoPartitionException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

public class JdbcUtilsTest {

    private static final String escape = "`";


    /**
     * 测试buildInsertSql方法
     */
    @Test
    public void testBuildInsertSql() {
        Assert.assertEquals("INSERT INTO test (col1) VALUES (?)",
                JdbcUtils.buildInsertSql(TestConfig.tableName, TestConfig.columnNames, TestConfig.escape));
    }

    /**
     * 测试batchInsert方法中触发TspiderNoPartitionException的情况
     *
     * @throws Exception
     */
    @Test(expected = TspiderNoPartitionException.class)
    public void testNoPartitionException() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Table has no partition")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发DataTooLongException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataTooLongException.class)
    public void testTooLongException1() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Data too long for column")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发DataTooLongException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataTooLongException.class)
    public void testTooLongException2() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Data truncated for column")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发DataTooLongException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataTooLongException.class)
    public void testTooLongException3() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Data truncation")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发DataTooLongException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataTooLongException.class)
    public void testTooLongException4() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("cannot be null")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectionBrokenException.class)
    public void testConnectionBrokenException1() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Remote MySQL server has gone away")).when(connection)
                .prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectionBrokenException.class)
    public void testConnectionBrokenException2() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("No operations allowed")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectionBrokenException.class)
    public void testConnectionBrokenException3() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Communications link failure")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectionBrokenException.class)
    public void testConnectionBrokenException4() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("The last packet successfully received from the server")).when(connection)
                .prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectionBrokenException.class)
    public void testConnectionBrokenException5() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Write failed")).when(connection).prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发TspiderInstanceDownException的情况
     *
     * @throws Exception
     */
    @Test(expected = TspiderInstanceDownException.class)
    public void testTspiderInstanceDownException() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("Unable to connect to foreign data source")).when(connection)
                .prepareStatement("sql");
        JdbcUtils.batchInsert(connection, "sql", new ArrayList<>(), 0);
    }

    /**
     * 测试batchInsert方法中触发SQLException的情况：failed to execute sql
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testSQLException() throws Exception {
        String sql = "INSERT INTO test (col1) VALUES (?)";
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("else msg")).when(connection).prepareStatement(sql);
        JdbcUtils.batchInsert(connection, sql, new ArrayList<>(), 0);
    }

    /**
     * 测试DeleteRecords方法
     *
     * @throws SQLException
     */
    @Test
    public void testDeleteRecords() throws SQLException {
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.doThrow(new SQLException("else msg")).when(connection).createStatement();
        String[] columns = {"col1", "col2", "col3"};
        List<List<Object>> records = new ArrayList<>();
        records.add(Arrays.asList("str", 1, null));
        JdbcUtils.deleteRecords(connection, "", "table", columns, records, escape);
        JdbcUtils.deleteRecords(connection, "", "table", columns, new ArrayList<>(), escape);
    }

    /**
     * 测试getRecordsInString方法
     */
    @Test
    public void testGetRecordsInString() {
        String sql = "INSERT INTO test (col1) VALUES (?)";
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList("val1"));
        String result = JdbcUtils.getRecordsInString(sql, recordList);
        Assert.assertEquals("\n" + "INSERT INTO test (col1) VALUES  ();", result);
    }

    /**
     * 覆盖execSql方法中finally代码块catch异常的分支
     */
    @Test
    public void testExecSql() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        Statement statement = PowerMockito.mock(Statement.class);
        PowerMockito.doThrow(new SQLException()).when(statement).close();
        PowerMockito.when(connection.createStatement()).thenReturn(statement);
        Method method = JdbcUtils.class.getDeclaredMethod("execSql", Connection.class, List.class);
        method.setAccessible(true);
        Assert.assertEquals("", method.invoke(JdbcUtils.class, connection, new ArrayList<>()));
    }

    /**
     * 覆盖buildConditions方法中needEscape为false的情况
     *
     * @throws Exception
     */
    @Test
    public void testBuildConditions() throws Exception {
        String[] columns = {"col1"};
        List<Object> record = Collections.singletonList("record1");
        Method method = JdbcUtils.class.getDeclaredMethod("buildConditions", String[].class, List.class, boolean.class);
        method.setAccessible(true);
        Assert.assertEquals("`col1`='record1'", method.invoke(JdbcUtils.class, columns, record, false));
    }

    @Test
    public void testConstructor() {
        JdbcUtils utils = new JdbcUtils();
        Assert.assertNotNull(utils);
    }
}
