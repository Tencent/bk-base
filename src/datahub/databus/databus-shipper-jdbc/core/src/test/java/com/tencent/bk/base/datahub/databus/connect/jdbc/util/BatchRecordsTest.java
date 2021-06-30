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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

import com.tencent.bk.base.datahub.databus.connect.jdbc.TestConfig;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.ConnectionBrokenException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DataTooLongException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderInstanceDownException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderNoPartitionException;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnPool;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class BatchRecordsTest {

    private static final String escape = "`";

    /**
     * 测试flush触发ConnectException的情况：unable to connect database
     */
    @Test(expected = ConnectException.class)
    public void testFlushConnectException() {
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        List<Object> list = new ArrayList<>();
        list.add(1);
        recordList.add(list);
        batchRecords.add(recordList);
    }

    /**
     * 因为没有集成tspider的测试环境，所以tspider相关的异常只能通过mock去覆盖，覆盖handleTspiderNoPartition中bad.size() > 0的分支
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testTspiderNoPartitionException1() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new TspiderNoPartitionException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String today = df.format(new Date(System.currentTimeMillis()));
        String[] colsInOrder = {"col1", "thedate", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        batchRecords.flush();
        Field field1 = batchRecords.getClass().getDeclaredField("threshold");
        field1.setAccessible(true);
        field1.set(batchRecords, 100);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Arrays.asList(1, Integer.parseInt(today) + 1));
        recordList.add(Arrays.asList(1, Integer.parseInt(today) - 8));
        recordList.add(Arrays.asList(1, Integer.parseInt(today) - 3));
        recordList.add(Arrays.asList("", "bad record"));
        batchRecords.add(recordList);
        Field field2 = batchRecords.getClass().getDeclaredField("buffered");
        field2.setAccessible(true);
        Assert.assertEquals(0, ((ArrayList) field2.get(batchRecords)).size());
    }

    /**
     * 覆盖handleTspiderNoPartition中bad.size() == 0的分支：Table has no partition' exception but all thedate values are valid
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    @PrepareForTest({JdbcUtils.class, ConnPool.class, ConnUtils.class})
    public void testTspiderNoPartitionException2() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new TspiderNoPartitionException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        PowerMockito.mockStatic(ConnPool.class);
        PowerMockito.when(ConnPool.getConnection(TestConfig.connUrlInit, TestConfig.connUser, TestConfig.connPass))
                .thenReturn(null);
        Connection connection = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(ConnUtils.class);
        PowerMockito
                .when(ConnUtils.initConnection(TestConfig.connUrlInit, TestConfig.connUser, TestConfig.connPass, 1000))
                .thenReturn(connection);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String today = df.format(new Date(System.currentTimeMillis()));
        String[] colsInOrder = {};
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList(Integer.parseInt(today) - 3));
        batchRecords.add(recordList);
    }

    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testTspiderNoPartitionException3() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doNothing().when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String today = df.format(new Date(System.currentTimeMillis()));
        String[] colsInOrder = {};
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList(Integer.parseInt(today) - 9));
        Method method = batchRecords.getClass()
                .getDeclaredMethod("handleTspiderNoPartition", Connection.class, List.class);
        method.setAccessible(true);
        method.invoke(batchRecords, ConnPool.getConnection("", "", ""), recordList);
    }

    /**
     * 覆盖flush方法中Catch DataTooLongException的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testDataTooLongException() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new DataTooLongException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList("xx"));
        batchRecords.add(recordList);
        Field field = batchRecords.getClass().getDeclaredField("buffered");
        field.setAccessible(true);
        Assert.assertEquals(0, ((ArrayList) field.get(batchRecords)).size());
    }

    /**
     * 覆盖flush方法中Catch ConnectionBrokenException的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testConnectionBrokenException() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new ConnectionBrokenException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList("xx"));
        batchRecords.add(recordList);
        Field field = batchRecords.getClass().getDeclaredField("buffered");
        field.setAccessible(true);
        Assert.assertEquals(0, ((ArrayList) field.get(batchRecords)).size());
    }

    /**
     * 覆盖flush方法中Catch TspiderInstanceDownException的情况：failed to write tspider as some instance is down!
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    @PrepareForTest({JdbcUtils.class})
    public void testTspiderInstanceDownException1() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new TspiderInstanceDownException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList("xx"));
        batchRecords.add(recordList);
    }

    @Test(expected = ConnectException.class)
    @PrepareForTest({JdbcUtils.class, BatchRecords.class})
    public void testTspiderInstanceDownException2() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new TspiderInstanceDownException("")).when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        Thread.sleep(15000);
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        List<List<Object>> recordList = new ArrayList<>();
        recordList.add(Collections.singletonList("xx"));
        batchRecords.add(recordList);
    }

    /**
     * jacoco覆盖率有问题，所以单独把cleanDataFromDb抽出来测试
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testCleanDataFromDb() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doThrow(new NullPointerException()).when(JdbcUtils.class);
        JdbcUtils.deleteRecords(anyObject(), anyString(), anyString(), anyObject(), anyList(), escape);
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        Method method = batchRecords.getClass().getDeclaredMethod("cleanDataFromDb", Connection.class, List.class);
        method.setAccessible(true);
        method.invoke(batchRecords, ConnPool.getConnection("xx", "xx", "xx"), new ArrayList<>());
    }

    /**
     * jacoco覆盖率有问题，所以单独把handleConnectionBroken抽出来测试
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({JdbcUtils.class, ConnPool.class})
    public void testHandleConnectionBroken() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doNothing().when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        PowerMockito.mockStatic(ConnPool.class);
        PowerMockito.doNothing().when(ConnPool.class);
        ConnPool.returnToPool(anyString(), anyObject());
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        Method method = batchRecords.getClass().getDeclaredMethod("handleConnectionBroken", List.class);
        method.setAccessible(true);
        method.invoke(batchRecords, new ArrayList<>());
    }

    @Test
    public void testHandleDataTooLong1() throws Exception {
        String sql = "INSERT INTO test (col1) VALUES (?)";
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        Method method = batchRecords.getClass().getDeclaredMethod("handleDataTooLong", Connection.class, List.class);
        method.setAccessible(true);
        method.invoke(batchRecords, ConnPool.getConnection("xx", "xx", "xx"),
                Collections.singletonList(Collections.singletonList("xx")));
    }

    @Test
    @PrepareForTest({JdbcUtils.class})
    public void testHandleDataTooLong2() throws Exception {
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.doNothing().when(JdbcUtils.class);
        JdbcUtils.batchInsert(anyObject(), anyString(), anyList(), anyInt());
        String sql = "INSERT INTO test (col1) VALUES (?)";
        BatchRecordsConfig batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(TestConfig.dbName)
                .tableName(TestConfig.tableName)
                .colsInOrder(TestConfig.columnNames)
                .insertSql("")
                .connUrl("")
                .connUser(TestConfig.connUser)
                .connPass(TestConfig.connPass)
                .batchSize(1)
                .timeoutSeconds(30)
                .maxDelayDays(7)
                .retryBackoffMs(1000)
                .build();
        BatchRecords batchRecords = new BatchRecords(batchRecordsConfig, escape);
        Method method = batchRecords.getClass().getDeclaredMethod("handleDataTooLong", Connection.class, List.class);
        method.setAccessible(true);
        method.invoke(batchRecords, ConnPool.getConnection("xx", "xx", "xx"),
                Collections.singletonList(Collections.singletonList("xx")));
    }

    /**
     * 测试batchInsert方法中触发SQLException的情况：failed to execute sql
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    @PrepareForTest({JdbcUtils.class})
    public void testBatchInsertSQLException() throws Exception {
        String sql = "INSERT INTO test (col1) VALUES (?)";
        Connection connection = PowerMockito.mock(Connection.class);
        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.doThrow(new SQLException("else msg")).when(preparedStatement).setQueryTimeout(0);
        PowerMockito.doThrow(new SQLException("close exception")).when(preparedStatement).close();
        PowerMockito.when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        Thread.sleep(anyLong());
        JdbcUtils.batchInsert(connection, sql, new ArrayList<>(), 0);
    }
}
