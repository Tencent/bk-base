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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskTest {

    private static Connection conn;
    private static Statement stat;

    /**
     * 创建db和table
     *
     * @throws SQLException
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        // TODO prepare db and table
//        conn = DriverManager.getConnection(TestConfig.connUrl, TestConfig.connUser, TestConfig.connPass);
//        stat = conn.createStatement();
//        stat.executeUpdate("create database " + TestConfig.dbName);
//        stat.close();
//        conn.close();
//
//        conn = DriverManager.getConnection(TestConfig.connUrl, TestConfig.connUser, TestConfig.connPass);
//        stat = conn.createStatement();
//        stat.executeUpdate("create table " + TestConfig.tableName + "(dtEventTime varchar(80), dtEventTimeStamp
//        timestamp, `localTime` time, id int, thedate date)");
    }

    /**
     * 删除db
     *
     * @throws SQLException
     */
    @AfterClass
    public static void afterClass() throws SQLException {
        // TODO clean db and table
//        stat.executeUpdate("drop database " + TestConfig.dbName);
//        stat.close();
//        conn.close();
    }

    /**
     * 测试任务启动/停止
     */
    @Test
    public void testStartStopTask() throws Exception {
        JdbcSourceConnector conn = new JdbcSourceConnector();
        conn.start(TestConfig.getProps());
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getProps());
        Thread.sleep(1000);
        task.stop();
        conn.stop();
    }

    /**
     * 测试时间范围模式采集 即采集历史数据
     */
    @Test
    public void testTimeMode() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getTimeProps());

        task.pollData();

        task.stop();
    }


    /**
     * 测试自增字段模式采集 即不采集历史数据
     */
    @Test
    public void testIncrementingMode() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getIncrementingProps());
        for (int i = 0; i < 10; i++) {
            task.pollData();
        }

        task.stop();
    }

    /**
     * 测试long类型timestamp模式采集,即不加载历史数据
     */
    @Test
    public void testTimeStampModeLongTimestamp() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getTimeStampProps());
        for (int i = 0; i < 10; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试string类型timestamp模式采集,即不加载历史数据
     */
    @Test
    public void testTimeStampMode() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getTimeProps());
        for (int i = 0; i < 100; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试 String 类型的 timestamp 转化
     * String timeString, String timestampColumn, String timestampTimeFormat
     *
     * "yyyyMMddHHmmss"
     * "yyyyMMddHHmm"
     * "yyyyMMdd HH:mm:ss.SSSSSS"
     * "yyyyMMdd HH:mm:ss"
     * "yyyyMMdd"
     *
     * "yyyy-MM-dd+HH:mm:ss"
     * "yyyy-MM-dd'T'HH:mm:ssXXX"
     * "yyyy-MM-dd HH:mm:ss.SSSSSS"
     * "yyyy-MM-dd HH:mm:ss"
     * "yyyy-MM-dd"
     *
     * "yy-MM-dd HH:mm:ss"
     * "Unix Time Stamp(seconds)"
     * "Unix Time Stamp(mins)"
     * "Unix Time Stamp(milliseconds)"
     * "MM/dd/yyyy HH:mm:ss"  ---- 不再提供转化
     * "dd/MMM/yyyy:HH:mm:ss" ---- 不再提供转化
     */
    @Test
    public void testStringTimestamp() throws Exception {
        TimestampIncrementingTableQuerier querier = PowerMockito.mock(TimestampIncrementingTableQuerier.class);

        Method method = PowerMockito
                .method(TimestampIncrementingTableQuerier.class, "stringTypeTimestampTranform", String.class);
        // Timestamp 无时区概念, 自动转为当地时区
        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampColumn")
                .set(querier, "timestampColumn");

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyyMMddHHmmss");
        assert ((Timestamp) method.invoke(querier, "19700101080000")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyyMMddHHmm");
        assert ((Timestamp) method.invoke(querier, "197001010800")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyyMMdd HH:mm:ss.SSSSSS");
        assert ((Timestamp) method.invoke(querier, "19700101 08:00:00.000000")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyyMMdd HH:mm:ss");
        assert ((Timestamp) method.invoke(querier, "19700101 08:00:00")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat").set(querier, "yyyyMMdd");
        assert ((Timestamp) method.invoke(querier, "19700101")).getTime() == -28800000;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyy-MM-dd+HH:mm:ss");
        assert ((Timestamp) method.invoke(querier, "1970-01-01+08:00:00")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyy-MM-dd'T'HH:mm:ssXXX");
        assert ((Timestamp) method.invoke(querier, "1970-01-01T08:00:00+08:00")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        assert ((Timestamp) method.invoke(querier, "1970-01-01 08:00:00.000000")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat")
                .set(querier, "yyyy-MM-dd HH:mm:ss");
        assert ((Timestamp) method.invoke(querier, "1970-01-01 08:00:00")).getTime() == 0;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat").set(querier, "yyyy-MM-dd");
        assert ((Timestamp) method.invoke(querier, "1970-01-01")).getTime() == -28800000;

        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat").set(querier, "yy-MM-dd");
        assert ((Timestamp) method.invoke(querier, "70-01-01")).getTime() == -28800000;

        String errorFormat = "yyyy-MM-dd HH:mm:ss";
        MemberModifier.field(TimestampIncrementingTableQuerier.class, "timestampTimeFormat").set(querier, errorFormat);

        try {
            method.invoke(querier, "01/Jan/1970:08:00:00");
        } catch (Exception e) {
            String errMsg =
                    "the type of timestampColumn: {timestampColumn} is String, but can't transform String to "
                            + "timestamp with TimeFormat:{"
                            + errorFormat + "}";
            Assert.assertEquals(errMsg, e.getCause().getMessage());
        }
    }

    /**
     * 测试历史数据和周期数据都加载的 timestamp类型timestamp模式采集
     * 加测: 同一Timestamp有超过300数据情况
     */
    @Test
    public void testTimestampLoadHistoryData() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getHisPeriodTimeProps());
        for (int i = 0; i < 100; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试历史数据和周期数据都加载的 String类型timestamp模式采集
     * 加测: 同一Timestamp有超过300数据情况
     */
    @Test
    public void testStrTimestampHistory() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getStringTypeTimeStampProps());
        for (int i = 0; i < 100; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试历史数据和周期数据都加载的 testHistoryAndPeriodMode_Increment模式采集
     * 加测: 同一Timestamp有超过300数据情况
     */
    @Test
    public void testIncrementLoadHistoryData() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getHisPeriodIncrementProps());
        for (int i = 0; i < 10; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试历史数据和周期数据都加载的 timestamp类型timestamp模式下  超过字节限制的情况
     * 加测: 同一Timestamp有超过300数据情况
     */
    @Test
    public void testTimestampDataMaxCapLimit() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        task.start(TestConfig.getHisPeriodTimeMaxCapProps());
        for (int i = 0; i < 100; i++) {
            task.pollData();
        }
        task.stop();
    }

    /**
     * 测试历史数据和周期数据都加载的 timestamp模式采集
     */
    @Test
    public void testTimestampTypeDataload() {
        JdbcSourceTask task = new JdbcSourceTask();

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());
        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        task.initialize(mockContext);
        Map testMap = TestConfig.getTimestampSecondsProps();
        testMap.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, "int_ts");
        testMap.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG,
                TimestampIncrementingTableQuerier.SECONDS);
//    testMap.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG, "yyyy-MM-dd HH:mm:ss");
//    testMap.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TS_MILLISECONDS);
        testMap.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TS_SECONDS);
        task.start(testMap);
        for (int i = 0; i < 10; i++) {
            task.pollData();
        }
        task.stop();
    }

    @Test
    public void tesConditions() {
        JdbcSourceTask task = new JdbcSourceTask();
        // ((id in 3) and (nr=123)) or (id in 1)
        String json = "[{\"key\":\"id\",\"logic_op\":\"and\",\"op\":\"in\",\"value\":\"3\"},{\"key\":\"nr\","
                + "\"logic_op\":\"and\",\"op\":\"=\",\"value\":\"123\"},{\"key\":\"id\",\"logic_op\":\"or\","
                + "\"op\":\"in\",\"value\":\"1\"}]";
        task.makeConditions(json);

        // empty
        {
            Map<String, Object> data = new HashMap<>();
            assert !task.match(data);
        }

        // test and
        {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "333");  // match
            data.put("nr", "123");  // match
            assert task.match(data);
        }

        {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "333");  // match
            data.put("nr", "12");  // not match
            assert !task.match(data);
        }

        {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "4");  // not match
            data.put("nr", "123");  // match
            assert !task.match(data);
        }

        {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "4");  // not match
            data.put("nr", "12");  // not match
            assert !task.match(data);
        }

        // test or
        {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "1");  // match
            data.put("nr", "12");  // not match
            assert task.match(data);
        }
    }

}
