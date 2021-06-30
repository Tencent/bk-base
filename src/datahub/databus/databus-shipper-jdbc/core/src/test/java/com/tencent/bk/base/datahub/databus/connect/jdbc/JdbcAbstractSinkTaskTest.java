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


package com.tencent.bk.base.datahub.databus.connect.jdbc;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class JdbcAbstractSinkTaskTest {

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
        conn = DriverManager.getConnection(TestConfig.connUrlInit, TestConfig.connUser, TestConfig.connPass);
        stat = conn.createStatement();
        stat.executeUpdate("create database " + TestConfig.dbName);
        stat.close();
        conn.close();

        conn = DriverManager.getConnection(
                TestConfig.connUrl, TestConfig.connUser, TestConfig.connPass);
        stat = conn.createStatement();
        stat.executeUpdate("create table " + TestConfig.tableName
                + "(dtEventTime varchar(80), dtEventTimeStamp timestamp, `localTime` time, id int, thedate date)");
    }

    /**
     * 删除db
     *
     * @throws SQLException
     */
    @AfterClass
    public static void afterClass() throws SQLException {
        stat.executeUpdate("drop database " + TestConfig.dbName);
        stat.close();
        conn.close();
    }

    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutSuccess() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        task.stop();

        ResultSet result = stat.executeQuery("select * from test");
        Assert.assertTrue(result.next());
    }

    /**
     * 覆盖put方法中 records.size() == 0 的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutRecordSizeIsZero() {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);
        task.put(new ArrayList<>());
        task.flush(new HashMap<>());
        task.stop();
    }

    /**
     * 覆盖put方法中 System.currentTimeMillis() - lastCheckTime > 900000 和 msgCountTotal - lastLogCount >= 5000 的分支
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutMoreBranch() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        Map<String, String> basicProps = new HashMap<>();
        basicProps.put(Consts.CONNECTOR_PREFIX + "disable.remove.duplicate", "true");
        BasicProps.getInstance().addProps(basicProps);

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);
        Field field1 = task.getClass().getDeclaredField("lastCheckTime");
        field1.setAccessible(true);
        field1.set(task, System.currentTimeMillis() - 1000000);
        Field field2 = task.getClass().getDeclaredField("lastLogCount");
        field2.setAccessible(true);
        field2.set(task, -5000);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        task.stop();

        ResultSet result = stat.executeQuery("select * from test");
        Assert.assertTrue(result.next());
    }

    /**
     * 测试put方法failed to update stat的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class, Metric.class})
    public void testPutUpdateStatFailed() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);
        PowerMockito.mockStatic(Metric.class);
        PowerMockito.doThrow(new NullPointerException()).when(Metric.class);
        Metric.getInstance();

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);
        Field field = task.getClass().getDeclaredField("removeDupTimes");
        field.setAccessible(true);
        field.set(task, 0);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        task.stop();

        ResultSet result = stat.executeQuery("select * from test");
        Assert.assertTrue(result.next());
    }

    /**
     * 测试start方法中config.cluster.equals("bk_data_jdbc")的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void startClusterEqualsBkDataJDBC() throws Exception {
        Map<String, String> props = getProps();
        props.put(BkConfig.GROUP_ID, "bk_data_jdbc");
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);

        Field field = task.getClass().getDeclaredField("password");
        field.setAccessible(true);
        Assert.assertEquals(TestConfig.connPass, field.get(task));
    }

    /**
     * 测试start方法中rtId.startsWith("622_")的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void startRtIdStartsWith622() throws Exception {
        Map<String, String> props = getProps();
        props.put(BkConfig.RT_ID, "622_xxx");
        props.put(JdbcSinkConfig.CONNECTION_DNS, "xxx");
        props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:mysql://mysql:3306/connect_test?");
        Map<String, String> rtProps = getRtProps();
        rtProps.put(BkConfig.RT_ID, "622_xxx");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("622_xxx")).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);

        Field field = task.getClass().getDeclaredField("password");
        field.setAccessible(true);
        Assert.assertEquals(TestConfig.connPass, field.get(task));
    }

    /**
     * 测试start方法中!tasks.equals("1")的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void startTasksNotEquals1() throws Exception {
        Map<String, String> props = getProps();
        props.put("tasks.max", "0");
        props.put(JdbcSinkConfig.CONNECTION_URL, "xx");
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);

        Field field = task.getClass().getDeclaredField("password");
        field.setAccessible(true);
        Assert.assertEquals(TestConfig.connPass, field.get(task));
    }

    /**
     * 测试start方法中disable.remove.duplicate == true的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void startDisableRemoveDuplicateIsTrue() throws Exception {
        Map<String, String> props = getProps();
        props.put(JdbcSinkConfig.TABLE_NAME, "ai_xxx");
        Map<String, String> rtProps = getRtProps();
        Map<String, String> basicProps = new HashMap<>();
        basicProps.put(Consts.CONNECTOR_PREFIX + "disable.remove.duplicate", "true");
        BasicProps.getInstance().addProps(basicProps);

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);

        Field field = task.getClass().getDeclaredField("password");
        field.setAccessible(true);
        Assert.assertEquals(TestConfig.connPass, field.get(task));
    }

    /**
     * 测试start方法执行失败的情况：no comma allowed in config topics
     */
    @Test(expected = ConfigException.class)
    @PrepareForTest({HttpUtils.class})
    public void testStartFailed() {
        Map<String, String> props = getProps();
        props.put("topics", "topic1,topic2");
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(TestConfig.rtId)).thenReturn(rtProps);

        JdbcAbstractSinkTask task = new JdbcAbstractSinkTask() {
            @Override
            protected String getEscape() {
                return "\"";
            }
        };
        task.start(props);
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.RT_ID, TestConfig.rtId);
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(JdbcSinkConfig.CONNECTION_URL, TestConfig.connUrl);
        props.put(JdbcSinkConfig.CONNECTION_USER, TestConfig.connUser);
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD, TestConfig.connPass);
        props.put(JdbcSinkConfig.TABLE_NAME, TestConfig.tableName);
        props.put("topics", "xxx");
        return props;
    }

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS, "id=int");
        rtProps.put(BkConfig.RT_ID, TestConfig.rtId);
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        return rtProps;
    }

    private String getAvroBinaryString() throws IOException {
        Schema recordSchema =
                new Schema.Parser()
                        .parse("{\"type\":\"record\", \"name\":\"msg\", \"fields\":[{\"name\":\"id\", "
                                + "\"type\":[\"null\",\"int\"]}]}");
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroRecord.put("id", 123);
        GenericArray<GenericRecord> avroArray = new GenericData.Array<>(1, recordArrSchema);
        avroArray.add(avroRecord);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_0")
                .fields()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
        GenericRecord msgRecord = new GenericData.Record(msgSchema);
        msgRecord.put(Consts._VALUE_, avroArray);

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>(msgSchema));
        ByteArrayOutputStream output = new ByteArrayOutputStream(5120);
        dataFileWriter.create(msgSchema, output);
        dataFileWriter.append(msgRecord);
        dataFileWriter.flush();
        String result = output.toString(Consts.ISO_8859_1);
        return result;
    }
}
