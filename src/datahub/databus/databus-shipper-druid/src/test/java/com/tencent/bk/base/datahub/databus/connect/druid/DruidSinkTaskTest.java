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

package com.tencent.bk.base.datahub.databus.connect.druid;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.tencent.bk.base.datahub.databus.connect.druid.transport.TaskWriter;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
public class DruidSinkTaskTest {

    private static final String rtId = "591_test";
    private static final String body = "[{\"__druid_reserved_ingestion_time\":\"${json-unit.any-number}\","
            + "\"dtEventTimeStamp\":1620357466471,\"__druid_reserved_hour\":\"2021-05-07 11:00:00\","
            + "\"__druid_reserved_timestamp_millisecond\":1620357466471,\"gseindex\":123,\"ip_v6\":\"xx.xx.xx.xx\","
            + "\"ip\":\"xx.xx.xx.xx\",\"__druid_reserved_minute\":\"2021-05-07 11:17:00\","
            + "\"__druid_reserved_second\":\"2021-05-07 11:17:46\",\"__druid_reserved_millisecond\":\"2021-05-07 "
            + "11:17:46\",\"ip_r\":\"xx.xx.xx.xx\",\"__druid_reserved_timestamp_day\":1620316800000,"
            + "\"__localtime\":null,\"path\":\"xx/xx/xx/xx\",\"__druid_reserved_timestamp_hour\":1620356400000,"
            + "\"__druid_reserved_timestamp_minute\":1620357420000,"
            + "\"__druid_reserved_timestamp_second\":1620357466000,\"thedate\":20190111,"
            + "\"__druid_reserved_day\":\"2021-05-07 00:00:00\",\"dtEventTime\":null,\"time\":\"yyyy-MM-dd "
            + "HH:mm:ss\"}]";
    private Map<String, String> columns = new HashMap<>();
    private ClientAndServer mockServer;

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, "druid-table_591_test");
        props.put(BkConfig.RT_ID, "591_test");
        props.put(BkConfig.GROUP_ID, "xxxxx");
        props.put(DruidSinkConfig.TABLE_NAME, "591_test");
        props.put(DruidSinkConfig.ZOOKEEPER_CONNECT, "zookeeper-druid:2181");
        props.put(DruidSinkConfig.DRUID_VERSION, "0.16");
        props.put(DruidSinkConfig.TASK_MAX, "1");
        props.put(DruidSinkConfig.TIMESTAMP_STRATEGY, "data_time");
        return props;
    }

    /**
     * 创建db和table
     *
     * @throws SQLException
     */
    @Before
    public void setup() throws Exception {
        //gseindex=long,ip=string,ip_r=string,ip_v6=string,path=string,time=string
        columns.put("gseindex", "long");
        columns.put("ip", "string");
        columns.put("ip_r", "string");
        columns.put("ip_v6", "string");
        columns.put("path", "string");
        columns.put("time", "string");
        columns.put("dtEventTimeStamp", "long");

        BasicProps.getInstance().addProps(Collections.singletonMap(Consts.API_DNS, "api.test.xxxx"));
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        mockServer = startClientAndServer(8123);

        //"http://" + children.get(0) + "/druid/indexer/v1/leader"
        mockServer.when(request()
                .withPath("/druid/indexer/v1/leader")
                .withMethod("GET")).respond(response().withStatusCode(200).withBody("http://localhost:8123"));

        mockServer.when(request()
                .withPath("/druid/indexer/v1/task/123/status")
                .withMethod("GET")).respond(response().withStatusCode(200).withBody(
                "{\"status\":{\"runnerStatusCode\":\"RUNNING\",\"status\":\"RUNNING\","
                        + "\"location\":{\"host\":\"localhost\",\"port\":8123}}}"));

        mockServer.when(request()
                .withPath("/druid/indexer/v1/task")
                .withMethod("POST"))
                .respond(response().withStatusCode(200).withBody("{\"task\":\"123\"}"));

        mockServer.when(request()
                .withPath("/druid/worker/v1/chat/{serviceName}/push-events")
                .withPathParameters(new Parameter("serviceName"))
                .withMethod("POST")
                .withBody(new JsonBody(body, MatchType.ONLY_MATCHING_FIELDS)))
                .respond(response().withStatusCode(200).withBody("{\"eventCount\":1}"));
    }


    @Test
    @PrepareForTest({HttpUtils.class, CuratorFrameworkFactory.class})
    public void testStartTask() throws Exception {
        Map<String, String> props = getProps();
        commonMock(props);
        DruidSinkTask task = new DruidSinkTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        DruidSinkConfig config = (DruidSinkConfig) configField.get(task);
        Assert.assertEquals(props.get(DruidSinkConfig.TABLE_NAME), config.tableName);
        Assert.assertEquals(props.get(DruidSinkConfig.ZOOKEEPER_CONNECT), config.zookeeperConnect);
        Assert.assertEquals(props.get(DruidSinkConfig.DRUID_VERSION), config.druidVersion);
        Assert.assertEquals(props.get(DruidSinkConfig.TASK_MAX), String.valueOf(config.taskMax));
        Assert.assertEquals(props.get(DruidSinkConfig.TIMESTAMP_STRATEGY), config.timestampStrategy);

        Field colsInOrderField = task.getClass().getDeclaredField("colsInOrder");
        colsInOrderField.setAccessible(true);
        String[] colsInOrder = (String[]) colsInOrderField.get(task);
        Assert.assertEquals("path", colsInOrder[0]);
        Assert.assertEquals("dtEventTimeStamp", colsInOrder[1]);
        Assert.assertEquals("localTime", colsInOrder[2]);
        Assert.assertEquals("gseindex", colsInOrder[3]);

        Field taskWriterField = task.getClass().getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        TaskWriter taskWriter = (TaskWriter) taskWriterField.get(task);
        Assert.assertNotNull(taskWriter);


    }

    @Test
    @PrepareForTest({HttpUtils.class, CuratorFrameworkFactory.class})
    public void testProcessData() throws Exception {
        Map<String, String> props = getProps();
        commonMock(props);
        DruidSinkTask task = new DruidSinkTask();
        SinkTaskContext context = mock(SinkTaskContext.class);
        task.initialize(context);
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        Collection<SinkRecord> records = new ArrayList<>(1);
        records.add(sinkRecord1);
        task.put(records);
        log.info("executed write");
        // sleep to wait backend flush complete
        Thread.sleep(1000);
        task.stop();

    }

    private void commonMock(Map<String, String> props) throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        PowerMockito.mockStatic(CuratorFrameworkFactory.class);
        PowerMockito.when(CuratorFrameworkFactory.newClient(anyString(), anyObject())).thenReturn(curator);
        PowerMockito.doNothing().when(curator).start();
        when(curator.blockUntilConnected(30, TimeUnit.SECONDS)).thenReturn(true);
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(curator.getChildren()).thenReturn(childrenBuilder);
        when(childrenBuilder.forPath("/druid/internal-discovery/OVERLORD"))
                .thenReturn(Collections.singletonList("localhost:8123"));
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(getRtProps());
    }

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS,
                "gseindex=long,ip=string,ip_r=string,ip_v6=string,path=string,time=string");
        rtProps.put(BkConfig.RT_ID, rtId);
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        return rtProps;
    }

    private String getAvroBinaryString() throws IOException {
        Schema recordSchema = AvroUtils.getRecordSchema(columns);
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroRecord.put("gseindex", 123);
        avroRecord.put("ip", "xx.xx.xx.xx");
        avroRecord.put("ip_r", "xx.xx.xx.xx");
        avroRecord.put("ip_v6", "xx.xx.xx.xx");
        avroRecord.put("path", "xx/xx/xx/xx");
        avroRecord.put("time", "yyyy-MM-dd HH:mm:ss");
        avroRecord.put("dtEventTimeStamp", 1620357466471L);

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

    @After
    public void tearDown() throws Exception {
        mockServer.stop(true);
    }

}
