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

package com.tencent.bk.base.datahub.databus.connect.common.sink.sample;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.JsonConverter;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SimpleSinkTaskTest {
    private static ClientAndServer mockServer;

    @BeforeClass
    public static void beforeClass() {
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> propsConfig = Maps.newHashMap();
        propsConfig.put(Consts.API_DNS, "127.0.0.1:9999");
        basicProps.addProps(propsConfig);
        mockServer = startClientAndServer(9999);
    }

//    @Rule
//    public MockServerRule server = new MockServerRule(this, 80);

    @AfterClass
    public static void afterClass() {
        mockServer.stop(true);
    }

    @After
    public void after() {
        mockServer.reset();
    }

    @Test
    public void startCase0() {
        SimpleSinkTask task = new SimpleSinkTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        assertNotNull(task);
    }

    @Test(expected = NullPointerException.class)
    public void startCase2() {
        SimpleSinkTask task = new SimpleSinkTask();
        Map map = Maps.newHashMap();
        task.start(map);
        assertNotNull(task);
    }

    @Test(expected = NullPointerException.class)
    public void startCase3() {
        SimpleSinkTask task = new SimpleSinkTask();
        task.start(null);
    }

    @Test
    public void putCase0() {
        SimpleSinkTask task = new SimpleSinkTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        Collection<SinkRecord> records = Lists.newArrayList();
        SinkRecord sinkRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(sinkRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(sinkRecord.kafkaOffset()).thenReturn(1l);
        PowerMockito.when(sinkRecord.key()).thenReturn("key");
        PowerMockito.when(sinkRecord.value()).thenReturn("value");
        records.add(sinkRecord);


        ConvertResult mockResult = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockResult.getMsgSize()).thenReturn(1);
        JsonConverter mockConvert = PowerMockito.mock(JsonConverter.class);
        PowerMockito.when(mockConvert.getJsonList(Matchers.anyString(), Matchers.anyString()))
                .thenReturn(mockResult);

        Whitebox.setInternalState(task, "converter", mockConvert);

        task.put(records);
    }


    @Test
    public void putCase1() {
        SimpleSinkTask task = new SimpleSinkTask();
        task.put(Lists.newArrayList());
    }

    @Test
    public void flush() {
        SimpleSinkTask task = new SimpleSinkTask();
        task.flush(null);
        assertNotNull(task);
    }

    @Test
    public void stop() {
        SimpleSinkTask task = new SimpleSinkTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"127.0.0.1:9092\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        task.stop();
        assertNotNull(task);
    }

    @Test
    public void testConstructor() {
        SimpleSinkTask obj = new SimpleSinkTask();
        assertNotNull(obj);
    }

    @Test
    public void testMarkBkTaskCtxChangedCase0() {
        SimpleSinkTask task = new SimpleSinkTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        task.markBkTaskCtxChanged();
        assertNotNull(task);
    }

    @Test
    public void testMarkBkTaskCtxChangedCase1() {
        SimpleSinkTask task = new SimpleSinkTask();
        task.markBkTaskCtxChanged();
        Assert.assertEquals("", task.version());
        assertNotNull(task);
    }

    @Test
    public void testRefreshTaskContextCase0() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "true");
        assertTrue(BasicProps.getInstance().getDisableContextRefresh());
        assertFalse(task.needRefreshTaskContext());
    }

    @Test
    public void testRefreshTaskContextCase1() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");

        task.initialize(mockContext);
        task.start(map);

        assertFalse(task.needRefreshTaskContext());
    }

    @Test
    public void testRefreshTaskContextCase2() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        mockServer.reset();
        //mockClient = new MockServerClient("127.0.0.1", 80);
        String expectedRefresh = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string,k3=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expectedRefresh));
        assertTrue(task.needRefreshTaskContext());
    }

    @Test
    public void testRefreshTaskContextCase3() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");

        task.initialize(mockContext);
        task.start(map);

        mockServer.reset();

        //mockClient = new MockServerClient("127.0.0.1", 80);
        String expectedRefresh = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string,k3=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expectedRefresh));


        assertTrue(task.needRefreshTaskContext());
    }

    @Test
    public void testRefreshTaskContextCase4() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");

        task.initialize(mockContext);
        task.start(map);

        mockServer.reset();

        //mockClient = new MockServerClient("127.0.0.1", 80);
        String expectedRefresh = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expectedRefresh));


        assertFalse(task.needRefreshTaskContext());
    }

    @Test
    public void testRefreshTaskContextCase5() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");

        task.initialize(mockContext);
        task.start(map);

        mockServer.reset();

        //mockClient = new MockServerClient("127.0.0.1", 80);
        String expectedRefresh = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"topic\":\"t_topic\",\n"
                + "                 \"rt.id\":\"100_xxx_test\",\n"
                + "                 \"etl.conf\":\"etl_config\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expectedRefresh));

        assertFalse(task.needRefreshTaskContext());
    }

    @Test
    @PrepareForTest(HttpUtils.class)
    public void testRefreshTaskContextCase6() throws Exception {
        SimpleSinkTask task = new SimpleSinkTask();
        BasicProps basicProps = BasicProps.getInstance();
        Field configField = basicProps.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        Map<String, String> config = (Map<String, String>) configField.get(basicProps);
        config.put(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false");
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(Matchers.anyString())).thenReturn(null);
        assertFalse(task.needRefreshTaskContext());


    }


    @Test
    public void testResetDelayTimeCounter() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "resetDelayTimeCounter");
        assertNotNull(simpleSinkTask);
    }


    @Test
    public void testMarkRecordProcessed() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(100l);
        Whitebox.invokeMethod(simpleSinkTask, "markRecordProcessed", mockRecord);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testLogRepeatRecordsCase0() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "logRepeatRecords");
    }

    @Test
    public void testLogRepeatRecordsCase1() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Set<String> oldPartitionOffsets = new LinkedHashSet<>(16);
        oldPartitionOffsets.add("1,100");
        Whitebox.setInternalState(simpleSinkTask, "oldPartitionOffsets", oldPartitionOffsets);
        Whitebox.invokeMethod(simpleSinkTask, "logRepeatRecords");
    }


    @Test
    public void testSetDelayTimeCase0() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 14600000000l, 14500000000l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase1() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 0l, 14500000000l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase2() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 0l, -1l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase3() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 14500000000l, 14600000000l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase4() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 95l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 80l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase5() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 80l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 80l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase6() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 90l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 80l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase7() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 90l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 80l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase8() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 90l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 90l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testSetDelayTimeCase9() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        Whitebox.setInternalState(simpleSinkTask, "maxDelayTime", 90l);
        Whitebox.setInternalState(simpleSinkTask, "minDelayTime", 100l);
        Whitebox.invokeMethod(simpleSinkTask, "setDelayTime", 90l, 100l);
        assertNotNull(simpleSinkTask);
    }

    @Test
    public void testIsRecordProcessedCase0() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(10l);
        boolean result = Whitebox.invokeMethod(simpleSinkTask, "isRecordProcessed", mockRecord);
        assertFalse(result);
    }

    @Test
    public void testIsRecordProcessedCase1() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(10l);

        Map<Integer, Long> processedOffsets = Maps.newHashMap();
        processedOffsets.put(1, 9l);

        Whitebox.setInternalState(simpleSinkTask, "processedOffsets", processedOffsets);

        boolean result = Whitebox.invokeMethod(simpleSinkTask, "isRecordProcessed", mockRecord);
        assertFalse(result);
    }

    @Test
    public void testIsRecordProcessedCase2() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(10l);

        Map<Integer, Long> processedOffsets = Maps.newHashMap();
        processedOffsets.put(2, 9l);

        Whitebox.setInternalState(simpleSinkTask, "processedOffsets", processedOffsets);

        boolean result = Whitebox.invokeMethod(simpleSinkTask, "isRecordProcessed", mockRecord);
        assertFalse(result);
    }

    @Test
    public void testIsRecordProcessedCase3() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(10l);

        Map<Integer, Long> processedOffsets = Maps.newHashMap();
        processedOffsets.put(1, 10l);

        Whitebox.setInternalState(simpleSinkTask, "processedOffsets", processedOffsets);

        boolean result = Whitebox.invokeMethod(simpleSinkTask, "isRecordProcessed", mockRecord);
        assertTrue(result);
    }

    @Test
    public void testIsRecordProcessedCase4() throws Exception {
        SimpleSinkTask simpleSinkTask = new SimpleSinkTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(10l);

        Set<String> mockSet = PowerMockito.mock(LinkedHashSet.class);
        PowerMockito.when(mockSet.size()).thenReturn(1000);
        Whitebox.setInternalState(simpleSinkTask, "oldPartitionOffsets", mockSet);


        Map<Integer, Long> processedOffsets = Maps.newHashMap();
        processedOffsets.put(1, 10l);

        Whitebox.setInternalState(simpleSinkTask, "processedOffsets", processedOffsets);

        boolean result = Whitebox.invokeMethod(simpleSinkTask, "isRecordProcessed", mockRecord);
        assertTrue(result);
    }



}