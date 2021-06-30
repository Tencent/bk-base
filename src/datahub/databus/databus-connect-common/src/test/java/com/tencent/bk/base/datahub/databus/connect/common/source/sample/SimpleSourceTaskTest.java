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

package com.tencent.bk.base.datahub.databus.connect.common.source.sample;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.JsonConverter;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SimpleSourceTaskTest {

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void beforeClass() {
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> propsConfig = Maps.newHashMap();
        propsConfig.put(Consts.API_DNS, "127.0.0.1:9999");
        basicProps.addProps(propsConfig);
        mockServer = startClientAndServer(9999);
    }

    @AfterClass
    public static void afterClass() {
        mockServer.stop(true);
    }

    @After
    public void after() {
        mockServer.reset();
    }


//    @Rule
//    public MockServerRule server = new MockServerRule(this, 80);

    @Test
    public void startCase0() throws InterruptedException {
        SimpleSourceTask task = new SimpleSourceTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());

        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
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

    @Test
    public void startCase1() throws InterruptedException {
        SimpleSourceTask task = new SimpleSourceTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(null);

        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
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
        SimpleSourceTask task = new SimpleSourceTask();
        Map map = Maps.newHashMap();
        task.start(map);
        assertNotNull(task);
    }

    @Test(expected = NullPointerException.class)
    public void startCase3() {
        SimpleSourceTask task = new SimpleSourceTask();
        task.start(null);
    }

    @Test
    @PrepareForTest(SimpleSourceTask.class)
    public void pollCase0() throws Exception {
        SimpleSourceTask task = new SimpleSourceTask();
        Field f = task.getClass().getDeclaredField("config");
        f.setAccessible(true);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        SimpleSourceConfig config = new SimpleSourceConfig(map);
        f.set(task, config);

        ConvertResult mockRs = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockRs.getObjListResult()).thenReturn(Lists.newArrayList());

        JsonConverter mockConverter = PowerMockito.mock(JsonConverter.class);
        PowerMockito.when(mockConverter
                .getListObjects(Matchers.anyString(), Matchers.anyString(), Matchers.any()))
                .thenReturn(mockRs);

        Whitebox.setInternalState(task, "converter", mockConverter);
        Whitebox.setInternalState(task, "offset", Maps.newHashMap());

        FileInputStream mockFis = PowerMockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockFis);

        BufferedReader mockReader = PowerMockito.mock(BufferedReader.class);
        PowerMockito.when(mockReader.readLine()).thenReturn("test msg").thenReturn(null);
        PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(mockReader);
        List<SourceRecord> results = task.poll();
        assertEquals(1, results.size());
    }

    @Test
    @PrepareForTest(SimpleSourceTask.class)
    public void pollCase1() throws Exception {

        SimpleSourceTask task = new SimpleSourceTask();
        task.stop();
        Field f = task.getClass().getDeclaredField("config");
        f.setAccessible(true);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        SimpleSourceConfig config = new SimpleSourceConfig(map);
        f.set(task, config);

        ConvertResult mockRs = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockRs.getObjListResult()).thenReturn(Lists.newArrayList());

        JsonConverter mockConverter = PowerMockito.mock(JsonConverter.class);
        PowerMockito.when(mockConverter
                .getListObjects(Matchers.anyString(), Matchers.anyString(), Matchers.any()))
                .thenReturn(mockRs);

        Whitebox.setInternalState(task, "converter", mockConverter);
        Whitebox.setInternalState(task, "offset", Maps.newHashMap());

        FileInputStream mockFis = PowerMockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockFis);

        BufferedReader mockReader = PowerMockito.mock(BufferedReader.class);
        PowerMockito.when(mockReader.readLine()).thenReturn("test msg").thenReturn(null);
        PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(mockReader);
        List<SourceRecord> results = task.poll();
        assertNull(results);
    }

    @Test
    @PrepareForTest(SimpleSourceTask.class)
    public void pollCase2() throws Exception {
        SimpleSourceTask task = new SimpleSourceTask();
        Field f = task.getClass().getDeclaredField("config");
        f.setAccessible(true);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        SimpleSourceConfig config = new SimpleSourceConfig(map);
        f.set(task, config);

        ConvertResult mockRs = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockRs.getObjListResult()).thenReturn(Lists.newArrayList());

        JsonConverter mockConverter = PowerMockito.mock(JsonConverter.class);
        PowerMockito.when(mockConverter
                .getListObjects(Matchers.anyString(), Matchers.anyString(), Matchers.any()))
                .thenReturn(mockRs);


        Whitebox.setInternalState(task, "converter", mockConverter);
        Whitebox.setInternalState(task, "offset", Maps.newHashMap());

        FileInputStream mockFis = PowerMockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockFis);

        BufferedReader mockReader = PowerMockito.mock(BufferedReader.class);
        PowerMockito.when(mockReader.readLine()).thenReturn("test msg").thenReturn(null);
        PowerMockito.doThrow(new IOException()).when(mockReader, "close");

        PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(mockReader);
        List<SourceRecord> results = task.poll();
        assertEquals(1, results.size());

    }

    @Test
    @PrepareForTest(SimpleSourceTask.class)
    public void pollCase3() throws Exception {
        SimpleSourceTask task = new SimpleSourceTask();
        Field f = task.getClass().getDeclaredField("config");
        f.setAccessible(true);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        SimpleSourceConfig config = new SimpleSourceConfig(map);
        f.set(task, config);

        ConvertResult mockRs = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockRs.getObjListResult()).thenReturn(Lists.newArrayList());

        JsonConverter mockConverter = PowerMockito.mock(JsonConverter.class);
        PowerMockito.when(mockConverter
                .getListObjects(Matchers.anyString(), Matchers.anyString(), Matchers.any()))
                .thenReturn(mockRs);


        Whitebox.setInternalState(task, "converter", mockConverter);
        Whitebox.setInternalState(task, "offset", Maps.newHashMap());

//        FileInputStream mockFis = PowerMockito.mock(FileInputStream.class);
//        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockFis);
//
//        BufferedReader mockReader = PowerMockito.mock(BufferedReader.class);
//        PowerMockito.when(mockReader.readLine()).thenReturn("test msg").thenReturn(null);
//        PowerMockito.doThrow(new IOException()).when(mockReader, "close");
//
//        PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(mockReader);
        List<SourceRecord> results = task.poll();
        assertEquals(Lists.newArrayList(), results);

    }

    @Test
    public void stopCase0() {
        SimpleSourceTask task = new SimpleSourceTask();
        task.stop();
    }

    @Test
    public void stopCase1() {
        SimpleSourceTask task = new SimpleSourceTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());

        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "connect_name");
        map.put(BkConfig.RT_ID, "100_xxx_test");
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        task.initialize(mockContext);
        task.start(map);
        task.stop();
    }

    @Test
    public void testConstructorCase0() {
        SimpleSourceTask task = new SimpleSourceTask();
        assertNotNull(task);
    }

    @Test
    public void testMarkBkTaskCtxChangedCase0() {
        SimpleSourceTask task = new SimpleSourceTask();
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        SourceTaskContext mockContext = PowerMockito.mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = PowerMockito.mock(OffsetStorageReader.class);
        PowerMockito.when(mockReader.offset(Matchers.anyMap())).thenReturn(Maps.newHashMap());

        PowerMockito.when(mockContext.offsetStorageReader()).thenReturn(mockReader);
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
        SimpleSourceTask task = new SimpleSourceTask();
        task.markBkTaskCtxChanged();
        assertEquals("", task.version());
        assertNotNull(task);
    }

}