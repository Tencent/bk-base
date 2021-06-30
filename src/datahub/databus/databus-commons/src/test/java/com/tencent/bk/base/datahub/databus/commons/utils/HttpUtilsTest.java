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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;

import org.apache.http.HttpHeaders;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;


/**
 * HttpUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/17/2018</pre>
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.crypto.*"})
public class HttpUtilsTest {

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
        System.out.println(mockServer.isRunning());
        mockServer.stop(true);
    }

    @After
    public void after() {
        mockServer.reset();
    }

//    @Rule
//    public MockServerRule server = new MockServerRule(this, 80);


    @Test
    public void testConstructor() {
        HttpUtils httpUtils = new HttpUtils();
        assertNotNull(httpUtils);
    }

    @Test
    public void testGetRtInfoCase0() {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getRtInfo("100_xxx_test");

        System.out.println("xxxxxxxxxx:" + result);
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);
        mockServer.reset();


    }

    @Test
    public void testGetRtInfoCase1() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test_2")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getRtInfo("100_xxx_test_2");
        assertTrue(result.isEmpty());
        mockServer.reset();

    }

    @Test
    public void testGetRtInfoCase2() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getRtInfo("100_xxx_test");
        assertEquals("testname", result.get("name"));
        mockServer.reset();

    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(LogUtils.class)
    public void testGetRtInfoCase4() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(300).withBody(expected));
        Map<String, String> result = HttpUtils.getRtInfo("100_xxx_test");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);
        mockServer.reset();

    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(LogUtils.class)
    public void testGetRtInfoCase5() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(101).withBody(expected));
        Map<String, String> result = HttpUtils.getRtInfo("100_xxx_test");
        mockServer.reset();
    }

    /**
     * Method: getClusterConfig(String clusterName)
     */
    @Test
    public void testGetClusterConfig() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_cluster_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("cluster_name", "default")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getClusterConfig("default");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);
        mockServer.reset();

    }

    /**
     * Method: getDataidsTopics(String dataIds)
     */
    @Test
    public void testGetDataidsTopics() throws Exception {
        //MockServerClient mockServer = new MockServerClient("127.0.0.1", 80);
        String expected =
                "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"topic\":\"test_topic\"},"
                        + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_dataids_topics")
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getDataidsTopics("default");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("topic", "test_topic");
        assertEquals(expectedMap, result);
        mockServer.reset();

    }

    /**
     * Method: getOfflineTaskInfo(String rtId, String type)
     */
    @Test
    public void testGetOfflineTaskInfo() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_offline_task_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test"))
                .withQueryStringParameter(new Parameter("storage_type", "tsdb")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getOfflineTaskInfo("100_xxx_test", "tsdb");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);
        mockServer.reset();

    }

    /**
     * Method: getOfflineTasks(String type)
     */
    @Test
    public void testGetOfflineTasks() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_offline_tasks")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("storage_type", "tsdb")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils.getOfflineTasks("tsdb");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);

    }

    /**
     * Method: updateOfflineTaskInfo(String rtId, String dataDir, String type, String status)
     */
    @Test
    public void testUpdateOfflineTaskInfo() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/update_offline_task_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test"))
                .withQueryStringParameter(new Parameter("data_dir", "/test/dir/"))
                .withQueryStringParameter(new Parameter("storage_type", "tsdb"))
                .withQueryStringParameter(new Parameter("status_update", "test_status")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils
                .updateOfflineTaskInfo("100_xxx_test", "/test/dir/", "tsdb", "test_status");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);

    }

    /**
     * Method: markOfflineTaskFinish(String rtId, String dataDir, String type)
     */
    @Test
    public void testMarkOfflineTaskFinish() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/finish_offline_task")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test"))
                .withQueryStringParameter(new Parameter("data_dir", "/test/dir/"))
                .withQueryStringParameter(new Parameter("storage_type", "tsdb")))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, String> result = HttpUtils
                .markOfflineTaskFinish("100_xxx_test", "/test/dir/", "tsdb");
        Map<String, String> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");
        assertEquals(expectedMap, result);

    }

    /**
     * Method: addDatabusStorageEvent(String rtId, String storage, String eventType, String
     * eventValue)
     */
    @Test
    public void testAddDatabusStorageEventCase0() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/add_databus_storage_event")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"event_type\":\"tdwFinishDir\",\"result_table_id\":\"100_xxx_test\",\"storage\":\"tsdb\","
                                + "\"event_value\":\"20180330\"}"))
                .respond(response().withStatusCode(200).withBody(expected));

        boolean result = HttpUtils
                .addDatabusStorageEvent("100_xxx_test", "tsdb", "tdwFinishDir", "20180330");
        assertTrue(result);
        mockServer.reset();

    }

    /**
     * Method: addDatabusStorageEvent(String rtId, String storage, String eventType, String
     * eventValue)
     */
    @Test
    public void testAddDatabusStorageEventCase1() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/add_databus_storage_event")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"event_type\":\"tdwFinishDir\",\"result_table_id\":\"100_xxx_test\",\"storage\":\"tsdb\","
                                + "\"event_value\":\"20180330\"}"))
                .respond(response().withStatusCode(200).withBody(expected));

        boolean result = HttpUtils
                .addDatabusStorageEvent("100_xxx_test", "tsdb", "tdwFinishDir", "20180330");
        assertTrue(result);
        mockServer.reset();

    }

    /**
     * Method: addDatabusStorageEvent(String rtId, String storage, String eventType, String
     * eventValue)
     */
    @Test
    public void testAddDatabusStorageEventCase2() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":false}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/add_databus_storage_event")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"event_type\":\"tdwFinishDir\",\"result_table_id\":\"100_xxx_test\",\"storage\":\"tsdb\","
                                + "\"event_value\":\"20180330\"}"))
                .respond(response().withStatusCode(200).withBody(expected));

        boolean result = HttpUtils
                .addDatabusStorageEvent("100_xxx_test", "tsdb", "tdwFinishDir", "20180330");
        assertFalse(result);
        mockServer.reset();

    }

    /**
     * Method: addDatabusStorageEvent(String rtId, String storage, String eventType, String
     * eventValue)
     */


    /**
     * Method: addDatabusStorageEvent(String rtId, String storage, String eventType, String
     * eventValue)
     */
    @Test
    @PrepareForTest(LogUtils.class)
    public void testAddDatabusStorageEventCase4() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/add_databus_storage_event")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"event_type\":\"tdwFinishDir\",\"rt_id\":\"100_xxx_test\",\"storage\":\"tsdb\","
                                + "\"event_value\":\"20180330\"}"))
                .respond(response().withStatusCode(200).withBody(expected));
        PowerMockito.whenNew(DataOutputStream.class).withAnyArguments().thenThrow(IOException.class);
        HttpUtils
                .addDatabusStorageEvent("100_xxx_test", "tsdb", "tdwFinishDir", "20180330");
        //FIXME add expected

    }

    /**
     * Method: getHdfsImportTasks()
     */
    @Test
    public void testGetHdfsImportTasksCase0() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":[{\"name\":\"testname\"}],"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_earliest_hdfs_import_task")
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");

        List<Map<String, Object>> expectedList = Lists.newArrayList(expectedMap);
        List<Map<String, Object>> result = HttpUtils.getHdfsImportTasks();
        assertEquals(expectedList, result);

    }

    /**
     * Method: getHdfsImportTasks()
     */
    @Test(expected = java.lang.LinkageError.class)
    public void testGetHdfsImportTasksCase1() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":[{\"name\":\"testname\"}],"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_earliest_hdfs_import_task")
                .withMethod("POST"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");

        List<Map<String, Object>> expectedList = Lists.newArrayList(expectedMap);
        List<Map<String, Object>> result = HttpUtils.getHdfsImportTasks();
        assertEquals(expectedList, result);

    }

    /**
     * Method: getHdfsImportTasks()
     */
    @Test
    public void testGetHdfsImportTasksCase2() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":[{\"name\":\"testname\"}],"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_earliest_hdfs_import_task")
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");

        List<Map<String, Object>> expectedList = Lists.newArrayList(expectedMap);
        List<Map<String, Object>> result = HttpUtils.getHdfsImportTasks();
        assertEquals(expectedList, result);

    }

    /**
     * Method: getHdfsImportTasks()
     */
    @Test
    public void testGetHdfsImportTasksCase3() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":[{\"name\":\"testname\"}],"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_earliest_hdfs_import_task")
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");

        List<Map<String, Object>> expectedList = Lists.newArrayList(expectedMap);
        List<Map<String, Object>> result = HttpUtils.getHdfsImportTasks();
        assertEquals(expectedList, result);

    }

    /**
     * Method: getHdfsImportTasks()
     */
    @Test
    public void testGetHdfsImportTasksCase4() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":[{\"name\":\"testname\"}],"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/get_earliest_hdfs_import_task")
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(expected));
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("name", "testname");

        List<Map<String, Object>> expectedList = Lists.newArrayList(expectedMap);
        List<Map<String, Object>> result = HttpUtils.getHdfsImportTasks();
        assertEquals(expectedList, result);

    }

    /**
     * Method: updateHdfsImportTaskStatus(long id, String rtId, String dataDir, String status, boolean
     * finished)
     */
    @Test
    public void testUpdateHdfsImportTaskStatusCase0() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/update_hdfs_import_task")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"status_update\":\"tdwFinishDir\",\"rt_id\":\"100_xxx_test\",\"finish\":\"true\","
                                + "\"id\":100,\"data_dir\":\"tsdb\"}"))
                .respond(response().withStatusCode(200).withBody(expected));

        boolean result = HttpUtils
                .updateHdfsImportTaskStatus(100, "100_xxx_test", "tsdb", "tdwFinishDir", true);
        assertTrue(result);

    }

    /**
     * Method: updateHdfsImportTaskStatus(long id, String rtId, String dataDir, String status, boolean
     * finished)
     */
    @Test
    public void testUpdateHdfsImportTaskStatusCase1() throws Exception {
        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"name\":\"testname\"},"
                + "\"result\":true}\n";
        mockServer.when(request()
                .withPath("/databus/shipper/update_hdfs_import_task")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON))
                .withBody(
                        "{\"status_update\":\"tdwFinishDir\",\"rt_id\":\"100_xxx_test\",\"finish\":\"false\","
                                + "\"id\":100,\"data_dir\":\"tsdb\"}"))
                .respond(response().withStatusCode(200).withBody(expected));

        boolean result = HttpUtils
                .updateHdfsImportTaskStatus(100, "100_xxx_test", "tsdb", "tdwFinishDir", false);
        assertTrue(result);


    }

    /**
     * Method: getKafkaHosts(String kafkaBsServers)
     */
    @Test
    @PrepareForTest(BasicProps.class)
    public void testGetKafkaHostsCase0() throws Exception {
        PowerMockito.mockStatic(BasicProps.class);
        BasicProps mockBasic = PowerMockito.mock(BasicProps.class);
        PowerMockito.when(mockBasic.getConsulServiceUrl()).thenReturn("http://127.0.0.1:9999/get_service_status");
        PowerMockito.when(BasicProps.getInstance()).thenReturn(mockBasic);

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":[{\"IP\":\"test_ip\"}],\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/get_service_status")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("service_name", "test_kafka"))
                .withQueryStringParameter(new Parameter("idc", "sh")))
                .respond(response().withStatusCode(200).withBody(expected));

        List<String> expectedList = Lists.newArrayList("test_ip");
        List<String> result = HttpUtils.getKafkaHosts("test_kafka.xx.sh");
        assertEquals(expectedList, result);

    }

    /**
     * Method: getKafkaHosts(String kafkaBsServers)
     */
    @Test
    @PrepareForTest(BasicProps.class)
    public void testGetKafkaHostsCase1() throws Exception {
        BasicProps mockProps = PowerMockito.mock(BasicProps.class);
        PowerMockito.when(mockProps.getConsulServiceUrl()).thenReturn("http://127.0.0.1:9999/get_service_status");
        PowerMockito.mockStatic(BasicProps.class);
        PowerMockito.when(BasicProps.getInstance()).thenReturn(mockProps);

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":[{\"IP\":\"test_ip\"}],\n"
                + "        \"result\":\"false\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/get_service_status")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("service_name", "test_kafka"))
                .withQueryStringParameter(new Parameter("idc", "sh")))
                .respond(response().withStatusCode(200).withBody(expected));
        List<String> expectedList = Lists.newArrayList();
        List<String> result = HttpUtils.getKafkaHosts("test_kafka.xx.sh");
        assertEquals(expectedList, result);

    }

    /**
     * Method: getKafkaHosts(String kafkaBsServers)
     */
    @Test
    @PrepareForTest(BasicProps.class)
    public void testGetKafkaHostsCase2() throws Exception {
        BasicProps mockProps = PowerMockito.mock(BasicProps.class);
        PowerMockito.when(mockProps.getConsulServiceUrl()).thenReturn("http://127.0.0.1:9999/get_service_status");
        PowerMockito.mockStatic(BasicProps.class);
        PowerMockito.when(BasicProps.getInstance()).thenReturn(mockProps);

        //MockServerClient mockClient = new MockServerClient("127.0.0.1", 80);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":[{\"IP\":\"test_ip\"}],\n"
                + "        \"result\":\"false\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/get_service_status")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("service_name", "test_kafka"))
                .withQueryStringParameter(new Parameter("idc", "sh")))
                .respond(response().withStatusCode(200).withBody(expected));
        List<String> expectedList = Lists.newArrayList();
        List<String> result = HttpUtils.getKafkaHosts("test_kafka.xx.xh");
        assertEquals(expectedList, result);

    }


}
