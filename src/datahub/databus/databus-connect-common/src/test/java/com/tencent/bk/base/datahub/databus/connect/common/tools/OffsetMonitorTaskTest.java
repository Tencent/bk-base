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

package com.tencent.bk.base.datahub.databus.connect.common.tools;

import static org.junit.Assert.assertNotNull;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.monitor.ConsumerOffsetStat;
import com.tencent.bk.base.datahub.databus.commons.utils.InfluxdbUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JmxUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.naming.*", "javax.security.*"})
public class OffsetMonitorTaskTest {

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void beforeClass() {
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> propsConfig = Maps.newHashMap();
        propsConfig.put(Consts.API_DNS, "127.0.0.1:9999");
        basicProps.addProps(propsConfig);
        mockServer = ClientAndServer.startClientAndServer(9999);
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
    @PrepareForTest(InfluxdbUtils.class)
    public void testConstructorCase0() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
        OffsetMonitorTask obj = new OffsetMonitorTask();
        //Thread.sleep(3000);
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testConstructorCase1() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "0");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
        OffsetMonitorTask obj = new OffsetMonitorTask();
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testConstructorCase2() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "abc");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
        OffsetMonitorTask obj = new OffsetMonitorTask();
        assertNotNull(obj);
    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testConstructorCase3() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
        Whitebox.setInternalState(OffsetMonitorTask.class, "LOOP", -1);
        OffsetMonitorTask obj = new OffsetMonitorTask();
        assertNotNull(obj);
        //Thread.sleep(3000);
    }

//    @Test
//    @PrepareForTest(StringUtils.class)
//    public void testConstructorCase4() throws Exception {
//        PowerMockito.mockStatic(StringUtils.class);
//        PowerMockito.when(StringUtils.isNoneBlank("tsdb.url")).thenThrow(Exception.class);
//        BasicProps basicProps = BasicProps.getInstance();
//        Map<String, String> clusterProps = Maps.newHashMap();
//        clusterProps.put("tsdb.url", "tsdb.url");
//        clusterProps.put("tsdb.user", "tsdb.user");
//        clusterProps.put("tsdb.dbname", "tsdb.dbname");
//        clusterProps.put("tsdb.batch.size", "100");
//        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
//        Whitebox.setInternalState(OffsetMonitorTask.class, "LOOP", -1);
//        OffsetMonitorTask obj = new OffsetMonitorTask();
//        assertNull(obj);
//        Thread.sleep(31000);
//    }

//    @Test
//    public void testConstructorCase5() throws Exception {
//        BasicProps basicProps = BasicProps.getInstance();
//        Map<String, String> clusterProps = Maps.newHashMap();
//        clusterProps.put("tsdb.url", "");
//        clusterProps.put("tsdb.user", "tsdb.user");
//        clusterProps.put("tsdb.dbname", "tsdb.dbname");
//        clusterProps.put("tsdb.batch.size", "100");
//        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);
//        Whitebox.setInternalState(OffsetMonitorTask.class, "LOOP", -1);
//        OffsetMonitorTask obj = new OffsetMonitorTask();
//        assertNotNull(obj);
//        Thread.sleep(31000);
//    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testStart0() throws Exception {
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

        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");

        obj.start(props);

        assertNotNull(obj);


    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testStart1() throws Exception {
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

        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "false");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(props);
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testStart2() throws Exception {
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

        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        clusterProps.put(Consts.SASL_USER, "sasl_user");
        clusterProps.put(Consts.SASL_PASS, "sasl_pass");
        clusterProps.put(Consts.INSTANCE_KEY, "instance_key");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "false");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(props);
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testStart3() throws Exception {
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

        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put("tsdb.url", "tsdb.url");
        clusterProps.put("tsdb.user", "tsdb.user");
        clusterProps.put("tsdb.dbname", "tsdb.dbname");
        clusterProps.put("tsdb.batch.size", "100");
        clusterProps.put(Consts.SASL_USER, "sasl_user");
        clusterProps.put(Consts.SASL_PASS, "sasl_pass");
        clusterProps.put(Consts.INSTANCE_KEY, "instance_key");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "false");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(props);
        assertNotNull(obj);

    }


    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testInitConsumerCase0() throws Exception {
        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put(Consts.SASL_USER, "sasl_user");
        clusterProps.put(Consts.SASL_PASS, "xxx");
        clusterProps.put(Consts.INSTANCE_KEY, "xxx");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");

        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);

        Whitebox.setInternalState(obj, "config", monitorConfig);

        Whitebox.invokeMethod(obj, "initConsumer");

    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testInitConsumerCase1() throws Exception {
        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put(Consts.SASL_USER, "sasl_user");
        clusterProps.put(Consts.SASL_PASS, "");
        clusterProps.put(Consts.INSTANCE_KEY, "250xxxxxxxxxxxxxxxxxxx==");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");

        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);

        Whitebox.setInternalState(obj, "config", monitorConfig);

        Whitebox.invokeMethod(obj, "initConsumer");

    }

    @Test
    @PrepareForTest(OffsetMonitorTask.class)
    public void testInitConsumerCase2() throws Exception {
        KafkaConsumer mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.doNothing().when(mockConsumer).subscribe(Matchers.anyCollection());

        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> clusterProps = Maps.newHashMap();
        clusterProps.put(Consts.SASL_USER, "sasl_user");
        clusterProps.put(Consts.SASL_PASS, "");
        clusterProps.put(Consts.INSTANCE_KEY, "xxx");
        Whitebox.setInternalState(basicProps, "clusterProps", clusterProps);

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "kafka.servers");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");

        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);

        Whitebox.setInternalState(obj, "config", monitorConfig);

        Whitebox.invokeMethod(obj, "initConsumer");
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(BasicProps.class)
    public void testInitTpToJmxMappingCase0() throws Exception {
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
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/get_service_status")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("service_name", "test_kafka"))
                .withQueryStringParameter(new Parameter("idc", "sh")))
                .respond(response().withStatusCode(200).withBody(expected));

        OffsetMonitorTask obj = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(obj, "config", monitorConfig);
        Whitebox.invokeMethod(obj, "initTpToJmxMapping");
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(JmxUtils.class)
    public void testInitTpToJmxMappingCase1() throws Exception {
//        BasicProps mockProps = PowerMockito.mock(BasicProps.class);
//        PowerMockito.when(mockProps.getConsulServiceUrl()).thenReturn("http://127.0.0.1/get_service_status");
//        PowerMockito.mockStatic(BasicProps.class);
//        PowerMockito.when(BasicProps.getInstance()).thenReturn(mockProps);

        MBeanServerConnection mockMBean = PowerMockito.mock(MBeanServerConnection.class);
        PowerMockito.when(mockMBean.queryNames(Matchers.any(), Matchers.any())).thenReturn(Sets.newHashSet());

        JMXConnector mockJmxCon = PowerMockito.mock(JMXConnector.class);

        PowerMockito.when(mockJmxCon.getMBeanServerConnection()).thenReturn(mockMBean);

        PowerMockito.mockStatic(JmxUtils.class);
        PowerMockito.when(JmxUtils.getJmxConnector(Matchers.anyString(), Matchers.anyString())).thenReturn(mockJmxCon);
        PowerMockito.doNothing().when(JmxUtils.class);
        JmxUtils.closeJmxConnector(mockJmxCon);

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

        OffsetMonitorTask obj = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(obj, "config", monitorConfig);
        Whitebox.invokeMethod(obj, "initTpToJmxMapping");
        assertNotNull(obj);

    }


    @Test
    public void testStop() {
        OffsetMonitorTask obj = new OffsetMonitorTask();
        obj.stop();
        assertNotNull(obj);
    }

    @Test(expected = NullPointerException.class)
    @PrepareForTest(JmxUtils.class)
    public void testCollectTpLogOffsetCase1() throws Exception {
        MBeanServerConnection mockMBean = PowerMockito.mock(MBeanServerConnection.class);
        PowerMockito.when(mockMBean.getAttribute(Matchers.any(), Matchers.anyString())).thenReturn(100l);

        JMXConnector mockJmxCon = PowerMockito.mock(JMXConnector.class);
        PowerMockito.when(mockJmxCon.getMBeanServerConnection()).thenReturn(mockMBean);

        PowerMockito.mockStatic(JmxUtils.class);
        PowerMockito.when(JmxUtils.getJmxConnector(Matchers.anyString(), Matchers.anyString())).thenReturn(mockJmxCon);
        PowerMockito.doNothing().when(JmxUtils.class);
        JmxUtils.closeJmxConnector(mockJmxCon);

        //TODO mock objectName
        ObjectName mockObjectName = PowerMockito.mock(ObjectName.class);
        PowerMockito.when(mockObjectName.getKeyProperty("topic")).thenReturn("t_topic");
        PowerMockito.when(mockObjectName.getKeyProperty("partition")).thenReturn("10");
        PowerMockito.when(mockObjectName.getKeyProperty("name")).thenReturn("LogEndOffset");

        ConcurrentHashMap<String, Set<ObjectName>> jmxHostToTps = new ConcurrentHashMap<>();
        Set<ObjectName> keySet = Sets.newHashSet();
        keySet.add(mockObjectName);
        jmxHostToTps.put("host1", keySet);

        OffsetMonitorTask obj = new OffsetMonitorTask();
        //TODO 这里jmx会阻塞住
        Whitebox.setInternalState(obj, "jmxHostToTps", jmxHostToTps);
        Whitebox.invokeMethod(obj, "collectTpLogOffset");
    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testCommitToTsdbCase0() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        OffsetMonitorTask obj = new OffsetMonitorTask();
        ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>();
        ConsumerOffsetStat consumerOffsetStat = new ConsumerOffsetStat("t_server.xx.sh", "t_group", "t_topic", 1, 100l,
                14500000l);
        OFFSET_STAT.put("t_server.xx.sh|t_group|t_topic|1", consumerOffsetStat);
        Field field = obj.getClass().getDeclaredField("OFFSET_STAT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, OFFSET_STAT);

        Whitebox.invokeMethod(obj, "commitToTsdb");

    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testCommitToTsdbCase1() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        OffsetMonitorTask obj = new OffsetMonitorTask();
        ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>();
        ConsumerOffsetStat consumerOffsetStat = new ConsumerOffsetStat("t_server.xx.sh", "t_group", "t_topic", 1, 100l,
                14500000l);
        OFFSET_STAT.put("t_server.xx.sh|t_group|t_topic|1", consumerOffsetStat);
        Field field = obj.getClass().getDeclaredField("OFFSET_STAT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, OFFSET_STAT);

        Whitebox.setInternalState(OffsetMonitorTask.class, "TSDB_BATCH_SIZE", 1);
        Whitebox.invokeMethod(obj, "commitToTsdb");

    }

    @Test
    @PrepareForTest(InfluxdbUtils.class)
    public void testCommitToTsdbCase2() throws Exception {
        PowerMockito.mockStatic(InfluxdbUtils.class);
        PowerMockito.doNothing().when(InfluxdbUtils.class);
        InfluxdbUtils.submitData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        OffsetMonitorTask obj = new OffsetMonitorTask();
        ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>();
        ConsumerOffsetStat consumerOffsetStat = new ConsumerOffsetStat("t_server.xx.sh", "t_group", "t_topic", 1, 100l,
                14500000l);
        OFFSET_STAT.put("t_server.xx.sh|t_group|t_topic|1", consumerOffsetStat);
        Field field = obj.getClass().getDeclaredField("OFFSET_STAT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, OFFSET_STAT);

        ConcurrentHashMap<String, Long> LOG_END_OFFSET = new ConcurrentHashMap<>();
        LOG_END_OFFSET.put("t_server.xx.sh|t_topic|1", 101l);
        Field fieldEnd = obj.getClass().getDeclaredField("LOG_END_OFFSET");
        fieldEnd.setAccessible(true);
        Field modifiersFieldEnd = Field.class.getDeclaredField("modifiers");
        modifiersFieldEnd.setAccessible(true);
        modifiersFieldEnd.setInt(fieldEnd, fieldEnd.getModifiers() & ~Modifier.FINAL);
        fieldEnd.set(null, LOG_END_OFFSET);

        Whitebox.setInternalState(OffsetMonitorTask.class, "TSDB_BATCH_SIZE", 1);
        Whitebox.invokeMethod(obj, "commitToTsdb");

    }


    @Test
    public void testPullCase0() throws Exception {
        OffsetMonitorTask task = new OffsetMonitorTask();
        task.stop();
        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase1() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase2() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, null, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase3() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, null);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase4() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) -1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase5() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 2);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase6() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) -1);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase7() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 2);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase8() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase9() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "__consumer_offsets");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase10() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>();
        ConsumerOffsetStat consumerOffsetStat = new ConsumerOffsetStat("t_server.xx.sh", "t_group", "t_topic", 1, 100l,
                14500000l);
        OFFSET_STAT.put("test_kafka.xx.sh|t_group|t_topic|1", consumerOffsetStat);
        Field field = task.getClass().getDeclaredField("OFFSET_STAT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, OFFSET_STAT);

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase11() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "t_group");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 0);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        //PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenThrow(Exception.class);

        OffsetMonitorTask task = new OffsetMonitorTask();

        ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>();
        ConsumerOffsetStat consumerOffsetStat = new ConsumerOffsetStat("t_server.xx.sh", "t_group", "t_topic", 1, 100l,
                14500000l);
        OFFSET_STAT.put("test_kafka.xx.sh|t_group|t_topic|1", consumerOffsetStat);
        Field field = task.getClass().getDeclaredField("OFFSET_STAT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, OFFSET_STAT);

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }

    @Test
    public void testPullCase12() throws Exception {
        KafkaConsumer<byte[], byte[]> mockConsumer = PowerMockito.mock(KafkaConsumer.class);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordList = Lists.newArrayList();

        //构造key的byte数组
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //版本
        buffer.putShort((short) 1);
        //group信息
        byte[] bytesGroup = Utils.utf8((String) "");
        buffer.putShort((short) bytesGroup.length);
        buffer.put(bytesGroup);
        //topic信息
        byte[] bytesTopic = Utils.utf8((String) "t_topic");
        buffer.putShort((short) bytesTopic.length);
        buffer.put(bytesTopic);
        //partition信息
        buffer.putInt(1);
        buffer.flip();

        byte[] keyBytes = new byte[buffer.remaining()];
        buffer.get(keyBytes);

        //构造Value的byte数组
        ByteBuffer buffer4Val = ByteBuffer.allocate(100);
        //版本
        buffer4Val.putShort((short) 2);
        //offset信息
        buffer4Val.putLong(100);
        //metadata信息
        byte[] bytesMeta = Utils.utf8((String) "t_meta");
        buffer4Val.putShort((short) bytesMeta.length);
        buffer4Val.put(bytesMeta);
        //timestamp信息
        buffer4Val.putLong(14500000000l);
        buffer4Val.flip();

        byte[] valBytes = new byte[buffer4Val.remaining()];
        buffer4Val.get(valBytes);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t_topic", 1, 100l, keyBytes, valBytes);
        recordList.add(record);
        ConsumerRecords<byte[], byte[]> mockRecords = new ConsumerRecords(records);
        records.put(new TopicPartition("t_topic", 1), recordList);
        PowerMockito.when(mockConsumer.poll(Matchers.anyLong())).thenReturn(mockRecords);

        OffsetMonitorTask task = new OffsetMonitorTask();

        Map<String, String> props = Maps.newHashMap();
        props.put("kafka.servers", "test_kafka.xx.sh");
        props.put("use.sasl.auth", "true");
        props.put("jmx.port", "jmx.port");
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        OffsetMonitorConfig monitorConfig = new OffsetMonitorConfig(props);
        Whitebox.setInternalState(task, "config", monitorConfig);

        Whitebox.setInternalState(task, "consumer", mockConsumer);

        task.poll();
        assertNotNull(task);
    }


}
