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


package com.tencent.bk.base.datahub.databus.connect.sink.es;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class EsSinkTaskTest {

    private static int httpPort = 9209;
    private static String rtId = "es_0";
    private static String groupId = "groupId0";
    private static String clusterName = "my_cluster";
    private static ClientAndServer mockServer;
    private static String expected = "{\n"
            + "        \"message\":\"ok\",\n"
            + "        \"errors\": \"\",\n"
            + "        \"code\":\"1500200\",\n"
            + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
            + "                 \"rt.id\":\"es_0\",\n"
            + "                 \"bootstrap.servers\":\"localhost:9092\"},\n"
            + "        \"result\":\"true\"\n"
            + "       }";

    // https://github.com/allegro/embedded-elasticsearch
    private static EmbeddedElastic embeddedElastic = EmbeddedElastic.builder()
            .withElasticVersion("6.6.2")
            .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
            .withSetting(PopularProperties.HTTP_PORT, httpPort)
            .withSetting(PopularProperties.CLUSTER_NAME, clusterName)
            .withStartTimeout(2, TimeUnit.MINUTES)
            .withEsJavaOpts("-Xms128m -Xmx512m")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Map<String, String> propsConfig = new HashMap<>();
        propsConfig.put(Consts.API_DNS, "localhost:8888");
        propsConfig.put(Consts.CLUSTER_PREFIX + "es.auto.create.index", "true");
        propsConfig.put(Consts.CLUSTER_PREFIX + "transport.batch.record.count", "1");
        BasicProps.getInstance().addProps(propsConfig);
        mockServer = startClientAndServer(8888);
        mockServer.when(request()
                .withPath(Consts.API_RT_PATH_DEFAULT)
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", rtId)))
                .respond(response().withStatusCode(200).withBody(expected));
        embeddedElastic.start();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        Thread.sleep(10 * 1000);
        mockServer.stop(true);
        embeddedElastic.stop();
    }

    /**
     * 测试start方法成功执行的情况
     *
     * @throws Exception
     */
    public void testStart() throws Exception {
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, Consts.ETL);
        EsSinkTask task = new EsSinkTask();
        Map<String, String> props = getProps();
        task.start(props);

        Field field = task.getClass().getDeclaredField("userEtl");
        field.setAccessible(true);
        Assert.assertTrue((boolean) field.get(task));
    }

    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    public void testPutSuccess() throws Exception {
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, Consts.JSON);

        // 修改触发批量提交操作的BATCH_SIZE，便于assert doc是否add成功
        Field field = EsClientUtils.class.getDeclaredField("BATCH_SIZE");
        field.setAccessible(true);
        field.set(EsClientUtils.getInstance(), 0);

        EsSinkConnector connector = new EsSinkConnector();
        EsSinkTask task = new EsSinkTask();
        Map<String, String> props = getProps();
        connector.start(props);
        task.start(props);

        // rtId表示的index alias应该和rtId_suffix建立关联，但是这一步是由存储模块做的，所以这个测试的存查都是基于rtId这个index进行的，和rtId_suffix没有关系
        long tsSec = System.currentTimeMillis() / 1000;
        String value = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        SinkRecord sinkRecord = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, Consts.COLLECTOR_DS + "=" + tsSec,
                Schema.STRING_SCHEMA, value, 0);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord);
        task.put(records);
        Thread.sleep(20000);
        Assert.assertTrue(embeddedElastic.fetchAllDocuments(rtId).contains(value));
        task.stop();
        connector.stop();
    }

    /**
     * 测试put方法成功执行的情况，覆盖isTaskContextChanged为true的分支
     *
     * @throws Exception
     */
    @Test
    public void testPutMoreBranch() throws Exception {
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, Consts.JSON);

        // 修改触发批量提交操作的BATCH_SIZE，便于assert doc是否add成功
        Field field = EsClientUtils.class.getDeclaredField("BATCH_SIZE");
        field.setAccessible(true);
        field.set(EsClientUtils.getInstance(), 0);

        EsSinkConnector connector = new EsSinkConnector();
        EsSinkTask task = new EsSinkTask();
        Map<String, String> props = getProps();
        props.remove(BkConfig.CONNECTOR_NAME);
        connector.start(props);
        task.start(props);
        task.markBkTaskCtxChanged();
        Field field1 = task.getClass().getDeclaredField("lastLogCount");
        field1.setAccessible(true);
        field1.set(task, -5000);
        Field field2 = task.getClass().getDeclaredField("lastLogTime");
        field2.setAccessible(true);
        field2.set(task, System.currentTimeMillis() - 10000);

        String value = "{\"k1\":\"v1\",\"k2\":\"v2\",\"k3\":\"v3\"}";
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value, 0);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        task.put(records);
        Thread.sleep(20000);
        Assert.assertTrue(embeddedElastic.fetchAllDocuments(rtId).contains(value));
        task.stop();
        connector.stop();
    }

    /**
     * 测试put方法中 records.size() == 0 的情况
     *
     * @throws Exception
     */
    @Test
    public void testPutRecordIsNull() throws Exception {
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, "");
        EsSinkTask task = new EsSinkTask();
        Map<String, String> props = getProps();
        task.start(props);
        task.put(new ArrayList<>());

        Field field = task.getClass().getDeclaredField("userEtl");
        field.setAccessible(true);
        Assert.assertFalse((boolean) field.get(task));
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, groupId);
        props.put(BkConfig.RT_ID, rtId);
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(EsSinkConfig.ES_CLUSTER_NAME_CONFIG, clusterName);
        props.put(EsSinkConfig.TYPE_NAME_CONFIG, "kafka-connect");
        props.put(EsSinkConfig.ES_HOSTS_CONFIG, "localhost");
        props.put(EsSinkConfig.ES_HTTP_PORT_CONFIG, String.valueOf(httpPort));
        return props;
    }

    /**
     * 测试将json串中字符串类型的json串，转换成json对象
     */
    @Test
    public void testParseJsonField() throws Exception {
        String orgJson = "{\"rt.id\":\"591_test\",\"etl.conf\":\"{\\\"conf\\\": {\\\"timezone\\\": 8, "
                + "\\\"time_format\\\": \\\"yyyy-MM-dd HH:mm:ss\\\", \\\"output_field_name\\\": \\\"timestamp\\\", "
                + "\\\"time_field_name\\\": \\\"datetime\\\", \\\"timestamp_len\\\": 0, \\\"encoding\\\": "
                + "\\\"UTF-8\\\"}, \\\"extract\\\": {\\\"label\\\": \\\"labelf0d0ce\\\", \\\"next\\\": "
                + "{\\\"assign\\\": [{\\\"assign_to\\\": \\\"dataid\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"dataid\\\"}, {\\\"assign_to\\\": \\\"type\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"type\\\"}, {\\\"assign_to\\\": \\\"datetime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"datetime\\\"}, {\\\"assign_to\\\": \\\"utctime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\":"
                + " \\\"utctime\\\"}, {\\\"assign_to\\\": \\\"timezone\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"timezone\\\"}, {\\\"assign_to\\\": \\\"data\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"data\\\"}], \\\"subtype\\\": \\\"assign_obj\\\", \\\"type\\\": \\\"assign\\\", "
                + "\\\"label\\\": \\\"label96bd67\\\", \\\"next\\\": null}, \\\"method\\\": \\\"from_json\\\", "
                + "\\\"args\\\": [], \\\"type\\\": \\\"fun\\\", \\\"result\\\": \\\"q\\\"}}\"}";
        EsSinkTask task = new EsSinkTask();
        String parsedJson = task.parseJsonField(orgJson, Lists.newArrayList(""));
        Assert.assertEquals(orgJson, parsedJson);
        parsedJson = task.parseJsonField(orgJson, null);
        Assert.assertEquals(orgJson, parsedJson);
        parsedJson = task.parseJsonField(orgJson, Lists.newArrayList("etl.conf"));
        String expected = "{\"rt.id\":\"591_test\",\"etl.conf\":{\"conf\":{\"timezone\":8,"
                + "\"time_format\":\"yyyy-MM-dd HH:mm:ss\",\"output_field_name\":\"timestamp\","
                + "\"time_field_name\":\"datetime\",\"timestamp_len\":0,\"encoding\":\"UTF-8\"},"
                + "\"extract\":{\"label\":\"labelf0d0ce\",\"next\":{\"assign\":[{\"assign_to\":\"dataid\","
                + "\"type\":\"string\",\"key\":\"dataid\"},{\"assign_to\":\"type\",\"type\":\"string\","
                + "\"key\":\"type\"},{\"assign_to\":\"datetime\",\"type\":\"string\",\"key\":\"datetime\"},"
                + "{\"assign_to\":\"utctime\",\"type\":\"string\",\"key\":\"utctime\"},{\"assign_to\":\"timezone\","
                + "\"type\":\"string\",\"key\":\"timezone\"},{\"assign_to\":\"data\",\"type\":\"string\","
                + "\"key\":\"data\"}],\"subtype\":\"assign_obj\",\"type\":\"assign\",\"label\":\"label96bd67\","
                + "\"next\":null},\"method\":\"from_json\",\"args\":[],\"type\":\"fun\",\"result\":\"q\"}}}";
        Assert.assertEquals(expected, parsedJson);
    }

    /**
     * 测试将json串中字符串类型的json串，转换成json对象
     */
    @Test
    public void testGenerateUniqueKey() throws Exception {
        String orgJson = "{\"rt.id\":\"591_test\",\"etl.conf\":\"{\\\"conf\\\": {\\\"timezone\\\": 8, "
                + "\\\"time_format\\\": \\\"yyyy-MM-dd HH:mm:ss\\\", \\\"output_field_name\\\": \\\"timestamp\\\", "
                + "\\\"time_field_name\\\": \\\"datetime\\\", \\\"timestamp_len\\\": 0, \\\"encoding\\\": "
                + "\\\"UTF-8\\\"}, \\\"extract\\\": {\\\"label\\\": \\\"labelf0d0ce\\\", \\\"next\\\": "
                + "{\\\"assign\\\": [{\\\"assign_to\\\": \\\"dataid\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"dataid\\\"}, {\\\"assign_to\\\": \\\"type\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"type\\\"}, {\\\"assign_to\\\": \\\"datetime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"datetime\\\"}, {\\\"assign_to\\\": \\\"utctime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\":"
                + " \\\"utctime\\\"}, {\\\"assign_to\\\": \\\"timezone\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"timezone\\\"}, {\\\"assign_to\\\": \\\"data\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"data\\\"}], \\\"subtype\\\": \\\"assign_obj\\\", \\\"type\\\": \\\"assign\\\", "
                + "\\\"label\\\": \\\"label96bd67\\\", \\\"next\\\": null}, \\\"method\\\": \\\"from_json\\\", "
                + "\\\"args\\\": [], \\\"type\\\": \\\"fun\\\", \\\"result\\\": \\\"q\\\"}}\"}";
        EsSinkTask task = new EsSinkTask();
        String uniqueKey = task.generateUniqueKey(orgJson, Lists.newArrayList("rt.id"), null, 1);
        Assert.assertEquals("591_test", uniqueKey);
        uniqueKey = task.generateUniqueKey(orgJson, Lists.newArrayList(),
                new SinkRecord("test_topic", 1, null, "test_key", null, "test_value", 123456), 0);
        Assert.assertEquals("test_topic11234560", uniqueKey);
    }
}
