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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * BizHdfsSinkTask Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */

public class BizHdfsSinkTaskTest extends TestWithMiniDFSCluster {

    private static ClientAndServer mockServer;


    @BeforeClass
    public static void beforeClass() {
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> propsConfig = Maps.newHashMap();
        propsConfig.put(Consts.API_DNS, "127.0.0.1:9999");
        propsConfig.put(Consts.METRIC_PREFIX + Consts.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        basicProps.addProps(propsConfig);
        mockServer = ClientAndServer.startClientAndServer(9999);
    }

    @AfterClass
    public static void afterClass() {
        mockServer.stop(true);
    }

    @After
    public void after() throws Exception {
        super.tearDown();
        mockServer.reset();
    }

    /**
     * Method: start(Map<String, String> props)
     */
    @Test
    public void testStartCase0() throws Exception {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.initialize(context);
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.start(props);
        task.open(Lists.newArrayList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));
        assertNotNull(task);

        Map<TopicPartition, Long> offsets = context.offsets();
        assertEquals(offsets.size(), 0);

        task.close(context.assignment());
        task.stop();
        assertNotNull(task);
    }

    /**
     * Method: start(Map<String, String> props)
     */
    @Test
    public void testStartCase1() throws Exception {
        System.setProperty(EtlConsts.DISPLAY_TIMEZONE, "UTC");
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.initialize(context);
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");
        props.put(Consts.MSG_SOURCE_TYPE, "json");
        task.start(props);
        assertNotNull(task);
    }

    /**
     * Method: start(Map<String, String> props)
     */
    @Test(expected = ConnectException.class)
    public void testStartCase2() throws Exception {
        System.setProperty("user.timezone", "UTC");
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.initialize(context);

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100a");
        props.put(Consts.MSG_SOURCE_TYPE, "json");
        task.start(props);
        assertNotNull(task);
    }

    @Test
    public void testFlush() {
        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.flush(null);
        assertNotNull(task);
    }

    @Test
    public void testOpenCase01() {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.initialize(context);
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.start(props);

        task.open(Lists.newArrayList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));

        assertNotNull(task);

        task.stop();

        task.close(context.assignment());
    }

    @Test
    public void testInitHdfsCase01() {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        task.initialize(context);
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.start(props);
        assertNotNull(task);
    }

    @Test
    public void testPutCase01() {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.initialize(context);
        task.start(props);
        task.put(sinkRecords);
        task.stop();
        assertNotNull(task);
    }

    @Test
    public void testPutCase02() {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.initialize(context);
        task.start(props);
        task.put(Lists.newArrayList());
        task.stop();
        assertNotNull(task);
    }

    @Test
    public void testPutCase03() {
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

        BizHdfsSinkTask task = new BizHdfsSinkTask();
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.initialize(context);
        task.start(props);
        Whitebox.setInternalState(task, "lastCheckTime", (System.currentTimeMillis() - 1000000));
        task.put(sinkRecords);

        task.stop();
        assertNotNull(task);
    }

    @Test
    public void testPutCase04() {
        Metric mockMetric = PowerMockito.mock(Metric.class);
        PowerMockito.doNothing().when(mockMetric)
                .updateStat(Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
                        Matchers.anyString(), Matchers.anyInt(), Matchers.anyInt(), Matchers.anyString(),
                        Matchers.anyString());
        PowerMockito.doNothing().when(mockMetric).updateTopicErrInfo(Matchers.anyString(), Matchers.anyList());
        PowerMockito.doNothing().when(mockMetric)
                .updateDelayInfo(Matchers.anyString(), Matchers.anyLong(), Matchers.anyLong(), Matchers.anyLong());

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\",\"rt.id\":\"100_xxx_test\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.initialize(context);
        task.start(props);
        Whitebox.setInternalState(task, "metric", mockMetric);
        Whitebox.setInternalState(task, "lastCheckTime", (System.currentTimeMillis() - 1000000));
        task.put(sinkRecords);

        mockServer.reset();

        String expected2 = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=int\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\",\"rt.id\":\"100_xxx_test\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected2));

        Whitebox.setInternalState(task, "lastCheckTime", (System.currentTimeMillis() - 1000000));

        task.put(sinkRecords);

        task.stop();

        assertNotNull(task);


    }


    @Test(expected = ConnectException.class)
    public void testPutCase05() {
        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doNothing().when(topicPartitionWriter).close();

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        Metric mockMetric = PowerMockito.mock(Metric.class);
        PowerMockito.doNothing().when(mockMetric)
                .updateStat(Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
                        Matchers.anyString(), Matchers.anyInt(), Matchers.anyInt(), Matchers.anyString(),
                        Matchers.anyString());
        PowerMockito.doNothing().when(mockMetric).updateTopicErrInfo(Matchers.anyString(), Matchers.anyList());
        PowerMockito.doNothing().when(mockMetric)
                .updateDelayInfo(Matchers.anyString(), Matchers.anyLong(), Matchers.anyLong(), Matchers.anyLong());

        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
                + "                 \"dimensions\":\"k1\",\n"
                + "                 \"bootstrap.servers\":\"bs.sh\",\"rt.id\":\"100_xxx_test\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", "100_xxx_test")))
                .respond(response().withStatusCode(200).withBody(expected));

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : context.assignment()) {
            for (long offset = 0; offset < 7; offset++) {
                SinkRecord sinkRecord =
                        new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
                sinkRecords.add(sinkRecord);
            }
        }
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        task.initialize(context);
        task.start(props);

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);
        Whitebox.setInternalState(task, "metric", mockMetric);
        Whitebox.setInternalState(task, "lastCheckTime", (System.currentTimeMillis() - 1000000));
        Whitebox.setInternalState(task, "lastLogCount", -5000l);
        task.put(sinkRecords);

        task.stop();

        assertNotNull(task);


    }


}
