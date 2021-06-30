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

package com.tencent.bk.base.datahub.databus.clean.connect;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TransformSinkTaskTest {

    private static ClientAndServer mockServer;
    private String rtId = "transform_0";
    private String destTopic = "table_" + rtId;
    private String bootstrapServer = "localhost:9092";
    private String connectorName = "name0";
    private String groupId = "groupId0";


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


    /**
     * 测试start方法
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testStart() throws Exception {
        Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(Consts.CLUSTER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        clusterInfo.put(Consts.CLUSTER_PREFIX + "recover.offset.from.key", "false");
        clusterInfo.put(Consts.CLUSTER_PREFIX + "disable.context.refresh", "false");
        BasicProps.getInstance().addProps(clusterInfo);
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        SinkTask ts = new TransformSinkTask();
        ts.start(getProps());
        ts.stop();

        Field field = ts.getClass().getDeclaredField("config");
        field.setAccessible(true);
        TransformSinkConfig config = (TransformSinkConfig) field.get(ts);
        Assert.assertEquals(rtId, config.rtId);
        Assert.assertEquals(groupId, config.cluster);
    }

    /**
     * 测试start方法，补充没有覆盖的分支
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class, TransformSinkTask.class})
    public void testStart2() throws Exception {
        Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(Consts.CLUSTER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        clusterInfo.put(Consts.CLUSTER_PREFIX + "disable.context.refresh", "false");
        BasicProps.getInstance().addProps(clusterInfo);
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());
        PowerMockito.mockStatic(System.class);
        System.setProperty(EtlConsts.DISPLAY_TIMEZONE, "Asia/Shanghai");

        SinkTask ts = new TransformSinkTask();
        ts.start(getProps());
        // 覆盖stop方法中producer == null的情况
        Field field1 = ts.getClass().getDeclaredField("producer");
        field1.setAccessible(true);
        field1.set(ts, null);
        ts.stop();

        Field field2 = ts.getClass().getDeclaredField("config");
        field2.setAccessible(true);
        TransformSinkConfig config = (TransformSinkConfig) field2.get(ts);
        Assert.assertEquals(rtId, config.rtId);
        Assert.assertEquals(groupId, config.cluster);
    }

    /**
     * 测试清洗成功的正常数据
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutSuccess() throws Exception {
        BasicProps.getInstance().addProps(getClusterInfo());
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10240000");

        TransformSinkTask ts = new TransformSinkTask();
        ts.start(getProps());
        ((TransformSinkTask) ts).markBkTaskCtxChanged();
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = ts.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(ts, producer);
        String data = "{\"timestamp\": \"2019-01-01 12:21:12\", \"field2\": -1, \"field3\": 82324989420000000332, "
                + "\"field4\": 11111123230090909.09883}";
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, "timestamp", Schema.BYTES_SCHEMA,
                data.getBytes(), 0);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        ts.put(records);
        Thread.sleep(1000);
        ts.stop();
    }

    /**
     * 测试清洗成功的正常数据
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutSuccessKeyIsNull() throws Exception {
        BasicProps.getInstance().addProps(getClusterInfo());
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"10240000");
        TransformSinkTask ts = new TransformSinkTask();
        ts.start(getProps());
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = ts.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(ts, producer);
        ((TransformSinkTask) ts).markBkTaskCtxChanged();
        Long beforeOffset = null;
        String data = "{\"timestamp\": \"2019-01-01 12:21:12\", \"field2\": -1, \"field3\": 82324989420000000332, "
                + "\"field4\": 11111123230090909.09883}";
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, null, Schema.BYTES_SCHEMA,
                data.getBytes(), beforeOffset == null ? 0 : beforeOffset);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, new HashMap<>(), Schema.BYTES_SCHEMA,
                data.getBytes(), beforeOffset == null ? 1 : beforeOffset + 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        ts.put(records);
        Thread.sleep(1000);
        ts.stop();
    }

    /**
     * 测试清洗成功的正常数据
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutUpdateStatFailed() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, groupId);
        props.put(BkConfig.RT_ID, rtId);
        props.put(BkConfig.CONNECTOR_NAME, "test");
        props.put(Consts.PRODUCER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        BasicProps.getInstance().addProps(getClusterInfo());
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"10240000");
        TransformSinkTask ts = new TransformSinkTask();
        ts.start(props);
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = ts.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(ts, producer);
        ((TransformSinkTask) ts).markBkTaskCtxChanged();
        Long beforeOffset = null;
        String data = "{\"timestamp\": \"2019-01-01 12:21:12\", \"field2\": -1, \"field3\": 82324989420000000332, "
                + "\"field4\": 11111123230090909.09883}";
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, "timestamp", Schema.BYTES_SCHEMA,
                data.getBytes(), beforeOffset == null ? 0 : beforeOffset);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        ts.put(records);
        Thread.sleep(1000);
        ts.stop();
    }


    /**
     * 测试清洗失败的异常数据
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutSetBadEtlMsg() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "timestamp=timestamp,id=long,info=bigint,cal=bigdecimal,xxx=bigdecimal");
        String conf = TestUtils.getFileContent("/assign_obj.json");
        rtProps.put(Consts.ETL_CONF, conf);
        rtProps.put(BkConfig.RT_ID, rtId);
        BasicProps.getInstance().addProps(getClusterInfo());
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10240000");
        TransformSinkTask ts = new TransformSinkTask();
        ts.start(getProps());
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = ts.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(ts, producer);
        ((TransformSinkTask) ts).markBkTaskCtxChanged();
        Long beforeOffset = null;
        String data = "{\"timestamp\": \"2019-01-01 12:21:12\", \"field2\": -1, \"field3\": 11111123230090909.09883, "
                + "\"field4\": 11111123230090909.09883}";
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, Schema.STRING_SCHEMA, "timestamp", Schema.BYTES_SCHEMA,
                data.getBytes(), beforeOffset == null ? 0 : beforeOffset);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        ts.put(records);
        Thread.sleep(1000);
        Field field = Metric.getInstance().getClass().getDeclaredField("badMsgStats");
        field.setAccessible(true);
        ConcurrentHashMap<String, Map<String, Object>> badMsgStats = (ConcurrentHashMap) field
                .get(Metric.getInstance());
        Assert.assertEquals(data, badMsgStats.get(rtId).get(Consts.MSGVALUE));
    }

    /**
     * 测试put方法ConnectException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testPutNeedThrowException() throws Exception {
        TransformSinkTask task = new TransformSinkTask();
        Field field = task.getClass().getDeclaredField("needThrowException");
        field.setAccessible(true);
        field.set(task, new AtomicBoolean(true));
        List<SinkRecord> sinkRecords = new ArrayList<>();
        sinkRecords.add(new SinkRecord("test", 1, null, "key", null,
                "value", 123L));
        task.put(sinkRecords);
    }

    /**
     * 覆盖put方法Records的size为0的情况
     */
    @Test
    public void testPutSizeIsNull() {
        TransformSinkTask task = new TransformSinkTask();
        task.put(new ArrayList<>());
    }


    /**
     * 测试open方法offset为null的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(TransformUtils.class)
    public void testOpenOffsetIsNull() throws Exception {
        TransformSinkTask ts = new TransformSinkTask();
        TopicPartition topicPartition = new TopicPartition(destTopic, 0);
        Collection<TopicPartition> collection = new ArrayList<>();
        collection.add(topicPartition);

        Field field1 = ts.getClass().getDeclaredField("recoverOffsetFromKey");
        field1.setAccessible(true);
        field1.set(ts, true);

        Map<String, String> props = new HashMap<>();
        TransformSinkConfig config = new TransformSinkConfig(props);
        Field field2 = ts.getClass().getDeclaredField("config");
        field2.setAccessible(true);
        field2.set(ts, config);

        PowerMockito.mockStatic(TransformUtils.class);
        PowerMockito.when(TransformUtils.getLastCheckpoint(null, null, 0)).thenReturn(null);
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        ts.initialize(new WorkerSinkTaskContext(new KafkaConsumer<>(consumerProps)));
        ts.open(collection);
        Field field = ts.getClass().getSuperclass().getSuperclass().getDeclaredField("context");
        field.setAccessible(true);
        WorkerSinkTaskContext context = (WorkerSinkTaskContext) field.get(ts);
        Assert.assertEquals(context.offsets().size(), 0);
    }

    /**
     * 覆盖open方法recoverOffsetFromKey为false的情况
     *
     * @throws Exception
     */
    @Test(expected = IllegalWorkerStateException.class)
    public void testOpenRecoverOffsetIsFalse() throws Exception {
        final TransformSinkTask ts = new TransformSinkTask();
        TopicPartition topicPartition = new TopicPartition(destTopic, 0);
        Collection<TopicPartition> collection = new ArrayList<>();
        collection.add(topicPartition);
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        ts.initialize(new WorkerSinkTaskContext(new KafkaConsumer<>(consumerProps)));
        ts.open(collection);

        Field field = ts.getClass().getSuperclass().getSuperclass().getDeclaredField("context");
        field.setAccessible(true);
        WorkerSinkTaskContext context = (WorkerSinkTaskContext) field.get(ts);
        Assert.assertEquals(context.offsets().size(), 0);
    }

    /**
     * 测试initProducer异常情况
     *
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void testInitProducer() throws Exception {
        TransformSinkTask ts = new TransformSinkTask();
        Map<String, String> props = new HashMap<>();
        props.put("producer.xxx", "xxx");
        TransformSinkConfig config = new TransformSinkConfig(props);
        Field field2 = ts.getClass().getDeclaredField("config");
        field2.setAccessible(true);
        field2.set(ts, config);

        Method method = ts.getClass().getDeclaredMethod("initProducer");
        method.setAccessible(true);
        method.invoke(ts);
    }

    /**
     * 提供时间配置时, 测试pipe-extractor执行兼容性
     */
    @Test
    public void testProcessDataHaveTimeColumns() throws Exception{
        TransformSinkTask task = new TransformSinkTask();
        String expected = "{\n" +
                "     \"errors\": null,\n" +
                "     \"message\": \"ok\",\n" +
                "     \"code\": \"1500200\",\n" +
                "     \"data\": {\n" +
                "          \"zookeeper.addr\": \"xx.xx.xx.xx:2181/kafka-test-3\",\n" +
                "          \"kafka\": {\n" +
                "               \"storage_config\": \"{}\",\n" +
                "               \"expires\": \"3d\",\n" +
                "               \"physical_table_name\": \"table_591_test_3\"\n" +
                "          },\n" +
                "          \"channel_cluster_name\": \"testinner\",\n" +
                "          \"hdfs\": {\n" +
                "               \"connection_info\": \"{\\\"flush_size\\\": 1000000, \\\"interval\\\": 60000, "
                + "\\\"hdfs_cluster_name\\\": \\\"testCluster\\\", \\\"servicerpc_port\\\": 53310, \\\"ids\\\": "
                + "\\\"nn1\\\", \\\"rpc_port\\\": 9000, \\\"hdfs_url\\\": \\\"hdfs://testCluster\\\", \\\"hosts\\\": "
                + "\\\"xx-xx-xx-xx\\\", \\\"log_dir\\\": \\\"/kafka/logs\\\", \\\"hdfs_default_params\\\": "
                + "{\\\"dfs.replication\\\": 2}, \\\"topic_dir\\\": \\\"/kafka/data/\\\", \\\"port\\\": 50070}\",\n"
                +
                "               \"expires\": \"360d\",\n" +
                "               \"cluster_name\": \"hdfs-testCluster\",\n" +
                "               \"storage_config\": \"{}\",\n" +
                "               \"version\": \"hadoop-2.6.0-cdh5.4.11\",\n" +
                "               \"id\": 452,\n" +
                "               \"physical_table_name\": \"/kafka/data/591/test_3_591\"\n" +
                "          },\n" +
                "          \"dimensions\": \"\",\n" +
                "          \"hdfs.data_type\": \"parquet\",\n" +
                "          \"etl.conf\": \"{\\\"extract\\\": {\\\"args\\\": [], \\\"next\\\": {\\\"subtype\\\": "
                + "\\\"assign_obj\\\", \\\"next\\\": null, \\\"type\\\": \\\"assign\\\", \\\"assign\\\": "
                + "[{\\\"assign_to\\\": \\\"col1\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": \\\"col1\\\"}, "
                + "{\\\"assign_to\\\": \\\"col2\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": \\\"col2\\\"}, "
                + "{\\\"assign_to\\\": \\\"time\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": \\\"time\\\"}], "
                + "\\\"label\\\": \\\"label58eb21\\\"}, \\\"result\\\": \\\"person\\\", \\\"label\\\": "
                + "\\\"labeld73e5f\\\", \\\"type\\\": \\\"fun\\\", \\\"method\\\": \\\"from_json\\\"}, \\\"conf\\\": "
                + "{\\\"timestamp_len\\\": 0, \\\"encoding\\\": \\\"UTF-8\\\", \\\"time_format\\\": \\\"yyyy-MM-dd "
                + "HH:mm:ss\\\", \\\"timezone\\\": 8, \\\"output_field_name\\\": \\\"timestamp\\\", "
                + "\\\"time_field_name\\\": \\\"time\\\"}}\",\n"
                +
                "          \"platform\": \"bk_data\",\n" +
                "          \"rt.id\": \"591_test_3\",\n" +
                "          \"geog_area\": \"inland\",\n" +
                "          \"project_id\": 4,\n" +
                "          \"columns\": \"col1=string,timestamp=timestamp,col2=string,time=string\",\n" +
                "          \"etl.id\": 713,\n" +
                "          \"channel_cluster_index\": 10,\n" +
                "          \"msg.type\": \"avro\",\n" +
                "          \"bootstrap.servers\": \"kafka.test:9092\",\n" +
                "          \"bk_biz_id\": 591,\n" +
                "          \"fields\": [\n" +
                "               \"col1\",\n" +
                "               \"col2\",\n" +
                "               \"time\",\n" +
                "               \"timestamp\"\n" +
                "          ],\n" +
                "          \"storages.list\": [\n" +
                "               \"hdfs\",\n" +
                "               \"kafka\"\n" +
                "          ],\n" +
                "          \"channel_type\": \"kafka\",\n" +
                "          \"data.id\": 123456,\n" +
                "          \"table_name\": \"test_3\",\n" +
                "          \"rt.type\": \"clean\"\n" +
                "     },\n" +
                "     \"result\": true\n" +
                "}";
        final String data = "{\"col1\":\"test1\",\"col2\":\"test2\",\"time\":\"2020-02-02 02:02:02\"}";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", rtId)))
                .respond(response().withStatusCode(200).withBody(expected));
        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);
        task.initialize(mockContext);
        Map map = Maps.newHashMap();
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        map.put(BkConfig.GROUP_ID, groupId);
        map.put(BkConfig.RT_ID, rtId);
        map.put(Consts.PRODUCER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        map.put(BkConfig.CONNECTOR_NAME, connectorName);

        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        task.start(map);

        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = task.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(task, producer);
        final Collection<SinkRecord> records = Lists.newArrayList();
        SinkRecord sinkRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(sinkRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(sinkRecord.kafkaOffset()).thenReturn(1L);
        PowerMockito.when(sinkRecord.key()).thenReturn("key");
        PowerMockito.when(sinkRecord.value()).thenReturn(data.getBytes());
        records.add(sinkRecord);
        task.processData(records);
    }

    /**
     * 测试提供默认时间配置时, pipe-extractor执行效果
     */
    @Test
    public void testProcessDataUseDefaultTime() throws Exception {
        TransformSinkTask task = new TransformSinkTask();
        String expected = "{\n" +
                "     \"errors\": null,\n" +
                "     \"message\": \"ok\",\n" +
                "     \"code\": \"1500200\",\n" +
                "     \"data\": {\n" +
                "          \"zookeeper.addr\": \"xx.xx.xx.xx:2181/kafka-test-3\",\n" +
                "          \"kafka\": {\n" +
                "               \"storage_config\": \"{}\",\n" +
                "               \"expires\": \"3d\",\n" +
                "               \"physical_table_name\": \"table_591_test_3\"\n" +
                "          },\n" +
                "          \"channel_cluster_name\": \"testinner\",\n" +
                "          \"hdfs\": {\n" +
                "               \"connection_info\": \"{\\\"flush_size\\\": 1000000, \\\"interval\\\": 60000, "
                + "\\\"hdfs_cluster_name\\\": \\\"testCluster\\\", \\\"servicerpc_port\\\": 53310, \\\"ids\\\": "
                + "\\\"nn1\\\", \\\"rpc_port\\\": 9000, \\\"hdfs_url\\\": \\\"hdfs://testCluster\\\", \\\"hosts\\\": "
                + "\\\"xx-xx-xx-xx\\\", \\\"log_dir\\\": \\\"/kafka/logs\\\", \\\"hdfs_default_params\\\": "
                + "{\\\"dfs.replication\\\": 2}, \\\"topic_dir\\\": \\\"/kafka/data/\\\", \\\"port\\\": 50070}\",\n"
                +
                "               \"expires\": \"360d\",\n" +
                "               \"cluster_name\": \"hdfs-testCluster\",\n" +
                "               \"storage_config\": \"{}\",\n" +
                "               \"version\": \"hadoop-2.6.0-cdh5.4.11\",\n" +
                "               \"id\": 452,\n" +
                "               \"physical_table_name\": \"/kafka/data/591/test_3_591\"\n" +
                "          },\n" +
                "          \"dimensions\": \"\",\n" +
                "          \"hdfs.data_type\": \"parquet\",\n" +
                "          \"etl.conf\": \"{\\\"extract\\\": {\\\"args\\\": [], \\\"next\\\": {\\\"subtype\\\": "
                + "\\\"assign_obj\\\", \\\"next\\\": null, \\\"type\\\": \\\"assign\\\", \\\"assign\\\": "
                + "[{\\\"assign_to\\\": \\\"col1\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": \\\"col1\\\"}, "
                + "{\\\"assign_to\\\": \\\"col2\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": \\\"col2\\\"}], "
                + "\\\"label\\\": \\\"label58eb21\\\"}, \\\"result\\\": \\\"person\\\", \\\"label\\\": "
                + "\\\"labeld73e5f\\\", \\\"type\\\": \\\"fun\\\", \\\"method\\\": \\\"from_json\\\"}, \\\"conf\\\": "
                + "{\\\"timestamp_len\\\": 13, \\\"encoding\\\": \\\"UTF-8\\\", \\\"time_format\\\": \\\"yyyy-MM-dd "
                + "HH:mm:ss\\\", \\\"timezone\\\": 8, \\\"output_field_name\\\": \\\"timestamp\\\", "
                + "\\\"time_field_name\\\": \\\"localTime\\\"}}\",\n"
                +
                "          \"platform\": \"bk_data\",\n" +
                "          \"rt.id\": \"591_test_3\",\n" +
                "          \"geog_area\": \"inland\",\n" +
                "          \"project_id\": 4,\n" +
                "          \"columns\": \"col1=string,timestamp=timestamp,col2=string\",\n" +
                "          \"etl.id\": 713,\n" +
                "          \"channel_cluster_index\": 10,\n" +
                "          \"msg.type\": \"avro\",\n" +
                "          \"bootstrap.servers\": \"kafka.test:9092\",\n" +
                "          \"bk_biz_id\": 591,\n" +
                "          \"fields\": [\n" +
                "               \"col1\",\n" +
                "               \"col2\",\n" +
                "               \"timestamp\"\n" +
                "          ],\n" +
                "          \"storages.list\": [\n" +
                "               \"hdfs\",\n" +
                "               \"kafka\"\n" +
                "          ],\n" +
                "          \"channel_type\": \"kafka\",\n" +
                "          \"data.id\": 123456,\n" +
                "          \"table_name\": \"test_3\",\n" +
                "          \"rt.type\": \"clean\"\n" +
                "     },\n" +
                "     \"result\": true\n" +
                "}";
        final String data = "{\"col1\":\"test1\",\"col2\":\"test2\",\"time\":\"2020-02-02 02:02:02\"}";
        mockServer.when(request()
                .withPath("/databus/shipper/get_rt_info")
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", rtId)))
                .respond(response().withStatusCode(200).withBody(expected));
        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);
        task.initialize(mockContext);
        Map map = Maps.newHashMap();
        map.put("data.file", "t_data_file");
        map.put("topic", "t_topic");
        map.put(BkConfig.GROUP_ID, groupId);
        map.put(BkConfig.RT_ID, rtId);
        map.put(Consts.PRODUCER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        map.put(BkConfig.CONNECTOR_NAME, connectorName);
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000");
        task.start(map);
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String s) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long l, TimeUnit timeUnit) {

            }
        };
        Field producerField = task.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(task, producer);
        final Collection<SinkRecord> records = Lists.newArrayList();
        SinkRecord sinkRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(sinkRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(sinkRecord.kafkaOffset()).thenReturn(1L);
        PowerMockito.when(sinkRecord.key()).thenReturn("key");
        PowerMockito.when(sinkRecord.value()).thenReturn(data.getBytes());
        records.add(sinkRecord);
        task.processData(records);
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, groupId);
        props.put(BkConfig.RT_ID, rtId);
        props.put(Consts.PRODUCER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        props.put(BkConfig.CONNECTOR_NAME, connectorName);
        return props;
    }

    private Map<String, String> getRtProps() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "timestamp=timestamp,id=long,info=bigint,cal=bigdecimal");
        String conf = TestUtils.getFileContent("/assign_obj.json");
        rtProps.put(Consts.ETL_CONF, conf);
        rtProps.put(BkConfig.RT_ID, rtId);
        return rtProps;
    }

    private Map<String, String> getClusterInfo() {
        Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(Consts.CLUSTER_PREFIX + Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        clusterInfo.put(Consts.CLUSTER_PREFIX + "recover.offset.from.key", "true");
        clusterInfo.put(Consts.CLUSTER_PREFIX + "disable.context.refresh", "false");
        return clusterInfo;
    }
}
