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
package com.tencent.bk.base.datahub.databus.connect.source.datanode;

import static org.mockito.Matchers.anyLong;

import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.Merge;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.TransformResult;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class MergeSourceTaskTest {

    private String sourceRt = "datanode_rt_0";
    private String rtId = "datanode_0";
    private String sourceTopic = "table_" + sourceRt;
    private String destTopic = "table_" + rtId;
    private String bootstrapServer = "kafka:9092";
    private Producer<byte[], byte[]> producer;
    private Map<String, Consumer<byte[], byte[]>> kafkaConsumers = new HashMap<>();
    private String groupId = "merge";
    private String consumerName = "consumer_merge";

    /**
     * 先往sourTopic发送数据，保证sourTopic中有数据
     */
    @Before
    public void before() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
        // 因为Merge节点无需对数据进行转换，原样返回即可，所以这里value值的设置比较简单
        producer.send(new ProducerRecord<>(sourceTopic, "key1".getBytes(), "value1".getBytes()));

        // poll方法执行的结果需要我们检测poll方法执行前后目的地topic的offset数，才能判断producer是否发送数据成功，所以这里起一个consumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumers.put(consumerName, consumer);
    }

    @After
    public void after() {
        producer.close();
        kafkaConsumers.get(consumerName).close();
    }

    /**
     * 测试Merge节点poll方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollMergeSuccess() throws Exception {

        TopicPartition tp = new TopicPartition(destTopic, 0);
        kafkaConsumers.get(consumerName).assign(Arrays.asList(tp));
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        // 记录poll方法执行前目的地topic的offset数
        final long beforeOffset = kafkaConsumers.get(consumerName).position(tp);

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        // 记录poll方法执行后目的地topic的offset数，如果poll方法执行成功，afterOffset应该大于beforeOffset
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        long afterOffset = kafkaConsumers.get(consumerName).position(tp);
        Assert.assertTrue(afterOffset > beforeOffset);
    }

    /**
     * 测试poll方法中updateStat时触发异常的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class, Metric.class})
    public void testPollUpdateStatFailed() throws Exception {

        TopicPartition tp = new TopicPartition(destTopic, 0);
        kafkaConsumers.get(consumerName).assign(Arrays.asList(tp));
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        // 记录poll方法执行前目的地topic的offset数
        final long beforeOffset = kafkaConsumers.get(consumerName).position(tp);

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);
        PowerMockito.mockStatic(Metric.class);
        PowerMockito.when(Metric.getInstance()).thenThrow(Exception.class);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);
        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        // 记录poll方法执行后目的地topic的offset数，如果poll方法执行成功，afterOffset应该大于beforeOffset
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        long afterOffset = kafkaConsumers.get(consumerName).position(tp);
        System.out.println("before: " + beforeOffset + ", after: " + afterOffset);
        Assert.assertTrue(afterOffset > beforeOffset);

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为Metric.getInstance().updateStat时触发了异常，所以recordCountTotal没有增加
        Assert.assertEquals(0, count);
    }

    /**
     * 测试poll方法records为null的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class, BaseDatanodeSourceTask.class})
    public void testPollRecordsIsNull() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        KafkaConsumer<byte[], byte[]> consumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.when(consumer, "poll", anyLong()).thenReturn(null);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(consumer);

        BaseDatanodeSourceTask sourceTask = new FilterBySourceTask();
        sourceTask.start(props);

        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("lastCheckTime");
        field1.setAccessible(true);
        field1.set(sourceTask, System.currentTimeMillis() - 1000000);
        Field field2 = sourceTask.getClass().getSuperclass().getDeclaredField("lastLogCount");
        field2.setAccessible(true);
        field2.set(sourceTask, -10000);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        Assert.assertEquals(0, count);
    }

    /**
     * 测试poll方法中consumer.poll触发WakeupException的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class, BaseDatanodeSourceTask.class})
    public void testPollRecordsWakeupException() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        KafkaConsumer<byte[], byte[]> consumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.when(consumer, "poll", anyLong()).thenThrow(WakeupException.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(consumer);

        BaseDatanodeSourceTask sourceTask = new FilterBySourceTask();
        sourceTask.start(props);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为consumer.poll触发了WakeupException，所以recordCountTotal仍然为0
        Assert.assertEquals(0, count);
    }

    /**
     * 覆盖poll方法中isStop.get为true的情况
     *
     * @throws Exception
     */
    @Test
    public void testPollIsStopIsTrue() throws Exception {
        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("isStop");
        field.setAccessible(true);
        field.set(sourceTask, new AtomicBoolean(true));
        Assert.assertNull(sourceTask.poll());
    }

    /**
     * 覆盖sleepQuietly方法InterruptedException的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({BaseDatanodeSourceTask.class})
    public void testSleepQuietly() throws Exception {
        final BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        Thread.sleep(1000);
        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("isStop");
        field.setAccessible(true);
        field.set(sourceTask, new AtomicBoolean(true));
        Method method = sourceTask.getClass().getSuperclass().getDeclaredMethod("sleepQuietly", int.class);
        method.setAccessible(true);
        method.invoke(sourceTask, 1);
    }

    /**
     * 覆盖initConsumers触发ConnectException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testInitConsumers() throws Exception {
        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        addProps.put(Consts.CONSUMER_PREFIX + "max.poll.records", "xxx");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> configMap = new HashMap<>();
        configMap.put("name", "xxx");
        configMap.put("source.rt.list", "xx");
        DatanodeSourceConfig config = new DatanodeSourceConfig(configMap);
        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        Field field = sourceTask.getClass().getSuperclass().getDeclaredField("config");
        field.setAccessible(true);
        field.set(sourceTask, config);
        Method method = sourceTask.getClass().getSuperclass().getDeclaredMethod("initConsumers", Map.class);
        method.setAccessible(true);
        Map<String, List<String>> kafkaToTopics = new HashMap<>();
        kafkaToTopics.put("xx:9092", new ArrayList<>());
        method.invoke(sourceTask, kafkaToTopics);
    }

    /**
     * 覆盖initProducer触发ConnectException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testInitProducer() throws Exception {
        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        Method method = sourceTask.getClass().getSuperclass().getDeclaredMethod("initProducer", String.class);
        method.setAccessible(true);
        method.invoke(sourceTask, "xx");
    }

    /**
     * 通过mock覆盖poll方法中Merge节点tmpBid不为null的分支
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollDestTopicTmpBidNotNull() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, sourceRt);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        ConsumerRecord<byte[], byte[]> tmpRecord = new ConsumerRecord<>("table_rt_0", 0, 0, "key0".getBytes(),
                "value0".getBytes());
        resultRecord.putRecord("bidId0", tmpRecord);
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        // 这里设为20000，保证while循环体至少能循环1次
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        Assert.assertTrue(count > 0);
    }

    /**
     * 通过mock覆盖poll方法中resultCode == ResultCode.FAILED的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollResultCodeFailed() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为resultCode == ResultCode.FAILED，所以这里recordCountTotal = 0
        Assert.assertEquals(0, count);
    }

    /**
     * 通过mock覆盖poll方法中resultCode == ResultCode.ERROR的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollResultCodeError() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为resultCode == ResultCode.ERROR，所以这里recordCountTotal = 0
        Assert.assertEquals(0, count);
    }

    /**
     * 通过mock覆盖poll方法中resultCode为其它类型的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollResultCodeElse() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为resultCode为其它类型，所以这里recordCountTotal = 0
        Assert.assertEquals(0, count);
    }

    /**
     * 通过mock覆盖poll方法中resultType为其它类型的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollResultTypeElse() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        // 因为resultType为其它类型，所以这里recordCountTotal = 0
        Assert.assertEquals(0, count);
    }

    /**
     * 覆盖poll方法中multiRecordMap == null的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollMultiRecordMapIsNull() throws Exception {

        TopicPartition tp = new TopicPartition(destTopic, 0);
        kafkaConsumers.get(consumerName).assign(Arrays.asList(tp));
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        // 记录poll方法执行前目的地topic的offset数
        final long beforeOffset = kafkaConsumers.get(consumerName).position(tp);

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", UUID.randomUUID().toString());
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);

        Merge transform = PowerMockito.mock(Merge.class);
        TransformResult resultRecord = new TransformResult();
        PowerMockito.when(transform, "transform", Matchers.anyObject()).thenReturn(resultRecord);

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
        Field field1 = sourceTask.getClass().getSuperclass().getDeclaredField("transform");
        field1.setAccessible(true);
        field1.set(sourceTask, transform);

        // poll方法中存在while循环体，需通过多线程修改isStop的内容才能跳出循环体
        new Thread(() -> {
            try {
                sourceTask.poll();
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // junit中主线程执行结束后，不论子线程是否结束，都会退出程序。需要让主线程休眠一段时间，让子线程能够运行结束。
        Thread.sleep(20000);
        sourceTask.stop();

        // 记录poll方法执行后目的地topic的offset数，如果poll方法执行成功，afterOffset应该大于beforeOffset
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        long afterOffset = kafkaConsumers.get(consumerName).position(tp);
        Assert.assertEquals(afterOffset, beforeOffset);

        Field field = sourceTask.getClass().getSuperclass().getSuperclass().getDeclaredField("recordCountTotal");
        field.setAccessible(true);
        long count = (long) field.get(sourceTask);
        Assert.assertTrue(count > 0);
    }


} 
