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

import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.Merge;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.TransformResult;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SplitBySourceTaskTest {

    private String sourceRt = "datanode_rt_1";
    private String rtId = "datanode_1";
    private String sourceTopic = "table_" + sourceRt;
    private String destTopic = "table_bid0_" + rtId.split("_", 2)[1];
    private String bootstrapServer = "kafka:9092";
    private Producer<byte[], byte[]> producer;
    private Map<String, Consumer<byte[], byte[]>> kafkaConsumers = new HashMap<>();
    private String groupId = "split";
    private String consumerName = "consumer_split";

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
        // 将生成的avro record对象序列化后作为value值
        producer.send(new ProducerRecord<>(sourceTopic, "20181212000000".getBytes(), getAvroValue().getBytes()));

        // poll方法执行的结果需要我们检测目的地topic的offset数，才能判断producer是否发送数据成功，所以这里起一个consumer
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
     * 测试分拆节点poll方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollSplitSuccess() throws Exception {

        TopicPartition tp = new TopicPartition(destTopic, 0);
        kafkaConsumers.get(consumerName).assign(Arrays.asList(tp));
        kafkaConsumers.get(consumerName).seekToEnd(Arrays.asList(tp));
        // 记录poll方法执行前目的地topic的offset数
        final long beforeOffset = kafkaConsumers.get(consumerName).position(tp);

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "a=string,b=string,c=bigdecimal,d=bigint,timestamp=timestamp");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo(sourceRt)).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", sourceRt);
        addProps.put(Consts.CONNECTOR_PREFIX + "name", groupId);
        addProps.put(Consts.CONSUMER_PREFIX + "enable.auto.commit", "true");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.offset.reset", "earliest");
        addProps.put(Consts.CONSUMER_PREFIX + "auto.commit.interval.ms", "1000");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, rtId);
        props.put("config", "{\"split_logic\":[{\"logic_exp\":\"b>a\",\"bid\":\"bid0\"}]}");

        BaseDatanodeSourceTask sourceTask = new SplitBySourceTask();
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
    }

    /**
     * 通过mock覆盖poll方法中SplitBy节点tmpBid为null的分支
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollDestTopicTmpBidIsNull() throws Exception {

        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        // 源topic的名字为table_rt_0，目的地topic的名字为table_0
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
        ConsumerRecord<byte[], byte[]> tmpRecord = new ConsumerRecord<>("table_rt_0", 0, 0, "key0".getBytes(),
                "value0".getBytes());
        resultRecord.putRecord(null, tmpRecord);
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
        Assert.assertTrue(count > 0);
    }

    private String getAvroValue() {
        Map<String, String> cols = new HashMap<>();
        cols.put("a", "string");
        cols.put("b", "string");
        cols.put("c", "bigdecimal");
        cols.put("d", "bigint");
        cols.put(Consts.TIMESTAMP, Consts.TIMESTAMP);
        final Schema recordSchema = AvroUtils.getRecordSchema(cols);
        long tsSec = System.currentTimeMillis() / 1000;
        SimpleDateFormat dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String[] columns = {"a", "b", "c", "d", "timestamp"};
        final List<List<Object>> records = new ArrayList<>();
        List<Object> list1 = new ArrayList<>();
        list1.add("valueA");
        list1.add("valueB");
        list1.add(new BigDecimal("28423894238838382810038585.328329598"));
        list1.add(new BigInteger("257468205563563"));
        list1.add((int) tsSec);
        records.add(list1);

        // 构建msgSchema
        Schema recordArrSchema = Schema.createArray(recordSchema);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_0")
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
        GenericRecord msgRecord = new GenericData.Record(msgSchema);
        GenericArray<GenericRecord> arr = AvroUtils.composeAvroRecordArray(records, columns, recordSchema);

        msgRecord.put(Consts._VALUE_, arr);
        msgRecord.put(Consts._TAGTIME_, tsSec);
        msgRecord.put(Consts._METRICTAG_, "Consumer");

        String value = AvroUtils.getAvroBinaryString(msgRecord,
                new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(msgSchema)));
        return value;
    }
}
