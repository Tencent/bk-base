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

package com.tencent.bk.base.datahub.databus.connect.queue;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class QueueSinkTaskTest {

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(BkConfig.RT_ID, "123");
        props.put(QueueSinkConfig.PRODUCER_BOOTSTRAP_SERVERS, "localhost:9092");
        props.put(QueueSinkConfig.DEST_TOPIC_PREFIX, "queue_");
        props.put(QueueSinkConfig.USE_SASL, "false");
        props.put(QueueSinkConfig.SASL_USER, "user");
        props.put(QueueSinkConfig.SASL_PASS, "xxxx");
        return props;
    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testStartTask() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> props = getProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(getRtProps());
        QueueSinkTask task = new QueueSinkTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        QueueSinkConfig config = (QueueSinkConfig) configField.get(task);
        Assert.assertEquals(props.get(QueueSinkConfig.PRODUCER_BOOTSTRAP_SERVERS), config.bootstrapServers);
        Assert.assertEquals(props.get(QueueSinkConfig.DEST_TOPIC_PREFIX), config.prefix);
        Assert.assertEquals(props.get(QueueSinkConfig.USE_SASL), String.valueOf(config.useSasl));
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_USER), config.saslUser);
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_PASS), config.saslPass);

        Field destTopicField = task.getClass().getDeclaredField("destTopic");
        destTopicField.setAccessible(true);
        String destTopic = (String) destTopicField.get(task);
        Assert.assertEquals(config.prefix + config.rtId, destTopic);


    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testProcessData()
            throws NoSuchFieldException, IllegalAccessException, ExecutionException, InterruptedException, IOException {
        Map<String, String> props = getProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(getRtProps());
        QueueSinkTask task = new QueueSinkTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        QueueSinkConfig config = (QueueSinkConfig) configField.get(task);
        Assert.assertEquals(props.get(QueueSinkConfig.PRODUCER_BOOTSTRAP_SERVERS), config.bootstrapServers);
        Assert.assertEquals(props.get(QueueSinkConfig.DEST_TOPIC_PREFIX), config.prefix);
        Assert.assertEquals(props.get(QueueSinkConfig.USE_SASL), String.valueOf(config.useSasl));
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_USER), config.saslUser);
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_PASS), config.saslPass);

        CompletableFuture<ProducerRecord<String, String>> completableFuture = new CompletableFuture<>();
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                callback.onCompletion(null, null);
                completableFuture.complete(producerRecord);
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
            public Map<MetricName, ? extends Metric> metrics() {
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

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        Collection<SinkRecord> records = new ArrayList<>(1);
        records.add(sinkRecord1);
        task.put(records);
        log.info("executed write");
        // sleep to wait backend flush complete
        Thread.sleep(1000);
        ProducerRecord<String, String> record = completableFuture.get();
        Assert.assertEquals(config.prefix + config.rtId, record.topic());
        Assert.assertEquals("{\"id\": 123}", record.value());
        task.stop();

    }

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS, "id=int");
        rtProps.put(BkConfig.RT_ID, "123_test_rt");
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        return rtProps;
    }

    private String getAvroBinaryString() throws IOException {
        Schema recordSchema =
                new Schema.Parser()
                        .parse("{\"type\":\"record\", \"name\":\"msg\", \"fields\":[{\"name\":\"id\", "
                                + "\"type\":[\"null\",\"int\"]}]}");
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroRecord.put("id", 123);
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
}
