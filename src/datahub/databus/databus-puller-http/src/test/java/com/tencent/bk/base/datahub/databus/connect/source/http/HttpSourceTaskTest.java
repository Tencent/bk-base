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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class HttpSourceTaskTest {

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(HttpSourceConnectorConfig.END_TIME_FIELD, "end_time");
        props.put(HttpSourceConnectorConfig.START_TIME_FIELD, "start_time");
        props.put(HttpSourceConnectorConfig.HTTP_URL, "http://test.com");
        props.put(HttpSourceConnectorConfig.PERIOD_SECOND, "1");
        props.put(HttpSourceConnectorConfig.HTTP_METHOD, "get");
        props.put(HttpSourceConnectorConfig.TIME_FORMAT, "yyyy-MM-dd HH:mm:ss");
        props.put(HttpSourceConnectorConfig.DEST_KAFKA_BS, "localhost:9092");
        props.put(HttpSourceConnectorConfig.DEST_TOPIC, "table_xxx");
        return props;
    }

    @Test
    public void testStartTask() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> props = getProps();
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offset(anyObject())).thenReturn(null);
        when(reader.offsets(anyObject())).thenReturn(null);
        when(context.offsetStorageReader()).thenReturn(reader);
        HttpSourceTask task = new HttpSourceTask();
        task.initialize(context);
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        HttpSourceConnectorConfig config = (HttpSourceConnectorConfig) configField.get(task);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.END_TIME_FIELD), config.endTimeField);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.START_TIME_FIELD), config.startTimeField);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.HTTP_URL), config.httpUrl);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.PERIOD_SECOND), String.valueOf(config.periodSecond));
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.HTTP_METHOD), config.httpMethod.toString());
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.TIME_FORMAT), config.timeFormat);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.DEST_KAFKA_BS), config.destKafkaBs);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.DEST_TOPIC), config.destTopic);

        Field workerNumField = task.getClass().getDeclaredField("isTimeQuery");
        workerNumField.setAccessible(true);
        boolean isTimeQuery = (boolean) workerNumField.get(task);
        Assert.assertTrue(isTimeQuery);

    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testDoGetCollect() throws Exception {
        String getResult = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"count\":12,"
                + "\"connectors\":[\"xxxx\"],\"error\":\"\"},\"result\":true}";
        PowerMockito.mockStatic(HttpUtils.class);

        Map<String, String> props = getProps();
        props.put(HttpSourceConnectorConfig.HTTP_METHOD, "get");

        HttpSourceConnectorConfig config = new HttpSourceConnectorConfig(props);

        HttpSourceTask task = new HttpSourceTask();
        TimeFormat timeFieldFormat = new TimeFormat(config.timeFormat);

        PowerMockito.when(HttpUtils.get(anyString())).thenReturn(getResult);

        Field isTimeQueryField = task.getClass().getDeclaredField("isTimeQuery");
        isTimeQueryField.setAccessible(true);
        isTimeQueryField.set(task, true);

        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(task, config);

        Field timeFieldFormatField = task.getClass().getDeclaredField("timeFieldFormat");
        timeFieldFormatField.setAccessible(true);
        timeFieldFormatField.set(task, timeFieldFormat);

        task.doGetCollect();

        Field queueField = task.getClass().getDeclaredField("queue");
        queueField.setAccessible(true);
        BlockingQueue<BkSourceRecord<String>> queue = (BlockingQueue<BkSourceRecord<String>>) queueField.get(task);
        Assert.assertEquals(1, queue.size());
        BkSourceRecord<String> bkSourceRecord = queue.peek();
        Assert.assertNotNull(bkSourceRecord);
        JsonParser jsonParser = new JsonParser();
        JsonElement data = jsonParser.parse(bkSourceRecord.getValue()).getAsJsonObject().get("data");

        Assert.assertEquals(data.toString(), jsonParser.parse(getResult).getAsJsonObject().get("data").toString());
    }


    @Test
    @PrepareForTest({HttpUtils.class})
    public void testDoPostCollect() throws Exception {
        String getResult = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"count\":12,"
                + "\"connectors\":[\"xxxx\"],\"error\":\"\"},\"result\":true}";
        PowerMockito.mockStatic(HttpUtils.class);

        Map<String, String> props = getProps();
        props.put(HttpSourceConnectorConfig.HTTP_METHOD, "post");

        HttpSourceConnectorConfig config = new HttpSourceConnectorConfig(props);

        HttpSourceTask task = new HttpSourceTask();
        TimeFormat timeFieldFormat = new TimeFormat(config.timeFormat);

        PowerMockito.when(HttpUtils.post(anyString(), anyMap())).thenReturn(getResult);

        Field isTimeQueryField = task.getClass().getDeclaredField("isTimeQuery");
        isTimeQueryField.setAccessible(true);
        isTimeQueryField.set(task, true);

        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(task, config);

        Field timeFieldFormatField = task.getClass().getDeclaredField("timeFieldFormat");
        timeFieldFormatField.setAccessible(true);
        timeFieldFormatField.set(task, timeFieldFormat);

        task.doPostCollect();

        Field queueField = task.getClass().getDeclaredField("queue");
        queueField.setAccessible(true);
        BlockingQueue<BkSourceRecord<String>> queue = (BlockingQueue<BkSourceRecord<String>>) queueField.get(task);
        Assert.assertEquals(1, queue.size());
        BkSourceRecord<String> bkSourceRecord = queue.peek();
        Assert.assertNotNull(bkSourceRecord);
        JsonParser jsonParser = new JsonParser();
        JsonElement data = jsonParser.parse(bkSourceRecord.getValue()).getAsJsonObject().get("data");

        Assert.assertEquals(data.toString(), jsonParser.parse(getResult).getAsJsonObject().get("data").toString());
    }

    @Test
    public void testGetEndTime() {
        HttpSourceTask task = new HttpSourceTask();
        long lastTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        long endTime = task.getEndTime(lastTime, 60);
        Assert.assertEquals(lastTime + TimeUnit.SECONDS.toMillis(60), endTime);

        lastTime = System.currentTimeMillis();
        endTime = task.getEndTime(lastTime, 60);
        Assert.assertTrue(endTime < lastTime + TimeUnit.SECONDS.toMillis(60));
    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPollData() throws Exception {
        String getResult = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"count\":12,"
                + "\"connectors\":[\"xxxx\"],\"error\":\"\"},\"result\":true}";
        PowerMockito.mockStatic(HttpUtils.class);

        Map<String, String> props = getProps();
        props.put(HttpSourceConnectorConfig.HTTP_METHOD, "post");

        HttpSourceConnectorConfig config = new HttpSourceConnectorConfig(props);

        HttpSourceTask task = new HttpSourceTask();
        TimeFormat timeFieldFormat = new TimeFormat(config.timeFormat);

        PowerMockito.when(HttpUtils.post(anyString(), anyMap())).thenReturn(getResult);

        Field isTimeQueryField = task.getClass().getDeclaredField("isTimeQuery");
        isTimeQueryField.setAccessible(true);
        isTimeQueryField.set(task, true);

        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(task, config);

        Field timeFieldFormatField = task.getClass().getDeclaredField("timeFieldFormat");
        timeFieldFormatField.setAccessible(true);
        timeFieldFormatField.set(task, timeFieldFormat);
        Producer<String, String> producer = new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
                callback.onCompletion(null, null);
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

        task.doPostCollect();

        List<SourceRecord> sourceRecords = task.pollData();
        Assert.assertEquals(1, sourceRecords.size());
    }
}
