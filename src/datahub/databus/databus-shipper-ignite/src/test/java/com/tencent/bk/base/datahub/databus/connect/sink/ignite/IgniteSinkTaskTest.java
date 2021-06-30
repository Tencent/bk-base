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

package com.tencent.bk.base.datahub.databus.connect.sink.ignite;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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
public class IgniteSinkTaskTest {

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(BkConfig.RT_ID, "591_test");
        props.put(IgniteSinkConfig.KEY_FIELDS, "id");
        props.put(CacheConsts.IGNITE_PASS, "xxxx");
        props.put(CacheConsts.IGNITE_USER, "admin");
        props.put(CacheConsts.IGNITE_CLUSTER, "ignite_cluster");
        props.put(CacheConsts.IGNITE_PORT, "10800");
        props.put(CacheConsts.IGNITE_HOST, "localhost");
        props.put(IgniteSinkConfig.KEY_SEPARATOR, ":");
        props.put(IgniteSinkConfig.IGNITE_CACHE, "bk_test_002_591");
        props.put(IgniteSinkConfig.IGNITE_MAX_RECORDS, "1900000");
        props.put(IgniteSinkConfig.USE_THIN_CLIENT, "true");
        return props;
    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testStartTask() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> props = getProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(getRtProps());
        IgniteSinkTask task = new IgniteSinkTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        IgniteSinkConfig config = (IgniteSinkConfig) configField.get(task);
        Assert.assertArrayEquals(props.get(IgniteSinkConfig.KEY_FIELDS).split(","), config.keyFields);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PASS), config.ignitePass);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_USER), config.igniteUser);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_CLUSTER), config.igniteCluster);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PORT), String.valueOf(config.ignitePort));
        Assert.assertEquals(props.get(CacheConsts.IGNITE_HOST), config.igniteHost);
        Assert.assertEquals(props.get(IgniteSinkConfig.KEY_SEPARATOR), config.keySeparator);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_CACHE), config.igniteCache);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_MAX_RECORDS), String.valueOf(config.igniteMaxRecords));
        Assert.assertEquals(props.get(IgniteSinkConfig.USE_THIN_CLIENT), String.valueOf(config.useThinClient));

        Field colsInOrderField = task.getClass().getDeclaredField("colsInOrder");
        colsInOrderField.setAccessible(true);
        String[] colsInOrder = (String[]) colsInOrderField.get(task);
        Assert.assertEquals("dtEventTime", colsInOrder[0]);
        Assert.assertEquals("dtEventTimeStamp", colsInOrder[1]);
        Assert.assertEquals("localTime", colsInOrder[2]);
        Assert.assertEquals("id", colsInOrder[3]);

    }

    @Test
    @PrepareForTest({HttpUtils.class})
    public void testProcessData()
            throws NoSuchFieldException, IllegalAccessException, InterruptedException, IOException, ExecutionException {
        Map<String, String> props = getProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(getRtProps());
        IgniteSinkTask task = new IgniteSinkTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        IgniteSinkConfig config = (IgniteSinkConfig) configField.get(task);
        Assert.assertArrayEquals(props.get(IgniteSinkConfig.KEY_FIELDS).split(","), config.keyFields);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PASS), config.ignitePass);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_USER), config.igniteUser);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_CLUSTER), config.igniteCluster);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PORT), String.valueOf(config.ignitePort));
        Assert.assertEquals(props.get(CacheConsts.IGNITE_HOST), config.igniteHost);
        Assert.assertEquals(props.get(IgniteSinkConfig.KEY_SEPARATOR), config.keySeparator);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_CACHE), config.igniteCache);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_MAX_RECORDS), String.valueOf(config.igniteMaxRecords));
        Assert.assertEquals(props.get(IgniteSinkConfig.USE_THIN_CLIENT), String.valueOf(config.useThinClient));

        CompletableFuture<Map<String, Map<String, Object>>> completableFuture = new CompletableFuture<>();
        BkCache<String> cache = new BkCache<String>() {
            @Override
            protected void initClient() {

            }

            @Override
            protected Map<String, Map> getAll(String s, Set<String> set, List<String> list) {
                return null;
            }

            @Override
            protected void putAll(String s, Map<String, Map<String, Object>> map) {
                completableFuture.complete(map);
            }

            @Override
            protected void removeAll(String s, Set<String> set) {

            }

            @Override
            public int size(String s) {
                return 0;
            }
        };

        Field producerField = task.getClass().getDeclaredField("cache");
        producerField.setAccessible(true);
        producerField.set(task, cache);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        Collection<SinkRecord> records = new ArrayList<>(1);
        records.add(sinkRecord1);
        task.put(records);
        log.info("executed write");
        // sleep to wait backend flush complete
        Thread.sleep(1000);
        Map<String, Map<String, Object>> caches = completableFuture.get();
        Assert.assertEquals(1, caches.size());
        Assert.assertEquals(123, caches.get("123").get("id"));
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
