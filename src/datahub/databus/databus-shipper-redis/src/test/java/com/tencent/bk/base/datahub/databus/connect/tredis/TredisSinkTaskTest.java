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

package com.tencent.bk.base.datahub.databus.connect.tredis;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
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
public class TredisSinkTaskTest {

    private static RedisServer redisServer = null;

    static {
        redisServer = new RedisServer();
        redisServer.configScheme(16, new AbstractOperation() {
        }); // 这里添加自己实现的redis命令
        try {
            redisServer.listener("127.0.0.1", 50016); // Redis runs in the background
        } catch (IOException e) {

        }
    }


    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testJoinPutSuccess() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);
        task.stop();
    }

    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testKeyValuePutSuccess() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        props.put(TredisSinkConfig.STORAGE_TYPE, TredisSinkConfig.STORAGE_TYPES_KV);
        props.put(TredisSinkConfig.STORAGE_VALUES, "id");
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);
        task.stop();
    }


    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testListPutSuccess() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        props.put(TredisSinkConfig.STORAGE_TYPE, TredisSinkConfig.STORAGE_TYPES_LIST);
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);
        task.stop();
    }

    /**
     * 测试put方法成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPublishPutSuccess() throws Exception {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        props.put(TredisSinkConfig.STORAGE_TYPE, TredisSinkConfig.STORAGE_TYPES_PUBLISH);
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, null, STRING_SCHEMA, value, 1);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);
        task.put(records);
        log.info("executed write");

        // sleep to wait backend flush complete
        Thread.sleep(1000);
        task.stop();
    }

    /**
     * 覆盖put方法中 records.size() == 0 的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testPutRecordSizeIsZero() {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);
        task.put(new ArrayList<>());
        task.flush(new HashMap<>());
        task.stop();
    }

    /**
     * 覆盖put方法中 startTask 的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testStartTask() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);

        TredisSinkTask task = new TredisSinkTask();
        task.start(props);
        Field bizId = task.getClass().getDeclaredField("bizId");
        bizId.setAccessible(true);
        Assert.assertEquals("615", bizId.get(task).toString());
    }


    /**
     * 测试start方法执行失败的情况：no comma allowed in config topics
     */
    @Test(expected = ConfigException.class)
    @PrepareForTest({HttpUtils.class})
    public void testStartFailed() {
        Map<String, String> props = getProps();
        props.put("topics", "topic1,topic2");
        Map<String, String> rtProps = getRtProps();

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(props.get(BkConfig.RT_ID))).thenReturn(rtProps);
        TredisSinkTask task = new TredisSinkTask();
        task.start(props);
    }

    private static Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.RT_ID, "123_test_rt");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(TredisSinkConfig.REDIS_PORT, "50016");
        props.put(TredisSinkConfig.REDIS_DNS, "127.0.0.1");
        props.put(TredisSinkConfig.REDIS_AUTH, "XXXXXXXX");
        props.put(TredisSinkConfig.STORAGE_KEYS, "id");
        props.put(TredisSinkConfig.STORAGE_EXPIRE_DAYS, "3");
        props.put(TredisSinkConfig.STORAGE_TYPE, "join");
        props.put(TredisSinkConfig.STORAGE_SEPARATOR, ":");
        props.put(TredisSinkConfig.STORAGE_KEY_SEPARATOR, "_");
        props.put("topics", "table_123_test_rt");
        return props;
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
