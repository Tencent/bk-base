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

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TransformUtilsTest {

    /**
     * 测试getAvroBinaryString
     */
    @Test
    @PrepareForTest({TransformUtils.class})
    public void testGetAvroBinaryString() throws Exception {
        PowerMockito.whenNew(ByteArrayOutputStream.class).withAnyArguments().thenThrow(new IOException());

        GenericRecord genericRecord = PowerMockito.mock(GenericRecord.class);
        DataFileWriter<GenericRecord> dataFileWriter = PowerMockito.mock(DataFileWriter.class);
        PowerMockito.doThrow(new AvroRuntimeException("")).when(dataFileWriter).close();

        String result = TransformUtils.getAvroBinaryString(genericRecord, dataFileWriter);
        Assert.assertEquals("", result);

        //这里finally代码块覆盖率存在问题
        PowerMockito.doNothing().when(dataFileWriter).close();
        TransformUtils.getAvroBinaryString(genericRecord, dataFileWriter);
    }

    /**
     * 测试composeAvroRecordArr触发异常的情况
     */
    @Test
    @PrepareForTest({TransformUtils.class})
    public void composeAvroRecordArr() throws Exception {
        Map<String, String> cols = new HashMap<>();
        cols.put(Consts.TIMESTAMP, Consts.TIMESTAMP);
        final Schema recordSchema = new Schema.Parser().parse(TransformUtils.getAvroSchema(cols));
        long tsSec = System.currentTimeMillis() / 1000;
        final SimpleDateFormat dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String[] columns = {"timestamp"};

        List<List<Object>> records = new ArrayList<>();
        List<Object> list1 = new ArrayList<>();
        list1.add(tsSec);
        records.add(list1);

        PowerMockito.whenNew(Date.class).withArguments(Matchers.anyLong()).thenThrow(new AvroRuntimeException(""));
        // 构建msgSchema
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericArray<GenericRecord> arr = TransformUtils
                .composeAvroRecordArr(tsSec, records, columns, recordSchema, recordArrSchema, dteventtimeDateFormat);
        Assert.assertNull(arr.get(0).get("dtEventTimeStamp"));
    }

    /**
     * 测试getLastCheckpoint中consumer关闭异常的情况
     */
    @Test
    @PrepareForTest({TransformUtils.class})
    public void testGetLastCheckpointFailed() throws Exception {
        long offset = 1;
        TopicPartition tp = new TopicPartition("test", 0);

        // 覆盖lastRecord == null的情况
        Map<TopicPartition, List<ConsumerRecord<String, String>>> mapRecords = new HashMap<>();
        List<ConsumerRecord<String, String>> listRecords = new ArrayList<>();
        mapRecords.put(tp, listRecords);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(mapRecords);
        KafkaConsumer<String, String> consumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.when(consumer.position(tp)).thenReturn(offset);
        PowerMockito.when(consumer.poll((100))).thenReturn(records);

        // 覆盖consumer.close()抛异常的情况
        PowerMockito.doThrow(new KafkaException("")).when(consumer).close();
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(consumer);
        Assert.assertNull(TransformUtils.getLastCheckpoint("xx", "test", 0));
    }

    /**
     * 覆盖getLastCheckpoint中lastRecord.key() == null的情况
     */
    @Test
    @PrepareForTest({TransformUtils.class})
    public void testGetLastCheckpointRecordIsNull() throws Exception {
        long offset = 1;
        TopicPartition tp = new TopicPartition("test", 0);

        Map<TopicPartition, List<ConsumerRecord<String, String>>> mapRecords = new HashMap<>();
        List<ConsumerRecord<String, String>> listRecords = new ArrayList<>();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test", 0, 0, null, null);
        listRecords.add(record);
        mapRecords.put(tp, listRecords);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(mapRecords);
        KafkaConsumer<String, String> consumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.when(consumer.position(tp)).thenReturn(offset);
        PowerMockito.when(consumer.poll((100))).thenReturn(records);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(consumer);
        Assert.assertNull(TransformUtils.getLastCheckpoint("xx", "test", 0));
    }

    /**
     * 覆盖getLastCheckpoint中offset < 1的情况
     */
    @Test
    @PrepareForTest({TransformUtils.class})
    public void testGetLastCheckpointOffsetIsZero() throws Exception {
        long offset = 0;
        TopicPartition tp = new TopicPartition("test", 0);
        KafkaConsumer<String, String> consumer = PowerMockito.mock(KafkaConsumer.class);
        PowerMockito.when(consumer.position(tp)).thenReturn(offset);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(consumer);
        Assert.assertNull(TransformUtils.getLastCheckpoint("xx", "test", 0));
    }
}
