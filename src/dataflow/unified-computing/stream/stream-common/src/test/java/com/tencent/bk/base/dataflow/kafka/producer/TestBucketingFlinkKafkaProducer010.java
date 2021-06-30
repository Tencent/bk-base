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

package com.tencent.bk.base.dataflow.kafka.producer;

import com.tencent.bk.base.dataflow.kafka.producer.util.DummyKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.kafka.producer.util.TestAbstractAvroKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.kafka.producer.util.TestAbstractAvroKeyedSerializationSchema.TestAvroKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.topology.TestStreamTopology;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;


public class TestBucketingFlinkKafkaProducer010 {

    @Test
    public void testInvoke() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "bootstrap servers");
        properties.put("acks", "all");
        properties.put("retries", 5);
        properties.put("max.in.flight.requests.per.connection", 5);
        properties.put("batch.size", 1048576);
        properties.put("linger.ms", 1000);
        properties.put("buffer.memory", 52428800);
        properties.put("max.request.size", 3145728);
        BucketingFlinkKafkaProducer010<Row> kafkaProducer =
                new BucketingFlinkKafkaProducer010<Row>(
                        "test_topic",
                        new DummyKeyedSerializationSchema(),
                        new TestAvroKeyedSerializationSchema(TestStreamTopology.getTestStreamTopology(),
                                TestAbstractAvroKeyedSerializationSchema.getTestSinkNode()),
                        properties,
                        TestAbstractAvroKeyedSerializationSchema.getTestSinkNode(),
                        TestStreamTopology.getTestStreamTopology());

        BlockingQueue<Row> testOutputValues = new ArrayBlockingQueue<>(200);
        IntStream.range(0, 99).forEach(i -> testOutputValues.add(Row.of(i, "abc", 1L, 2.1F, 3.4D, 1621829147)));
        Field outputValues = kafkaProducer.getClass().getDeclaredField("outputValues");
        outputValues.setAccessible(true);
        outputValues.set(kafkaProducer, testOutputValues);

        KafkaProducer mockProducer = PowerMockito.mock(KafkaProducer.class);
        Mockito.when(mockProducer.send(null, null)).thenReturn(null);
        Field producer = kafkaProducer.getClass().getSuperclass().getSuperclass().getDeclaredField("producer");
        producer.setAccessible(true);
        producer.set(kafkaProducer, mockProducer);

        ProcessingTimeService mockProcessingTimeService = PowerMockito.mock(ProcessingTimeService.class);
        Mockito.when(mockProcessingTimeService.getCurrentProcessingTime()).thenReturn(System.currentTimeMillis());
        Field processingTimeService = kafkaProducer.getClass().getDeclaredField("processingTimeService");
        processingTimeService.setAccessible(true);
        processingTimeService.set(kafkaProducer, mockProcessingTimeService);

        Field partitions = kafkaProducer.getClass().getDeclaredField("partitions");
        partitions.setAccessible(true);
        partitions.set(kafkaProducer, new int[]{0});

        kafkaProducer.invoke(Row.of(100, "abc", 1L, 2.1F, 3.4D, 1621829147), null);
    }
}
