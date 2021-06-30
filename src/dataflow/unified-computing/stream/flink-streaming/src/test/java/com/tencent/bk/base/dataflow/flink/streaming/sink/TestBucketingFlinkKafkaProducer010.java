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

package com.tencent.bk.base.dataflow.flink.streaming.sink;

import static org.junit.Assert.assertEquals;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.configuration.UCConfiguration;
import com.tencent.bk.base.dataflow.core.topo.KafkaOutput;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.kafka.producer.util.AbstractAvroKeyedSerializationSchema;
import java.util.Properties;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBucketingFlinkKafkaProducer010 {

    public static int UC_BATCH_SIZE = 10;
    public static int UC_BATCH_FACTOR = 3;

    @Test
    public void testGetBatchRows() throws Exception {

        DateTime dt = DateTime.parse("2021-01-26 23:00:00.123", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        //无跨小时级别的时间乱序
        testOrderSendData(dt, 10, 2, new int[]{1});
        testOrderSendData(dt, 20, 1, new int[]{1, 1});
        testOrderSendData(dt, 35, 1, new int[]{1, 1, 1});
        //无跨小时级别的时间乱序，正常跨天
        testOrderSendData(dt, 90, 1, new int[]{1, 1, 1, 1, 1, 2, 1, 1, 1});

        //有跨多个小时级别的时间乱序,第三批执行时累计大批次数据(batchSize * batchFactor) 后执行分组切分
        testOrderSendData(dt, 90, 6, new int[]{2, 2, 4, 4});

        DateTime dt2 = DateTime.parse("2021-01-26 23:30:00.123", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        //有跨多个小时级别的时间乱序,第三批执行时累计大批次数据(batchSize * batchFactor) 后执行分组切分
        //单小时分组超过batchSize再次切分
        testOrderSendData(dt2, 110, 5, new int[]{2, 2, 4, 6, 4});
    }

    /**
     * 测试批量发送数据
     *
     * @param dt 起始时间
     * @param numRecords 测试数据总数
     * @param timeInterval 测试数据时间间隔，单位:分钟
     * @param recordBatchNumArr 最终拆分出的总批次数量
     * @throws Exception ex
     */
    private void testOrderSendData(DateTime dt, int numRecords, int timeInterval, int[] recordBatchNumArr)
            throws Exception {

        BucketingFlinkKafkaProducer010 mockFlinkKafkaProducer = initBucketingFlinkKafkaProducer010();
        int batchRounds = 0;
        for (int i = 1; i <= numRecords; i++) {
            Row row = Row.of(dt.plusMinutes(i * timeInterval).toString("yyyy-MM-dd HH:mm:ss.SSS"), 1);
            mockFlinkKafkaProducer.putOutputValue(row);
            if (mockFlinkKafkaProducer.shouldSend()) {
                assertEquals(recordBatchNumArr[batchRounds++], mockFlinkKafkaProducer.getBatchRows().size());
            }
        }
    }

    private BucketingFlinkKafkaProducer010 initBucketingFlinkKafkaProducer010() {
        // init params
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", "");
        FlinkStreamingTopology topology = Mockito.mock(FlinkStreamingTopology.class);
        Mockito.when(topology.getRunMode()).thenReturn(RunMode.product.name());

        //prepare uc batch conf
        KafkaOutput output = new KafkaOutput("", 0);
        UCConfiguration conf = new UCConfiguration();
        conf.setProperty("uc.batch.size", UC_BATCH_SIZE);
        conf.setProperty("uc.batch.factor", UC_BATCH_FACTOR);
        output.setConf(conf);
        SinkNode node = Mockito.mock(SinkNode.class);
        Mockito.when(node.getOutput()).thenReturn(output);

        //mock producer
        String topicId = "";
        KeyedSerializationSchema<String> serializationSchema = Mockito.mock(KeyedSerializationSchema.class);
        AbstractAvroKeyedSerializationSchema costumAvroSchema =
                Mockito.mock(AbstractAvroKeyedSerializationSchema.class);
        AbstractFlinkStreamingCheckpointManager checkpointManager = Mockito.mock(
                AbstractFlinkStreamingCheckpointManager.class);
        BucketingFlinkKafkaProducer010 flinkKafkaProducer = new BucketingFlinkKafkaProducer010(
                topicId, serializationSchema, costumAvroSchema, producerConfig, node, topology, checkpointManager);
        BucketingFlinkKafkaProducer010 mockFlinkKafkaProducer = Mockito.mock(BucketingFlinkKafkaProducer010.class,
                Mockito.withSettings().spiedInstance(flinkKafkaProducer)
                        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                        .serializable());

        assertEquals(UC_BATCH_SIZE * UC_BATCH_FACTOR, mockFlinkKafkaProducer.batchProcessingSize());
        return mockFlinkKafkaProducer;
    }
}