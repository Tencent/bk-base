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

import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.kafka.producer.util.AbstractAvroKeyedSerializationSchema;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nullable;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.10.x
 */
@PublicEvolving
public class BucketingFlinkKafkaProducer010<T> extends FlinkKafkaProducer09<T> implements ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketingFlinkKafkaProducer010.class);

    /**
     * The default time between checks for inactive buckets. By default, {1 sec}.
     */
    private static final long DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS = 1000L;

    /**
     * The default maximum size of output data(currently 100).
     */
    private static final int DEFAULT_BATCH_SIZE = 100;

    private static final String dtEventTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    /**
     * Flag controlling whether we are writing the Flink record's timestamp into Kafka.
     */
    private transient ProcessingTimeService processingTimeService;

    private long lastSentToTime;

    private long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;
    private int batchSize = DEFAULT_BATCH_SIZE;

    private BlockingQueue<Row> outputValues;

    private AbstractAvroKeyedSerializationSchema avroSchema;

    private SinkNode node;
    private Topology topology;
    private String tagPrefix;

    private int partitionCounter = 0;

    private int[] partitions;

    /**
     * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
     * the topic.
     *
     * Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
     * the partitioner. This default partitioner maps each sink subtask to a single Kafka
     * partition (i.e. all records received by a sink subtask will end up in the same
     * Kafka partition).
     *
     * To use a custom partitioner, please use
     * {@link #BucketingFlinkKafkaProducer010(String, KeyedSerializationSchema, Properties, FlinkKafkaPartitioner)}
     * instead.
     *
     * @param topicId ID of the Kafka topic.
     * @param serializationSchema User defined serialization schema supporting key/value messages
     * @param producerConfig Properties with the producer configuration.
     */
    public BucketingFlinkKafkaProducer010(String topicId, KeyedSerializationSchema<T> serializationSchema,
            AbstractAvroKeyedSerializationSchema customAvroSchema,
            Properties producerConfig, SinkNode node, Topology topology) {
        this(topicId, serializationSchema, producerConfig, new FlinkFixedPartitioner<T>());

        this.avroSchema = customAvroSchema;
        this.node = node;
        this.topology = topology;
    }

    /**
     * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
     * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link
     * FlinkKafkaPartitioner}.
     *
     * If a partitioner is not provided, written records will be partitioned by the attached key of each
     * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
     * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
     * will be distributed to Kafka partitions in a round-robin fashion.
     *
     * @param topicId The topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into a
     *         kafka-consumable byte[] supporting key/value messages
     * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only
     *         required argument.
     * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
     *         If set to {@code null}, records will be partitioned by the key of each record
     *         (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
     *         are {@code null}, then records will be distributed to Kafka partitions in a
     *         round-robin fashion.
     */

    public BucketingFlinkKafkaProducer010(
            String topicId,
            KeyedSerializationSchema<T> serializationSchema,
            Properties producerConfig,
            @Nullable FlinkKafkaPartitioner<T> customPartitioner) {

        super(topicId, serializationSchema, producerConfig, customPartitioner);
    }

    // ------------------- User configuration ----------------------

    /**
     * Sets the maximum data size one send.
     *
     * @param batchSize The bucket data size
     */
    public BucketingFlinkKafkaProducer010 setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void open(Configuration configuration) {
        LOGGER.info("Initialize kafka sink procedueer for node {}", this.node.getNodeId());
        super.open(configuration);

        this.outputValues = new ArrayBlockingQueue<>(200);

        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();

        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval,
                this);
        this.lastSentToTime = currentProcessingTime;

        // metric init
        tagPrefix = topology.getJobName() + "|" + node.getNodeId() + "|" + getRuntimeContext().getIndexOfThisSubtask()
                + "_";
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErroneous();
        Row input = (Row) value;
        String tag = tagPrefix + System.currentTimeMillis() / 60000 * 60;

        // collect value to output values, 100 or 1s
        outputValues.put(input);

        if (shouldSend()) {
            sendRecord(tag);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        sendRecordByTime(currentProcessingTime);

        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
    }

    private boolean shouldSend() {
        boolean shouldPersist = false;

        if (outputValues.size() >= batchSize) {
            shouldPersist = true;
        }
        return shouldPersist;
    }

    private void sendRecordByTime(long currentProcessingTime) {
        if (lastSentToTime < currentProcessingTime - inactiveBucketCheckInterval && outputValues.size() > 0) {
            String tag = tagPrefix + System.currentTimeMillis() / 60000 * 60;
            sendRecord(tag);
        }
    }

    /**
     * Send record to kafka
     *
     * @param tag metric tag
     */
    private void sendRecord(String tag) {
        MessageHead messageHead = new MessageHead();
        messageHead.setTag(tag);

        // 获取相同分钟记录组成一个批次
        List<Row> rows = null;
        while ((rows = getSameMinuteRows()) != null && !rows.isEmpty()) {
            Tuple2<MessageHead, List<Row>> tuple2 = new Tuple2<>(messageHead, rows);
            byte[] serializedKey = avroSchema.serializeKey(tuple2);
            byte[] serializedValue = avroSchema.serializeValue(tuple2);
            String targetTopic = avroSchema.getTargetTopic(tuple2);
            if (targetTopic == null) {
                throw new RuntimeException("Not found target topic.");
            }

            ProducerRecord<byte[], byte[]> record;
            if (partitionCounter == 0) {
                partitions = getPartitionsByTopic(targetTopic, producer);
                topicPartitionsMap.put(targetTopic, partitions);
            }

            record = new ProducerRecord<>(targetTopic, partitionCounter, null, serializedKey, serializedValue);

            if (flushOnCheckpoint) {
                synchronized (pendingRecordsLock) {
                    pendingRecords++;
                }
            }
            producer.send(record, callback);

            if (partitionCounter < partitions.length - 1) {
                // if partition counter is less than partitions's length, plus one
                partitionCounter++;
            } else {
                // else reset to zero
                partitionCounter = 0;
            }
        }

        lastSentToTime = processingTimeService.getCurrentProcessingTime();
    }

    /**
     * 获取相同分钟记录组成一个批次
     *
     * @return
     */
    private List<Row> getSameMinuteRows() {
        List<Row> rows = new ArrayList<>();
        long lastDtEventTimeStampMinute = 0;
        while (rows.size() < batchSize && !outputValues.isEmpty()) {
            Row row = outputValues.peek();
            if (null != row && row.getField(0) != null) {
                long dtEventTimeStampMinute = 0;
                try {
                    FastDateFormat utcFormat = FastDateFormat.getInstance(dtEventTimeFormat, utcTimeZone);
                    // 取分钟
                    dtEventTimeStampMinute = utcFormat.parse(row.getField(0).toString()).getTime() / 60;
                    if (0 != lastDtEventTimeStampMinute && dtEventTimeStampMinute != lastDtEventTimeStampMinute) {
                        // 不是同一分钟
                        break;
                    }
                } catch (ParseException e) {
                    // e.printStackTrace();
                }
                lastDtEventTimeStampMinute = dtEventTimeStampMinute;
            }

            outputValues.poll();
            rows.add(row);
        }
        return rows;
    }

    @Override
    public void flush() {
        String tag = tagPrefix + System.currentTimeMillis() / 60000 * 60;
        sendRecord(tag);
        // The super flush method must be invoked at the end,
        // because super method makes all buffered records immediately available to send.
        super.flush();
    }

}

