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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.dataflow.core.configuration.UCConfiguration;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.kafka.producer.util.AbstractAvroKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.metric.MetricMapper;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
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
     * The time for refresh partition info.
     */
    private static final long REFRESH_PARTITION_INFO_INTERVAL_MS = 60000L;
    /**
     * The default maximum size of output data(currently 100).
     */
    private static final int DEFAULT_BATCH_SIZE = 100;
    /**
     * The default factor of batch processing.
     */
    private static final int DEFAULT_BATCH_FACTOR = 50;
    /**
     * Flag controlling whether we are writing the Flink record's timestamp into Kafka.
     */
    private transient ProcessingTimeService processingTimeService;
    private long lastSentToTime;
    private long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;
    private int batchSize;
    private int batchFactor;
    /**
     * false: processing data when `outputValues` more than batch size.
     * true: processing data when `outputValues` more than batch size * batch size factor.
     */
    private boolean isLargeBatch = false;
    /**
     * before processing batch have multi hours.
     */
    private boolean isBeforeBatchMultiHours = false;
    private long lastRefreshPartitionInfoTime = 0L;
    private BlockingQueue<Row> outputValues;

    private AbstractAvroKeyedSerializationSchema avroSchema;

    private MetricMapper metricMapper;
    private SinkNode node;
    private FlinkStreamingTopology topology;
    private String tagPrefix;

    private transient PersistedStateGuardMap persistedStateGuardMap;

    private AbstractFlinkStreamingCheckpointManager checkpointManager;

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
            AbstractAvroKeyedSerializationSchema costumAvroSchema,
            Properties producerConfig, SinkNode node, FlinkStreamingTopology topology,
            AbstractFlinkStreamingCheckpointManager checkpointManager) {
        this(topicId, serializationSchema, producerConfig, new FlinkFixedPartitioner<T>());
        UCConfiguration outputConf = node.getOutput().getConf();
        this.batchSize = outputConf.getInt("uc.batch.size", DEFAULT_BATCH_SIZE);
        this.batchFactor = outputConf.getInt("uc.batch.factor", DEFAULT_BATCH_FACTOR);
        this.avroSchema = costumAvroSchema;
        this.node = node;
        this.topology = topology;
        this.metricMapper = new MetricMapper(node, topology);
        this.checkpointManager = checkpointManager;
        this.outputValues = new ArrayBlockingQueue<>(this.batchProcessingSize());
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
     * Sets the default time between checks for inactive buckets.
     *
     * @param interval The timeout, in milliseconds.
     */
    public BucketingFlinkKafkaProducer010 setInactiveBucketCheckInterval(long interval) {
        this.inactiveBucketCheckInterval = interval;
        return this;
    }

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
        LOGGER.info("batchSize is {}, batchFactor is {}", batchSize, batchFactor);
        super.open(configuration);
        this.checkpointManager.open();

        this.persistedStateGuardMap = new PersistedStateGuardMap(topology, checkpointManager, node);

        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();

        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
        this.lastSentToTime = currentProcessingTime;

        // metric init
        this.metricMapper.realOpen(configuration, getRuntimeContext());
        tagPrefix = topology.getJobName() + "|" + node.getNodeId() + "|" + getRuntimeContext().getIndexOfThisSubtask()
                + "_";
        this.persistedStateGuardMap.initMetrix(this.getRuntimeContext());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErroneous();
        Row input = (Row) value;
        // 数据持久化
        Row persistedValue = persistedStateGuardMap.map(input);
        if (null != persistedValue) {
            // metric
            String tag = getFullTag();
            this.metricMapper.realMap(input, tag);
            // collect value to output values, 100 or 1s
            putOutputValue(persistedValue);
        }

        if (shouldSend()) {
            sendRecord();
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        sendRecordByTime(currentProcessingTime);

        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
    }

    @VisibleForTesting
    public boolean shouldSend() {
        boolean shouldPersist = false;
        if (isLargeBatchProcessing() || isMicroBatchProcessing()) {
            shouldPersist = true;
        }
        return shouldPersist;
    }

    private boolean isMicroBatchProcessing() {
        if (!isLargeBatch && outputValues.size() == this.batchSize) {
            return true;
        }
        return false;
    }

    private boolean isLargeBatchProcessing() {
        if (isLargeBatch && outputValues.size() == this.batchProcessingSize()) {
            return true;
        }
        return false;
    }

    private void sendRecordByTime(long currentProcessingTime) {
        if (lastSentToTime < currentProcessingTime - inactiveBucketCheckInterval && outputValues.size() > 0) {
            sendRecord();
        } else if (lastSentToTime < currentProcessingTime - inactiveBucketCheckInterval && outputValues.size() == 0) {
            // 需要保存一个end
            persistedStateGuardMap.saveCheckPointForWindowEnd();
        }
    }

    /**
     * Send record to kafka
     */
    private void sendRecord() {
        MessageHead messageHead = new MessageHead();
        String tag = getFullTag();
        messageHead.setTag(tag);

        // 获取按小时分组后根据批量大小切分的批次
        List<List<Row>> batchRows = getBatchRows();
        batchRows.stream().forEach(rows -> {
            Tuple2<MessageHead, List<Row>> tuple2 = new Tuple2<>(messageHead, rows);
            byte[] serializedKey = avroSchema.serializeKey(tuple2);
            byte[] serializedValue = avroSchema.serializeValue(tuple2);
            String targetTopic = avroSchema.getTargetTopic(tuple2);
            if (targetTopic == null) {
                throw new RuntimeException("Not found target topic.");
            }

            ProducerRecord<byte[], byte[]> record;
            if (partitionCounter == 0
                    && System.currentTimeMillis() - this.lastRefreshPartitionInfoTime
                            > REFRESH_PARTITION_INFO_INTERVAL_MS) {
                partitions = getPartitionsByTopic(targetTopic, producer);
                topicPartitionsMap.put(targetTopic, partitions);
                this.lastRefreshPartitionInfoTime = System.currentTimeMillis();
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
        });

        //save redis checkpoint
        persistedStateGuardMap.saveCheckPoint();

        lastSentToTime = processingTimeService.getCurrentProcessingTime();
    }

    /**
     * 将批量处理的数据按小时分组成为多个集合
     *
     * @return rows 数据集合
     */
    private Map<String, List<Row>> getKeyByHourRows() {
        Map<String, List<Row>> timedRows = Maps.newLinkedHashMap();
        while (!outputValues.isEmpty()) {
            Row row = outputValues.poll();
            if (null != row && row.getField(0) != null) {
                // yyyy-MM-dd HH:mm:ss，取前13位
                String dtEventTimeHourStr = row.getField(0).toString().substring(0, 13);
                List<Row> rows = timedRows.getOrDefault(dtEventTimeHourStr, Lists.newArrayList());
                rows.add(row);
                timedRows.put(dtEventTimeHourStr, rows);
            }
        }
        return timedRows;
    }

    /**
     * 将按小时分组的数据再次根据batchSize进行二次切分
     *
     * @return rows 数据集合
     */
    @VisibleForTesting
    public List<List<Row>> getBatchRows() {
        List<List<Row>> batchRow = Lists.newArrayList();
        Map<String, List<Row>> timedRows = getKeyByHourRows();
        timedRows.values().stream()
                .forEach(rows -> {
                    if (rows.size() > batchSize) {
                        List<List<Row>> partitionList = Lists.partition(rows, batchSize);
                        batchRow.addAll(partitionList);
                    } else {
                        batchRow.add(rows);
                    }
                });
        boolean isCurrentBatchMultiHours = timedRows.size() > 1;

        if (isIndeedUpgrade(isCurrentBatchMultiHours)) {
            upgradeToLargeBatch();
        } else {
            degradeToMicroBatch();
        }
        updateBeforeBatchMultiHours(isCurrentBatchMultiHours);

        return batchRow;
    }

    /**
     * 防止凌晨 23:59:59-00:00:00 时段正常跨天导致误升级,只有连续两批数据都是包含多个时段时，才进行升级
     *
     * @param isCurrentBatchMultiHours 当前批次的是否包含不同小时的数据
     * @return 是否需要升级为大批次处理
     */
    private boolean isIndeedUpgrade(boolean isCurrentBatchMultiHours) {
        if (isCurrentBatchMultiHours && isBeforeBatchMultiHours) {
            return true;
        }
        return false;
    }

    /**
     * 升级为大批次处理
     */
    private void upgradeToLargeBatch() {
        this.isLargeBatch = true;
    }

    /**
     * 降级为小批次处理
     */
    private void degradeToMicroBatch() {
        this.isLargeBatch = false;
    }

    private void updateBeforeBatchMultiHours(boolean isCurrentBatchMultiHours) {
        this.isBeforeBatchMultiHours = isCurrentBatchMultiHours;
    }

    @Override
    public void flush() {
        sendRecord();
        // The super flush method must be invoked at the end,
        // because super method makes all buffered records immediately available to send.
        super.flush();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (persistedStateGuardMap != null) {
            persistedStateGuardMap.destroyRedisPool();
        }
    }

    /**
     * 获取包含当前时间信息的完整的Tag
     *
     * @return tag full tag
     */
    private String getFullTag() {
        String tag = tagPrefix + System.currentTimeMillis() / 60000 * 60;
        return tag;
    }

    /**
     * 批量处理数据大小
     *
     * @return 批量大小
     */
    @VisibleForTesting
    public int batchProcessingSize() {
        return batchSize * batchFactor;
    }

    @VisibleForTesting
    public void putOutputValue(Row row) throws Exception {
        outputValues.put(row);
    }

}