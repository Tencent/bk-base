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

package com.tencent.bk.base.dataflow.flink.streaming.source.kafka;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class BkKafka010Fetcher<T> extends Kafka010Fetcher<T> {

    private static final long serialVersionUID = 2324564345203409112L;

    protected volatile boolean stopped = false;
    protected volatile long stopTime = 0L;

    public BkKafka010Fetcher(Builder builder) throws Exception {
        super(
                builder.sourceContext,
                builder.assignedPartitionsWithInitialOffsets,
                builder.watermarksPeriodic,
                builder.watermarksPunctuated,
                builder.processingTimeProvider,
                builder.autoWatermarkInterval,
                builder.userCodeClassLoader,
                builder.taskNameWithSubtasks,
                builder.deserializer,
                builder.kafkaProperties,
                builder.pollTimeout,
                builder.subtaskMetricGroup,
                builder.consumerMetricGroup,
                builder.useMetrics);
    }

    @Override
    protected void emitRecord(T record, KafkaTopicPartitionState<TopicPartition> partition, long offset,
            ConsumerRecord<?, ?> consumerRecord) throws Exception {
        if (stopped) {
            // 如果是stop不emit record 到下游，也就不会更新partitionState，checkpoint和savepoint的数据都是已经emit的数据
            // 仅仅每条kafka记录过滤休眠1毫秒，不做关闭处理，简化stop功能代码修改逻辑。
            // stop之后的job必须进行cancel，因为数据已经丢弃了一部分。
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                // nothing
            }
            long now = System.currentTimeMillis();
            if (stopTime > 0 && (now - stopTime) > 120_000) {
                // 防止stop之后任务没有取消
                throw new IllegalAccessException(
                        "The job has been stopped for more than 120 seconds, The job must be canceled");
            }
        } else {
            super.emitRecord(record, partition, offset, consumerRecord);
        }
    }

    public void stop() {
        stopTime = System.currentTimeMillis();
        stopped = true;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private SourceFunction.SourceContext sourceContext;
        private Map assignedPartitionsWithInitialOffsets;
        private SerializedValue watermarksPeriodic;
        private SerializedValue watermarksPunctuated;
        private ProcessingTimeService processingTimeProvider;
        private long autoWatermarkInterval;
        private ClassLoader userCodeClassLoader;
        private String taskNameWithSubtasks;
        private KeyedDeserializationSchema deserializer;
        private Properties kafkaProperties;
        private long pollTimeout;
        private MetricGroup subtaskMetricGroup;
        private MetricGroup consumerMetricGroup;
        private boolean useMetrics;

        public Builder setSourceContext(SourceContext sourceContext) {
            this.sourceContext = sourceContext;
            return this;
        }

        public Builder setAssignedPartitions(Map assignedPartitionsWithInitialOffsets) {
            this.assignedPartitionsWithInitialOffsets = assignedPartitionsWithInitialOffsets;
            return this;
        }

        public Builder setWatermarksPeriodic(SerializedValue watermarksPeriodic) {
            this.watermarksPeriodic = watermarksPeriodic;
            return this;
        }

        public Builder setWatermarksPunctuated(SerializedValue watermarksPunctuated) {
            this.watermarksPunctuated = watermarksPunctuated;
            return this;
        }

        public Builder setProcessingTimeProvider(ProcessingTimeService processingTimeProvider) {
            this.processingTimeProvider = processingTimeProvider;
            return this;
        }

        public Builder setAutoWatermarkInterval(long autoWatermarkInterval) {
            this.autoWatermarkInterval = autoWatermarkInterval;
            return this;
        }

        public Builder setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
            this.userCodeClassLoader = userCodeClassLoader;
            return this;
        }

        public Builder setTaskNameWithSubtasks(String taskNameWithSubtasks) {
            this.taskNameWithSubtasks = taskNameWithSubtasks;
            return this;
        }

        public Builder setDeserializer(KeyedDeserializationSchema deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder setKafkaProperties(Properties kafkaProperties) {
            this.kafkaProperties = kafkaProperties;
            return this;
        }

        public Builder setPollTimeout(long pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        public Builder setSubtaskMetricGroup(MetricGroup subtaskMetricGroup) {
            this.subtaskMetricGroup = subtaskMetricGroup;
            return this;
        }

        public Builder setConsumerMetricGroup(MetricGroup consumerMetricGroup) {
            this.consumerMetricGroup = consumerMetricGroup;
            return this;
        }

        public Builder setUseMetrics(boolean useMetrics) {
            this.useMetrics = useMetrics;
            return this;
        }

        public BkKafka010Fetcher create() throws Exception {
            return new BkKafka010Fetcher(this);
        }
    }

}
