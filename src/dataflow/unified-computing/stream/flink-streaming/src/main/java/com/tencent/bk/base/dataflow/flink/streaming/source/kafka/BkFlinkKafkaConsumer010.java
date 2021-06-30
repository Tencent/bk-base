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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class BkFlinkKafkaConsumer010<T> extends FlinkKafkaConsumer010<T> implements StoppableFunction {

    private static final long serialVersionUID = 2324564345203409112L;
    private static final Logger LOG = LoggerFactory.getLogger(BkFlinkKafkaConsumer010.class);

    private transient volatile BkKafka010Fetcher bkKafka010Fetcher;

    public BkFlinkKafkaConsumer010(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
    }

    public BkFlinkKafkaConsumer010(String topic, KeyedDeserializationSchema<T> deserializer, Properties props) {
        super(topic, deserializer, props);
    }

    public BkFlinkKafkaConsumer010(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    public BkFlinkKafkaConsumer010(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    public BkFlinkKafkaConsumer010(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer,
            Properties props) {
        super(subscriptionPattern, valueDeserializer, props);
    }

    public BkFlinkKafkaConsumer010(Pattern subscriptionPattern, KeyedDeserializationSchema<T> deserializer,
            Properties props) {
        super(subscriptionPattern, deserializer, props);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode, MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        // 保存持有Fetcher对象，供stop使用
        bkKafka010Fetcher = BkKafka010Fetcher.builder()
                .setSourceContext(sourceContext)
                .setAssignedPartitions(assignedPartitionsWithInitialOffsets)
                .setWatermarksPeriodic(watermarksPeriodic)
                .setWatermarksPunctuated(watermarksPunctuated)
                .setProcessingTimeProvider(runtimeContext.getProcessingTimeService())
                .setAutoWatermarkInterval(runtimeContext.getExecutionConfig().getAutoWatermarkInterval())
                .setUserCodeClassLoader(runtimeContext.getUserCodeClassLoader())
                .setTaskNameWithSubtasks(runtimeContext.getTaskNameWithSubtasks())
                .setDeserializer(this.deserializer)
                .setKafkaProperties(this.properties)
                .setPollTimeout(this.pollTimeout)
                .setSubtaskMetricGroup(runtimeContext.getMetricGroup())
                .setConsumerMetricGroup(consumerMetricGroup)
                .setUseMetrics(useMetrics)
                .create();

        return bkKafka010Fetcher;
    }

    @Override
    public void stop() {
        if (null != bkKafka010Fetcher) {
            LOG.warn("stop bkKafka010Fetcher.");
            bkKafka010Fetcher.stop();
        }
    }
}
