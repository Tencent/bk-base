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

package com.tencent.bk.base.datahub.databus.connect.queue;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(QueueSinkTask.class);

    private QueueSinkConfig config;
    private Producer<String, String> producer;
    private SimpleDateFormat dateFormat;
    private String destTopic = "";
    private final AtomicBoolean needThrowException = new AtomicBoolean(false);


    /**
     * 通过解析配置，构建数据转换处理的对象，生成写入kafka的producer
     */
    @Override
    public void startTask() {
        config = new QueueSinkConfig(configProperties);
        dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        destTopic = config.prefix + rtId;
        // 创建kafka producer，获取相应的topic配置信息
        initProducer();
    }

    /**
     * 从kafka topic上获取到消息，通过数据转换，提取用户所需的数据记录，
     * 将数据记录打包，写到kafka queue topic中
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        if (needThrowException.get()) {
            throw new ConnectException(rtId + " got something wrong when sending msg to kafka, please check the log!");
        }

        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            final long now = System.currentTimeMillis() / 1000;  // 取秒
            final long tagTime = now / 60 * 60; // 取分钟对应的秒数
            String value = record.value().toString();
            String key = (record.key() == null) ? "" : record.key().toString();
            ConvertResult result = converter.getJsonList(key, value);

            // 将结果集发送到kafka中
            sendMsgToKafka(result.getJsonResult());

            markRecordProcessed(record); // 标记此条消息已被处理过
            msgCountTotal++;
            msgSizeTotal += value.length();

            // 上报打点数据
            try {
                Metric.getInstance().updateStat(config.rtId, config.connector, record.topic(), "queue",
                        result.getJsonResult().size(), value.length(), result.getTag(), tagTime + "");
                if (StringUtils.isNotBlank(result.getFailedResult())) {
                    List<String> errors = new ArrayList<>();
                    errors.add(result.getFailedResult());
                    Metric.getInstance().updateTopicErrInfo(config.connector, errors);
                }
                setDelayTime(result.getTagTime(), now);
                Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
            } catch (Exception ignore) {
                LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                        ignore.getMessage());
            }
        }

        // 重置延迟计时
        resetDelayTimeCounter();
    }

    /**
     * 停止运行，将一些使用到的资源清理掉。
     */
    @Override
    public void stopTask() {
        LogUtils.info(log, "{} Queue task going to stop!", rtId);
        if (producer != null) {
            producer.flush();
            producer.close(10, TimeUnit.SECONDS);
        }
    }

    /**
     * 初始化kafka producer,用于发送清洗后的结果数据
     */
    private void initProducer() {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, config.bootstrapServers);
            if (StringUtils.isBlank(config.bootstrapServers)) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                        "{} config error, no bootstrap.servers config found! producer config: {}", rtId, producerProps);
                throw new ConfigException(rtId + " config error!");
            }

            // 鉴权相关配置
            if (config.useSasl) {
                producerProps.put("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + config.saslUser
                                + "\" password=\"" + config.getDecryptionPassWord() + "\";");
                producerProps.put("security.protocol", "SASL_PLAINTEXT");
                producerProps.put("sasl.mechanism", "SCRAM-SHA-512");
                LogUtils.info(log, "use salsl config");
            } else {
                LogUtils.info(log, "use PLAINTEXT mode");
            }
            // 序列化配置
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    rtId + " failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 发送消息到目标kafka中
     *
     * @param results 转换后的数据结果集
     */
    private void sendMsgToKafka(List<String> results) {
        String dateStr = dateFormat.format(new Date(System.currentTimeMillis()));
        for (String result : results) {
            // 改为回调，打印异常日志
            producer.send(new ProducerRecord<>(destTopic, dateStr, result),
                    new ProducerCallback(destTopic, 0, dateStr, result, needThrowException));
        }
    }
}
