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

package com.tencent.bk.base.datahub.databus.commons.callback;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用于记录kafka发送失败的场景
 */
public class ProducerCallback implements Callback {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class);

    // kafka发送消息的参数
    protected String topic;
    protected int partition = 0;
    protected String key = "";
    protected String value = "";
    private AtomicBoolean raiseException = null;

    /**
     * 构造函数
     *
     * @param topic kafka topic名称
     * @param key kafka消息的msg key
     * @param value kafka消息的msg value
     */
    public ProducerCallback(String topic, String key, String value) {
        this(topic, 0, key, value, null);
    }

    /**
     * 构造函数
     *
     * @param topic kafka topic名称
     * @param partition kafka topic的分区编号
     * @param key kafka消息的msg key
     * @param value kafka消息的msg value
     */
    public ProducerCallback(String topic, int partition, String key, String value) {
        this(topic, partition, key, value, null);
    }

    /**
     * 构造函数
     *
     * @param topic kafka topic名称
     * @param partition kafka topic的分区编号
     * @param key kafka消息的msg key，字节数组
     * @param value kafka消息的msg value，字节数组
     */
    public ProducerCallback(String topic, int partition, byte[] key, byte[] value) {
        this(topic, partition, new String(key, StandardCharsets.ISO_8859_1),
                new String(value, StandardCharsets.ISO_8859_1), null);
    }

    /**
     * 构造函数
     *
     * @param topic kafka topic名称
     * @param partition kafka topic的分区编号
     * @param key kafka消息的msg key
     * @param value kafka消息的msg value
     * @param raiseException 是否抛出异常
     */
    public ProducerCallback(String topic, int partition, String key, String value, AtomicBoolean raiseException) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.raiseException = raiseException;
    }

    /**
     * 发送消息完成后的回调函数
     *
     * @param metadata 消息record的元数据信息
     * @param exception 异常
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL, log,
                    String.format("Error when sending message to %s-%d with key: %s, value: %s", topic, partition, key,
                            value), exception);
            // 标记出现异常
            if (raiseException != null) {
                raiseException.set(true);
            }
        }
    }
}

