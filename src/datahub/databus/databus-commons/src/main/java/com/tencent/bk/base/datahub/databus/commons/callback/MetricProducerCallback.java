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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 用于记录kafka发送metric失败的场景
 * metric发送失败的消息要避免再次发送形成恶性循环
 */
public class MetricProducerCallback extends ProducerCallback {

    private static final Logger log = LoggerFactory.getLogger(MetricProducerCallback.class);

    public MetricProducerCallback(String topic, String key, String value) {
        super(topic, key, value);
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
            String msg = String
                    .format("Error when sending metric message to %s-%d with key: %s, value: %s", topic, partition, key,
                            value);
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL, log, msg, exception);
        }
    }

}
