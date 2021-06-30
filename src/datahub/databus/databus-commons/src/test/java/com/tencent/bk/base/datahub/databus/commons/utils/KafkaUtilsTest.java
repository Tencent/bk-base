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

package com.tencent.bk.base.datahub.databus.commons.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Test;

public class KafkaUtilsTest {

    private static final String KafkaAddr = "127.0.0.1:9092";
    private static final String group = "test_group";

    @Test
    public void testInitStringConsumer() {
        KafkaConsumer<String, String> kafkaConsumer = KafkaUtils.initStringConsumer(group, KafkaAddr);
        Assert.assertNotNull(kafkaConsumer);
    }



    //@Test
    public void testConsumer() {
        // TODO create multi partition topic and produce some data
        String topic = "multi_partition";

        ConsumerRecords<String, String> records = KafkaUtils.getLastMessage(KafkaAddr, topic, group);
        Assert.assertTrue(records.count() > 0);
    }

    //@Test
    public void testConsumerEmptyTopic() {
        // TODO create empty topic first
        String topic = "empty";

        ConsumerRecords<String, String> records = KafkaUtils.getLastMessage(KafkaAddr, topic, group);
        Assert.assertEquals(0, records.count());
    }

    //@Test
    public void testConsumerNonExistTopic() {
        // TODO delete topic first
        String topic = "unknown";

        ConsumerRecords<String, String> records = KafkaUtils.getLastMessage(KafkaAddr, topic, group);
        Assert.assertEquals(0, records.count());
    }

}
