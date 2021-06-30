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
package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform;

import static org.junit.Assert.assertNotNull;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;


public class TransformResultTest {

    /**
     * 测试有参构造函数
     */
    @Test
    public void testConstructorHavePara() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic_1", 0, 0, "key1".getBytes(),
                "val1".getBytes());
        TransformResult result = new TransformResult();
        result.putRecord("test_bid", record);
        for (TransformRecord stringConsumerRecordEntry : result) {
            Assert.assertEquals("test_bid", stringConsumerRecordEntry.getBid());
            Assert.assertEquals(record, stringConsumerRecordEntry.getRecord());
        }
    }

    /**
     * 测试无参构造函数
     */
    @Test
    public void testConstructorNoPara() {
        TransformResult obj = new TransformResult();
        assertNotNull(obj);
    }

} 
