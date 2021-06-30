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


package com.tencent.bk.base.datahub.databus.connect.jdbc.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * offset 工具类, 插入一条测试数据, key为用户指定的offset
 * jdbc connect启动的时候会读取数据的时候会从kafka最新的数据的key中读取offset.
 * 生效需要在connect停止后插入数据, 然后启动connect
 */
public class OffsetUtils {

    /**
     * 设置offset
     *
     * @param kafkaAddr kafka地址
     * @param topic topic
     * @param value key
     */
    public static void setIncrOffset(String kafkaAddr, String topic, String value) {
        setOffset(kafkaAddr, topic, "incrementing=" + value);
    }

    /**
     * 设置offset
     *
     * @param kafkaAddr kafka地址
     * @param topic topic
     * @param value key
     */
    public static void setTimeStampOffset(String kafkaAddr, String topic, String value) {
        setOffset(kafkaAddr, topic, "timestamp=" + value);
    }

    /**
     * 设置offset
     *
     * @param kafkaAddr kafka地址
     * @param topic topic
     * @param key key
     */
    private static void setOffset(String kafkaAddr, String topic, String key) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddr);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> p = new KafkaProducer<>(props);
        String value = "bkdata_reset_offset";
        ProducerRecord<String, String> pr = new ProducerRecord(topic, key, value);
        p.send(pr);
        p.flush();
        p.close();
    }

    /**
     * offset工具, 可以手动设置offset, 向kafka中插入一条数据, key为设置的offset
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            return;
        }
        String method = args[0];
        String kafka = args[1];
        String topic = args[2];
        String value = args[3];
        switch (method) {
            case "set_incr_offset":
                setIncrOffset(kafka, topic, value);
                break;
            case "set_ts_offset":
                setTimeStampOffset(kafka, topic, value);
                break;
            default:
        }
    }
}
