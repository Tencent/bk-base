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

package com.tencent.bk.base.datahub.hubmgr.service.kafka.offset;

public class ConsumerOffsetStat {

    private String kafka;
    private String group;
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;

    /**
     * 构造函数
     *
     * @param kafka kafka地址
     * @param group 消费组
     * @param topic 消费的队列名称
     * @param partition 队列的分区号码
     * @param offset 当前分区的offset
     * @param timestamp commit offset的时间戳（毫秒）
     */
    public ConsumerOffsetStat(String kafka, String group, String topic, int partition, long offset, long timestamp) {
        this.kafka = kafka;
        this.group = group;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * 更新offset和时间戳
     *
     * @param offset 当前分区的offset
     * @param timestamp commit offset的时间戳（毫秒）
     */
    public void updateOffsetAndTs(long offset, long timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * toString方法
     *
     * @return 包含此对象内容的字符串
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[kafka='").append(kafka).append("', ");
        sb.append("group='").append(group).append("', ");
        sb.append("topic='").append(topic).append("', ");
        sb.append("partition=").append(partition).append(", ");
        sb.append("offset=").append(offset).append(", ");
        sb.append("timestamp=").append(timestamp).append("]");

        return sb.toString();
    }

    // getter
    public String getKafka() {
        return kafka;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
