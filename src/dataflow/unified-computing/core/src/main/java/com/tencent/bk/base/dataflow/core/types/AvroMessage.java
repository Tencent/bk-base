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

package com.tencent.bk.base.dataflow.core.types;

import java.io.Serializable;

public class AvroMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;
    private int partition;
    private long offset;

    private byte[] message;
    private byte[] key;

    public AvroMessage(String topic, int partition, long offset, byte[] key, byte[] message) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.message = message;
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

    public byte[] getMessage() {
        return message;
    }

    public byte[] getKey() {
        return key;
    }

    /**
     * bkdata_par_offset字段内容变更： [avroIndex]_[partition]_[offset]_[topic]
     *
     * @param avroIndex message所在avro包中的index
     * @return
     */
    public String getBkdataPartitionOffsetField(int avroIndex) {
        return avroIndex + "_" + partition + '_' + offset + '_' + topic;
    }

    @Override
    public String toString() {
        return String.format("topic %s partition %d offset %d", topic, partition, offset);
    }
}
