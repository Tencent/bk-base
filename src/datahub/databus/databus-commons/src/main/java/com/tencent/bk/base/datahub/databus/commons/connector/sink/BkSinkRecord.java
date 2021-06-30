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

package com.tencent.bk.base.datahub.databus.commons.connector.sink;

import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;

public class BkSinkRecord<T> extends BkSourceRecord<T> {

    private String topicName;

    public BkSinkRecord(String topicName, String key, T value, int partitionId, long recordSequence) {
        super(key, value, -1, partitionId, recordSequence);
        this.topicName = topicName;
    }

    public BkSinkRecord(String topicName, BkSourceRecord<T> sourceRecord) {
        this(topicName, sourceRecord.getKey(), sourceRecord.getValue(), sourceRecord.getPartition(),
                sourceRecord.getRecordSequence());
    }

    public String getTopicName() {
        return topicName;
    }


    @Override
    public String toString() {
        return "Record{"
                + "topic='"
                + topicName
                + '\''
                + ", partition="
                + partition
                + ", key="
                + key
                + ", value="
                + value
                + ", recordSequence="
                + recordSequence
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BkSinkRecord that = (BkSinkRecord) o;

        if (partition != (that.partition)) {
            return false;
        }
        if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }
        if (recordSequence != (that.recordSequence)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + (Integer.hashCode(partition));
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (Long.hashCode(recordSequence));
        return result;
    }
}
