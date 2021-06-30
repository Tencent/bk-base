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

package com.tencent.bk.base.datahub.databus.commons.connector.source;

public class BkSourceRecord<T> {

    protected String key;
    protected T value;
    protected long valueLength;
    protected int partition;
    protected long recordSequence;

    private Runnable failFunction;
    private Runnable ackFunction;

    public BkSourceRecord(String key, T value, long valueLength) {
        this(key, value, valueLength, -1, -1);
    }

    public BkSourceRecord(String key, T value, long valueLength, int partition, long recordSequence) {
        this.key = key;
        this.value = value;
        this.valueLength = valueLength;
        this.partition = partition;
        this.recordSequence = recordSequence;
    }

    public void setFailFunction(Runnable failFunction) {
        this.failFunction = failFunction;
    }

    public void setAckFunction(Runnable ackFunction) {
        this.ackFunction = ackFunction;
    }

    /**
     * 消息确认
     */
    public final void ack() {
        if (null != ackFunction) {
            ackFunction.run();
        }
    }

    /**
     * 消息确认
     */
    public final void fail() {
        if (null != failFunction) {
            failFunction.run();
        }
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

    public long getValueLength() {
        return valueLength;
    }

    public int getPartition() {
        return partition;
    }

    public long getRecordSequence() {
        return recordSequence;
    }

    @Override
    public String toString() {
        return "Record{"
                + ", key="
                + key
                + ", value="
                + value
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

        BkSourceRecord that = (BkSourceRecord) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
