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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public class ParsedMsg {

    private ConvertResult result;
    private String key;
    private String value;
    private long offset;
    private int partition;
    private String topic;
    private int count;

    public ParsedMsg(SinkRecord record, Converter converter) {
        value = record.value().toString();
        key = (record.key() == null) ? "" : record.key().toString();
        offset = record.kafkaOffset();
        partition = record.kafkaPartition();
        topic = record.topic();
        result = converter.getAvroArrayResultExtendThedate(key, value);
        count = result.getAvroValues() == null ? 0 : result.getAvroValues().size();
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean getIsDatabusEvent() {
        return result.getIsDatabusEvent();
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

    public List<String> getJsonResult() {
        return result.getJsonResult();
    }

    public String getFailedResult() {
        return result.getFailedResult();
    }

    public List<String> getErrors() {
        return result.getErrors();
    }

    public GenericArray<GenericRecord> getAvroValues() {
        return result.getAvroValues();
    }

    public String getTag() {
        return result.getTag();
    }

    public long getTagTime() {
        return result.getTagTime();
    }

    public int getMsgSize() {
        return result.getMsgSize();
    }

    public long getDateTime() {
        return result.getDateTime();
    }

    public int getCount() {
        return count;
    }
}
