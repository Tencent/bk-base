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

package com.tencent.bk.base.datahub.iceberg.parser;

import java.util.List;
import org.apache.iceberg.data.Record;

public class ParseResult {

    private final List<Record> records;
    private final long tagTime;
    private final long dateTime;  // yyyyMMddHHmmss
    private final String metricTag;

    /**
     * 构造函数
     *
     * @param records iceberg record列表
     * @param tagTime tag time，时间戳
     * @param dateTime date time，时间数值，格式yyyyMMddHHmmss
     * @param metricTag metric tag，数据标签，用于追踪
     */
    public ParseResult(List<Record> records, long tagTime, long dateTime, String metricTag) {
        this.records = records;
        this.tagTime = tagTime;
        this.dateTime = dateTime;
        this.metricTag = metricTag;
    }

    /**
     * 获取记录列表
     *
     * @return 记录列表
     */
    public List<Record> getRecords() {
        return records;
    }

    /**
     * 获取tag time
     *
     * @return tag time
     */
    public long getTagTime() {
        return tagTime;
    }

    /**
     * 获取datetime
     *
     * @return date time，格式yyyyMMddHHmmss，数值
     */
    public long getDateTime() {
        return dateTime;
    }

    /**
     * 获取打点tag
     *
     * @return 打点tag
     */
    public String getMetricTag() {
        return metricTag;
    }

}