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

package com.tencent.bk.base.datahub.databus.commons.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterStatBean {

    // bean的属性
    private long msgCount;
    private long msgSize;
    private long recordCount;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int percent;
    private long time;

    /**
     * 构造函数
     */
    public ClusterStatBean() {
        percent = 0;
        time = System.currentTimeMillis() / 1000; // 时间戳，秒
    }

    /**
     * 构造函数
     *
     * @param msgCount 消息数量
     * @param msgSize 消息字节数
     * @param recordCount 消息中包含的数据条数
     * @param totalMemory 目前分配总内存字节数
     * @param freeMemory 目前空闲内存字节数
     * @param maxMemory 最大可分配总内存字节数
     */
    public ClusterStatBean(long msgCount, long msgSize, long recordCount, long totalMemory, long freeMemory,
            long maxMemory) {
        this.msgCount = msgCount;
        this.msgSize = msgSize;
        this.recordCount = recordCount;
        this.totalMemory = totalMemory;
        this.freeMemory = freeMemory;
        this.maxMemory = maxMemory;
        this.percent = maxMemory > 0 ? (int) ((maxMemory - totalMemory + freeMemory) * 100 / maxMemory) : 0;
        time = System.currentTimeMillis() / 1000; // 时间戳，秒
    }

    // getter & setter


    @JsonProperty("msg_count")
    public long getMsgCount() {
        return msgCount;
    }

    public void setMsgCount(long msgCount) {
        this.msgCount = msgCount;
    }

    @JsonProperty("msg_size")
    public long getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(long msgSize) {
        this.msgSize = msgSize;
    }

    @JsonProperty("record_count")
    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    @JsonProperty("total_memory")
    public long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

    @JsonProperty("free_memory")
    public long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }

    @JsonProperty("max_memory")
    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    @JsonProperty("available_memory_percent")
    public int getPercent() {
        return percent;
    }

    public void setPercent(int percent) {
        this.percent = percent;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }


    /**
     * toString方法
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return "[msg_count: " + msgCount + ", msg_size: " + msgSize + ", record_count: " + recordCount
                + ", total_memory: " + totalMemory + ", free_memory: " + freeMemory + ", max_memory: " + maxMemory
                + ", available_memory_percent: " + percent + "%, time: " + time + "]";
    }

    /**
     * 转换为tsdb中的fields所需的字符串
     *
     * @return 转换为tsdb fields字符串
     */
    public String toTsdbFields() {
        return String
                .format("msg_count=%si,msg_size=%si,record_count=%si,total_memory=%si,free_memory=%si,max_memory=%si,"
                                + "available_memory_percent=%si",
                        msgCount, msgSize, recordCount, totalMemory, freeMemory, maxMemory, percent);
    }
}
