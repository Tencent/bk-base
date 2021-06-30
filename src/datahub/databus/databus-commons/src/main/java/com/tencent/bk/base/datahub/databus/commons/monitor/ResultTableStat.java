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

package com.tencent.bk.base.datahub.databus.commons.monitor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class ResultTableStat {

    private String rtId;
    private String connector;
    private String topic;
    private String clusterType;
    private AtomicLong count;
    private AtomicLong totalCount;
    private boolean useMsgCount;

    private AtomicLong msgSize;
    private Map<String, AtomicInteger> inputTagsCount;
    private Map<String, AtomicInteger> outputTagsCount;
    private Map<String, AtomicInteger> errCounts;

    //数据延迟
    private long delayNow = 0; //墙上时间
    private long delayMaxTime = 0; //最大延迟的数据时间
    private long delayMinTime = 0; //最小延迟的数据时间

    ResultTableStat(String rtId, String connector, String topic, String clusterType) {
        this.rtId = rtId;
        this.connector = connector;
        this.topic = topic;
        this.clusterType = clusterType;
        this.count = new AtomicLong(0);
        this.totalCount = new AtomicLong(0);
        this.msgSize = new AtomicLong(0);
        // 对于直接消费outer kafka的任务，使用kafka消息数量作为计数器
        this.useMsgCount = connector.startsWith("clean-") || connector.startsWith("eslog-");
        inputTagsCount = new ConcurrentHashMap<>();
        outputTagsCount = new ConcurrentHashMap<>();
        errCounts = new ConcurrentHashMap<>();
    }

    /**
     * 记录此次处理的数据条数，以及输入的tag和输出的tag的计数器（一个上报周期内的）。
     *
     * @param cnt 处理数据条数
     * @param size 处理字节数
     * @param inputTag 输入tag
     * @param outTag 输出tag
     */
    void processedRecords(long cnt, long size, String inputTag, String outTag) {
        totalCount.addAndGet(cnt);
        msgSize.addAndGet(size);
        count.incrementAndGet();

        // input tag或output tag非空时才统计
        if (!inputTag.isEmpty()) {
            if (useMsgCount) {
                if (inputTagsCount.containsKey(inputTag)) {
                    inputTagsCount.get(inputTag).incrementAndGet();
                } else {
                    inputTagsCount.put(inputTag, new AtomicInteger(1));
                }
            } else {
                if (inputTagsCount.containsKey(inputTag)) {
                    inputTagsCount.get(inputTag).addAndGet((int) cnt);
                } else {
                    inputTagsCount.put(inputTag, new AtomicInteger((int) cnt));
                }
            }
        }

        if (!outTag.isEmpty()) {
            if (outputTagsCount.containsKey(outTag)) {
                outputTagsCount.get(outTag).addAndGet((int) cnt);
            } else {
                outputTagsCount.put(outTag, new AtomicInteger((int) cnt));
            }
        }
    }

    /**
     * 更新此topic的错误信息计数器
     */
    void updateErrorCount(List<String> errors) {
        for (String error : errors) {
            if (errCounts.containsKey(error)) {
                errCounts.get(error).incrementAndGet();
            } else {
                errCounts.put(error, new AtomicInteger(1));
            }
        }
    }

    /**
     * 更新数据延迟数据
     * 需要注意的是，这里不做比较了，只是单纯的覆盖就好,按照打点数据的频率上报，就当抽样了
     */
    void updateDelayCount(long now, long maxDelayTime, long minDelayTime) {
        if (maxDelayTime == 0) {
            return;
        }
        this.delayNow = now;
        this.delayMaxTime = maxDelayTime;
        this.delayMinTime = minDelayTime;
    }

    /**
     * 获取这个result_table的名称
     *
     * @return result_table的名称
     */
    String getRtId() {
        return rtId;
    }

    /**
     * 获取connector名称
     *
     * @return connector名称
     */
    String getConnector() {
        return connector;
    }

    /**
     * 获取kafka topic名称
     *
     * @return topic名称
     */
    String getTopic() {
        return topic;
    }

    /**
     * 获取存储集群类型
     *
     * @return clusterType的值
     */
    String getClusterType() {
        return clusterType;
    }

    /**
     * 获取和这个result_table相关的总处理消息条数（kafka msg count）。
     *
     * @return 总处理消息条数
     */
    long getCount() {
        return count.get();
    }

    long getTotalCount() {
        return totalCount.get();
    }

    long getMsgSize() {
        return msgSize.get();
    }

    long getDelayNow() {
        return delayNow;
    }

    long getDelayMaxTime() {
        return delayMaxTime;
    }

    long getDelayMinTime() {
        return delayMinTime;
    }

    /**
     * 获取一个上报周期内的输入的tagTime计数器。
     *
     * @return 一个上报周期内的输入tagTime统计数据
     */
    Map<String, AtomicInteger> getInputTagsCount() {
        return inputTagsCount;
    }

    /**
     * 获取一个上报周期内的输出的tagTime计数器。
     *
     * @return 一个上报周期内输出的tagTime统计数据
     */
    Map<String, AtomicInteger> getOutputTagsCount() {
        return outputTagsCount;
    }

    /**
     * 获取一个上报周期内的错误数量。
     *
     * @return 一个周期内的错误记录数量
     */
    Map<String, AtomicInteger> getErrCounts() {
        return errCounts;
    }
}
