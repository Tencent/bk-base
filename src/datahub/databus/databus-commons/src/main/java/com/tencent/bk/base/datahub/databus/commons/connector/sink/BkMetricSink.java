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

import com.tencent.bk.base.datahub.databus.commons.BkDatabusContext;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.DataBusResult;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.EtlConverter;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.ZkUtils;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BkMetricSink<INPUT, OUTPUT> implements BkSink<INPUT, OUTPUT>, TaskContextChangeCallback {

    private static final Logger log = LoggerFactory.getLogger(BkMetricSink.class);
    protected static final int OFFSET_SET_CAPACITY = 100;

    protected AtomicBoolean isStop = new AtomicBoolean(false);
    protected Map<Integer, Long> processedOffsets = new HashMap<>();
    protected Set<String> oldPartitionOffsets = new LinkedHashSet<>(OFFSET_SET_CAPACITY);
    protected boolean skipProcessEmptyRecords = true;
    protected BkDatabusContext context;

    protected long msgCountTotal = 0;
    protected long msgSizeTotal = 0;

    protected long start = 0;
    protected long lastCheckTime;
    protected long lastLogTime;
    protected long lastLogCount = 0;

    //本批次(一次poll)中数据时间延至最大和最小的时间戳
    protected long maxDelayTime = 0;
    protected long minDelayTime = 0;

    @Override
    public final void open(final BkDatabusContext context) throws Exception {
        this.context = context;
        Thread.currentThread().setName(context.getName());    // 将任务名称设置到当前线程上，便于上报数据
        lastCheckTime = lastLogTime = start = System.currentTimeMillis();
        // 调用子类的startTask方法
        doOpen(context);
        Metric.getInstance().reportEvent(this.context.getName(), Consts.TASK_START_SUCC, "", "");
        // 注册回调函数，当rt配置发生变化时，会将isTaskContextChanged设置为true
        ZkUtils.registerCtxChangeCallback(this.context.getRtId(), this);
    }


    @Override
    public final DataBusResult<OUTPUT> write(BkSinkRecord<INPUT> record) throws Exception {
        if (skipProcessEmptyRecords && (record == null || record.getValue() == null)) {
            return null;
        }
        LogUtils.debug(log, "{} got {} record to process", this.context.getRtId(), record);
        if (System.currentTimeMillis() - lastCheckTime > 900000) { // 每15分钟刷新一次
            LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", msgCountTotal, msgSizeTotal,
                    this.context.getRtId());
            lastCheckTime = System.currentTimeMillis();
        }
        // 控制下日志输出频率，十秒内最多输出一次
        if (msgCountTotal - lastLogCount >= 5000 && System.currentTimeMillis() - lastLogTime >= 10000) {
            LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", msgCountTotal, msgSizeTotal,
                    this.context.getRtId());
            lastLogTime = System.currentTimeMillis();
            lastLogCount = msgCountTotal;
        }

        // 实际的数据处理逻辑在子类中实现
        DataBusResult result;
        try {
            result = doWrite(record);
            msgCountTotal++;
            if (null != result) {
                try {
                    long msgSize = result.getOutputMessageSize();
                    msgSizeTotal += msgSize;
                    ConvertResult convertResult = result.getConvertResult();
                    final long now = System.currentTimeMillis() / 1000; // 取秒
                    final long tagTime = now / 60 * 60; // 取分钟对应的秒数
                    Metric.getInstance()
                            .updateStat(context.getRtId(), context.getName(), record.getTopicName(),
                                    this.getClusterType(),
                                    result.getOutputMessageCount(), msgSize, convertResult.getTag(),
                                    this.getMetricTag(tagTime));
                    setDelayTime(convertResult.getTagTime(), now);
                    Metric.getInstance().updateDelayInfo(context.getName(), now, maxDelayTime, minDelayTime);
                    if (convertResult.getErrors().size() > 0) {
                        Metric.getInstance().updateTopicErrInfo(context.getName(), convertResult.getErrors());
                        if (context.getConverter() instanceof EtlConverter) {
                            // 上报清洗失败的异常数据
                            Metric.getInstance().setBadEtlMsg(context.getRtId(), context.getDataId(), record.getKey(),
                                    context.getTransfer().getMsgStr(), record.getPartition(),
                                    record.getRecordSequence(),
                                    convertResult.getFailedResult(), now);
                        }
                    }
                } catch (Exception e) {
                    LogUtils.warn(log, "Failed to update stat info for {}, just ignore this. {}",
                            this.context.getRtId(),
                            e.getMessage());
                } finally {
                    resetDelayTimeCounter();
                }
            }

        } catch (Exception e) {
            // TODO 记录error信息
            if (!(e instanceof WakeupException || e instanceof CommitFailedException)) {
                Metric.getInstance()
                        .reportEvent(this.context.getName(), Consts.TASK_RUN_FAIL, ExceptionUtils.getStackTrace(e),
                                e.getMessage());
            }
            throw e;
        }
        return result;
    }

    @Override
    public final void close() throws Exception {
        isStop.set(true);
        try {
            doClose();
            if (null == this.context) {
                return;
            }
            if (StringUtils.isNotBlank(this.context.getRtId())) {
                ZkUtils.removeCtxChangeCallback(this.context.getRtId(), this);
            }
        } catch (Exception e) {
            Metric.getInstance()
                    .reportEvent(this.context.getName(), Consts.TASK_STOP_FAIL, ExceptionUtils.getStackTrace(e),
                            e.getMessage());
            throw e;
        }
        long duration = System.currentTimeMillis() - start;
        LogUtils.info(log, "processed {} msgs, {} bytes in {} (ms) for result table {}", msgCountTotal, msgSizeTotal,
                duration, this.context.getRtId());
        Metric.getInstance().reportEvent(this.context.getName(), Consts.TASK_STOP_SUCC, "",
                String.format("task run for %s ms", duration));
    }


    /**
     * 启动task
     */
    protected abstract void doOpen(final BkDatabusContext context);

    /**
     * 处理kafka中的记录
     *
     * @param record 从kafka/pulsar中获取的记录
     */
    protected abstract DataBusResult<OUTPUT> doWrite(BkSinkRecord<INPUT> record);

    /**
     * 停止task
     */
    protected void doClose() {
        // do nothing
    }

    /**
     * task的rt配置发生变化，在本方法中仅仅记录此变化，如果需要监听配置变化， 需要在子类方法里检查isTaskContextChanged的值，实现自身的逻辑
     */
    public void markBkTaskCtxChanged() {
        LogUtils.info(log, "{} task rt config is changed, going to reload rt config!", this.context.getRtId());
        this.context.refreshContext();
    }

    /**
     * 重置延迟计数
     */
    protected void resetDelayTimeCounter() {
        this.maxDelayTime = 0;
        this.minDelayTime = 0;
    }

    /**
     * 根据数据时间和墙上时间进行比较，找到本批次延迟最大和最小的值
     */
    protected void setDelayTime(long dataTimestamp, long now) {
        if (now - dataTimestamp < 0 || dataTimestamp == 0) {
            return; //数据时间比当前时间还大，或者数据时间为0的情况不考虑
        }
        maxDelayTime = (maxDelayTime == 0 || dataTimestamp < maxDelayTime) ? dataTimestamp : maxDelayTime;
        minDelayTime = (minDelayTime == 0 || dataTimestamp > minDelayTime) ? dataTimestamp : minDelayTime;
    }

    /**
     * 判断此kafka消息是否曾经被处理过，是旧消息
     *
     * @param record kafka消息记录
     * @return true/false
     */
    protected boolean isRecordProcessed(BkSinkRecord<INPUT> record) {
        // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
        if (processedOffsets.containsKey(record.getPartition()) && record.getRecordSequence() <= processedOffsets
                .get(record.getPartition())) {
            if (oldPartitionOffsets.size() == 0) {
                // 判断此条msg是否已经处理过，如果已经处理过，则跳过。每个周期出现第一条旧数据时打印日志。
                LogUtils.warn(log, "{} {}-{} got old record with offset {} less than processed offset {}",
                        this.context.getRtId(), record.getTopicName(), record.getPartition(),
                        record.getRecordSequence(), processedOffsets.get(record.getPartition()));
            }
            if (oldPartitionOffsets.size() < OFFSET_SET_CAPACITY) {
                // 避免此集合过大，占用太多内存
                oldPartitionOffsets.add(record.getPartition() + "-" + record.getRecordSequence());
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 记录处理过的kafka消息
     *
     * @param record kafka消息记录
     */
    protected void markRecordProcessed(BkSinkRecord<INPUT> record) {
        // 更新处理过的offset信息
        processedOffsets.put(record.getPartition(), record.getRecordSequence());
    }

    /**
     * 打印日志记录此任务收到的重复kafka消息记录
     */
    private void logRepeatRecords() {
        if (oldPartitionOffsets.size() > 0) {
            LogUtils.warn(log, "{} got old record with offset {} ... in last checking loop", this.context.getRtId(),
                    StringUtils.join(oldPartitionOffsets, ","));
            oldPartitionOffsets.clear();
        }
    }

    protected abstract String getClusterType();

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    protected String getMetricTag(long tagTime) {
        if (context.getConverter() instanceof EtlConverter) {
            return context.getName() + "|" + tagTime;
        }
        return String.valueOf(tagTime);

    }
}
