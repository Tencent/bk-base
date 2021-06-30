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

import com.tencent.bk.base.datahub.databus.commons.BkDatabusContext;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BkMetricSource<T> implements BkSource<T> {

    private static final Logger log = LoggerFactory.getLogger(BkMetricSource.class);
    private static final int DEFAULT_QUEUE_LENGTH = 1000;
    protected AtomicBoolean isStop = new AtomicBoolean(false);
    protected BkDatabusContext context;
    private LinkedBlockingQueue<BkSourceRecord<T>> queue;
    // 统计信息
    private long recordCountTotal = 0;
    private long recordSizeTotal = 0;
    protected long start = 0;
    private long lastCheckTime = System.currentTimeMillis();

    @Override
    public final void open(final BkDatabusContext context) throws Exception {
        this.context = context;
        Thread.currentThread().setName(context.getName());    // 将任务名称设置到当前线程上，便于上报数据
        start = System.currentTimeMillis();
        queue = new LinkedBlockingQueue<>(this.getQueueLength());
        doOpen(context);
        Metric.getInstance().reportEvent(this.context.getName(), Consts.TASK_START_SUCC, "", "");
    }

    @Override
    public final BkSourceRecord<T> read() throws Exception {
        if (isStop.get()) {
            return null;
        }

        try {
            BkSourceRecord<T> record = queue.take();
            if (null == record) {
                return null;
            }
            recordCountTotal++;
            recordSizeTotal += record.getValueLength();
            if (System.currentTimeMillis() - lastCheckTime > TimeUnit.SECONDS.toMillis(30)) {
                lastCheckTime = System.currentTimeMillis();
                long duration = System.currentTimeMillis() - start;
                if (StringUtils.isNotBlank(context.getRtId())) {
                    LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for result table {}",
                            recordCountTotal, recordSizeTotal, duration, context.getRtId());
                } else {
                    LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for dataid {}", recordCountTotal,
                            recordSizeTotal, duration, context.getDataId());
                }
            }
            // 打点信息
            final String now = String.valueOf(System.currentTimeMillis() / 1000); // 取秒
            Metric.getInstance().updateStat(String.valueOf(context.getDataId()), context.getName(), getDestTopic(),
                    this.getClusterType(), 1, record.getValueLength(), now, now);
            return record;
        } catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
                Metric.getInstance()
                        .reportEvent(context.getName(), Consts.TASK_RUN_FAIL, ExceptionUtils.getStackTrace(e),
                                e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public final void close() throws Exception {
        isStop.set(true);
        try {
            doClose();
        } catch (Exception e) {
            Metric.getInstance().reportEvent(context.getName(), Consts.TASK_STOP_FAIL, ExceptionUtils.getStackTrace(e),
                    e.getMessage());
            throw e;
        }
        long duration = System.currentTimeMillis() - start;
        if (StringUtils.isNotBlank(context.getRtId())) {
            LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for result table {}", recordCountTotal,
                    recordSizeTotal, duration, context.getRtId());
        } else {
            LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for dataid {}", recordCountTotal,
                    recordSizeTotal, duration, context.getDataId());
        }
    }


    /**
     * 启动task
     */
    protected abstract void doOpen(final BkDatabusContext context);

    /**
     * 将{@link BkSourceRecord} 放入临时队列，异步将记录发送到pulsar
     *
     * @param record 下一条将要发送到pulsar的消息
     */
    public final void consume(BkSourceRecord<T> record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 停止task
     */
    protected void doClose() throws Exception {
        // do nothing
    }

    /**
     * 获取将记录推送到用户的队列的长度可以重写此方法以自定义队列长度
     *
     * @return 临时队列的长度
     */
    protected int getQueueLength() {
        return DEFAULT_QUEUE_LENGTH;
    }

    protected abstract String getClusterType();

    /**
     * 获取目标topic
     *
     * @return 目标topic
     */
    protected abstract String getDestTopic();
}
