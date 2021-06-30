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

package com.tencent.bk.base.datahub.databus.commons.connector.task;

import com.tencent.bk.base.datahub.databus.commons.BkDatabusContext;
import com.tencent.bk.base.datahub.databus.commons.connector.sink.BkSink;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSource;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.connector.sink.BkSinkRecord;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskEngine implements AutoCloseable, Runnable {

    private static final Logger log = LoggerFactory.getLogger(TaskEngine.class);
    private static final int THREAD_SHUTDOWN_TIMEOUT_MILLIS = 10_000;
    private final AtomicBoolean isStop = new AtomicBoolean(false);
    // The thread that invokes the function
    private Thread fnThread;
    private BkSourceRecord<?> currentRecord;
    private BkSource<?> source;
    private BkSink<?, ?> sink;
    private BkDatabusContext context;

    public TaskEngine(BkSource<?> source, BkSink<?, ?> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * 启动任务
     */
    public final void start(final BkDatabusContext context) throws Exception {
        this.context = context;
        this.sink.open(context);
        this.source.open(context);
        this.isStop.set(false);
        this.fnThread = new Thread(this);
        this.fnThread.setUncaughtExceptionHandler(
                (t, e) -> LogUtils.error("", log, "[{}] Error while consuming records", t.getName(), e));
        this.fnThread.setName(String.format("%s-%s", context.getRtId(), context.getInstanceId()));
        this.fnThread.start();
    }


    @Override
    public final void run() {
        try {

            while (!this.isStop.get()) {
                currentRecord = readInput();
                try {
                    sendOutputMessage(currentRecord);
                } catch (Exception e) {
                    LogUtils.warn(log, "Failed to process result of message {}", currentRecord, e);
                    currentRecord.fail();
                }
            }
        } catch (Throwable t) {
            LogUtils.warn(log, "[{}]-{} Uncaught exception in Java Instance", this.context.getRtId(),
                    this.context.getInstanceId(), t);
        } finally {
            LogUtils.info(log, "Closing TaskEngine");
            close();
        }
    }

    private BkSourceRecord<?> readInput() {
        BkSourceRecord<?> record;

        try {
            record = this.source.read();
        } catch (Exception e) {
            LogUtils.info(log, "Encountered exception in source read: ", e);
            throw new RuntimeException(e);
        }

        // check record is valid
        if (record == null) {
            throw new IllegalArgumentException("The record returned by the source cannot be null");
        } else if (record.getValue() == null) {
            throw new IllegalArgumentException("The value in the record returned by the source cannot be null");
        }
        return record;
    }

    private void sendOutputMessage(BkSourceRecord<?> srcRecord) {
        try {
            this.sink.write(new BkSinkRecord("", srcRecord));
        } catch (Exception e) {
            LogUtils.info(log, "Encountered exception in sink write: ", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() {
        if (source != null) {

            try {
                source.close();
            } catch (Throwable e) {
                LogUtils.error("", log, "Failed to close source {}", this.context.getRtId(), e);
            }
            source = null;
        }

        if (sink != null) {
            try {
                sink.close();
            } catch (Throwable e) {
                LogUtils.error("", log, "Failed to close sink {}", this.context.getRtId(), e);
            }
            sink = null;
        }
    }

    /**
     * 停止任务
     */
    public final void stop() {
        this.isStop.set(true);
        if (fnThread != null) {
            // interrupt the instance thread
            fnThread.interrupt();
            try {
                // If the instance thread doesn't respond within some time, attempt to
                // kill the thread
                fnThread.join(THREAD_SHUTDOWN_TIMEOUT_MILLIS, 0);
            } catch (InterruptedException e) {
                // ignore this
            }
            this.close();
        }
    }

    /**
     * 是否还活着
     *
     * @return 是否还活着的boolean值
     */
    public boolean isAlive() {
        if (this.fnThread != null) {
            return this.fnThread.isAlive();
        } else {
            return false;
        }
    }

}
