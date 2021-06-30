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

package com.tencent.bk.base.datahub.databus.commons;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;


public class BulkProcessor<T> {

    private final int bulkActions;
    private final Consumer<List<T>> bulkHandler;
    private List<T> buffer;
    private volatile boolean closed = false;
    private final ReentrantLock lock = new ReentrantLock();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture cancellableFlushTask;

    public BulkProcessor(int bulkActions, long flushInterval, Consumer<List<T>> bulkHandler) {
        this.bulkActions = bulkActions;
        this.bulkHandler = bulkHandler;
        this.buffer = new ArrayList<>(bulkActions);
        // Start period flushing task after everything is setup
        this.cancellableFlushTask = startFlushTask(flushInterval);
    }


    private ScheduledFuture startFlushTask(long flushInterval) {
        return scheduler.scheduleAtFixedRate(new Flush(), flushInterval, flushInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭操作
     */
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            this.cancellableFlushTask.cancel(true);
            if (!buffer.isEmpty()) {
                execute();
            }
        } finally {
            lock.unlock();
        }
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    /**
     * Adds an {@link T} to the list of actions to execute. Follows the same behavior of {@link T} (for example, if no
     * id
     * is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(T request) {
        internalAdd(Collections.singletonList(request));
        return this;
    }

    /**
     * 添加一批记录到BulkProcessor
     *
     * @param request 待添加的一组数据
     * @return BulkProcessor
     */
    public BulkProcessor addAll(List<T> request) {
        internalAdd(request);
        return this;
    }

    private void internalAdd(List<T> request) {
        //bulkRequest and instance swapping is not threadsafe, so execute the mutations under a lock.
        //once the bulk request is ready to be shipped swap the instance reference unlock and send the local
        // reference to the handler.
        List<T> bulkToExecute;
        lock.lock();
        try {
            ensureOpen();
            buffer.addAll(request);
            bulkToExecute = swapBufferIfNeeded();
        } finally {
            lock.unlock();
        }
        //execute sending the local reference outside the lock to allow handler to control the concurrency via it's
        // configuration.
        if (bulkToExecute != null) {
            execute(bulkToExecute);
        }
    }


    /**
     * Flush pending delete or index requests.
     */
    public void flush() {
        lock.lock();
        try {
            ensureOpen();
            if (!this.buffer.isEmpty()) {
                execute();
            }
        } finally {
            lock.unlock();
        }
    }


    // needs to be executed under a lock
    private void execute() {
        final List<T> bulkRequest = this.buffer;
        this.buffer = new ArrayList<>(bulkActions);
        execute(bulkRequest);
    }


    private void execute(List<T> bulk) {
        bulkHandler.accept(bulk);
    }

    // needs to be executed under a lock
    private List<T> swapBufferIfNeeded() {
        ensureOpen();
        if (!isOverTheLimit()) {
            return null;
        }
        final List<T> bulk = this.buffer;
        this.buffer = new ArrayList<>(bulkActions);
        return bulk;
    }

    // needs to be executed under a lock
    private boolean isOverTheLimit() {
        if (bulkActions != -1 && buffer.size() >= bulkActions) {
            return true;
        }
        return false;
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                if (buffer.isEmpty()) {
                    return;
                }
                execute();
            } finally {
                lock.unlock();
            }
        }
    }

}
