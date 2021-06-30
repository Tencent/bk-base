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

package com.tencent.bk.base.dataflow.jobnavi.runner.task.process.callback;

import com.tencent.bk.base.dataflow.jobnavi.runner.task.SchedulerManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventConvertor;
import com.tencent.bk.base.dataflow.jobnavi.state.event.CustomTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;

public abstract class AbstractTaskEventProcessCallback implements
        AsyncMethodCallback<com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventResult> {

    private static final Logger logger = Logger.getLogger(AbstractTaskEventProcessCallback.class);

    private final Configuration conf;
    private final TaskEventInfo eventInfo;
    private final TNonblockingTransport transport;
    private final TAsyncClientManager clientManager;

    public AbstractTaskEventProcessCallback(Configuration conf,
            TaskEventInfo eventInfo,
            TNonblockingTransport transport,
            TAsyncClientManager clientManager) {
        this.conf = conf;
        this.transport = transport;
        this.eventInfo = eventInfo;
        this.clientManager = clientManager;
    }

    private static void sendEventResult(TaskEventResult result, TaskEventInfo originalTaskEventInfo) {
        final Long eventId = originalTaskEventInfo.getId();
        final TaskEvent event = originalTaskEventInfo.getTaskEvent();
        if (event instanceof CustomTaskEvent) {
            try {
                SchedulerManager.sendEventResult(eventId, result);
            } catch (Exception e) {
                logger.error("send event result error.", e);
            }
        }
    }

    @Override
    public void onComplete(com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventResult response) {
        TaskEventResult taskEventResult;
        try {
            taskEventResult = TaskEventConvertor.toCommonResult(response);
        } catch (NaviException e) {
            logger.error("convert event result error.", e);
            throw new RuntimeException("convert event result error.", e);
        }
        complete(taskEventResult);
        sendEventResult(taskEventResult, eventInfo);
        transport.close();
        clientManager.stop();
    }

    @Override
    public void onError(Exception exception) {
        error(exception);
        TaskEventResult taskEventResult = new TaskEventResult();
        taskEventResult.setSuccess(false);
        taskEventResult.setProcessInfo(exception.getMessage());
        sendEventResult(taskEventResult, eventInfo);
        transport.close();
        clientManager.stop();
    }

    public TaskEventInfo getEventInfo() {
        return eventInfo;
    }

    public Configuration getConf() {
        return conf;
    }

    public TaskEvent getEvent() {
        return eventInfo.getTaskEvent();
    }

    public TNonblockingTransport getTransport() {
        return transport;
    }

    public TAsyncClientManager getClientManager() {
        return clientManager;
    }

    public abstract void complete(TaskEventResult result);

    public abstract void error(Exception e);
}
