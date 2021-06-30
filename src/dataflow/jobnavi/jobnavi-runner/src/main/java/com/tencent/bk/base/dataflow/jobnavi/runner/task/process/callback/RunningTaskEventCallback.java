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

import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAggregationService;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.SchedulerManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;

public class RunningTaskEventCallback extends AbstractTaskEventProcessCallback {

    private static final Logger LOGGER = Logger.getLogger(RunningTaskEventCallback.class);

    public RunningTaskEventCallback(Configuration conf, TaskEventInfo eventInfo, TNonblockingTransport transport,
            TAsyncClientManager clientManager) {
        super(conf, eventInfo, transport, clientManager);
    }

    @Override
    public void complete(TaskEventResult taskEventResult) {
        try {
            LOGGER.info("Task Running complete, result is :" + taskEventResult.toJson());
            for (TaskEvent event : taskEventResult.getNextEvents()) {
                SchedulerManager.sendEvent(event);
            }
        } catch (Exception e) {
            LOGGER.error("Complete Running task error.", e);
        } finally {
            TaskProcessManager.unregisterTask(getEvent().getContext().getExecuteInfo().getId());
            LogAggregationService.callAggregateLog(getEvent().getContext().getExecuteInfo().getId());
        }
    }

    @Override
    public void error(Exception e) {
        LOGGER.error("Running task error. exec_id is " + getEvent().getContext().getExecuteInfo().getId(), e);
        TaskProcessManager.unregisterTask(getEvent().getContext().getExecuteInfo().getId());
        try {
            DefaultTaskEvent failedEvent = DefaultTaskEventBuilder.changeEvent(getEvent(), "failed", TaskStatus.failed);
            failedEvent.getContext().setEventInfo(e.getMessage());
            SchedulerManager.sendEvent(failedEvent);
            LogAggregationService.callAggregateLog(getEvent().getContext().getExecuteInfo().getId());
        } catch (Exception e1) {
            LOGGER.error("sendEvent task failed error.", e1);
        }
    }

}
