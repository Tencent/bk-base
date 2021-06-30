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

package com.tencent.bk.base.dataflow.jobnavi.api;

import com.tencent.bk.base.dataflow.jobnavi.exception.AbstractJobNaviTaskException;
import com.tencent.bk.base.dataflow.jobnavi.rpc.RequestException;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventConvertor;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public class TaskEventServiceImpl implements TaskEventService.AsyncIface {

    private static final Logger logger = Logger.getLogger(TaskEventServiceImpl.class);

    private JobNaviTask task;

    private TaskEventResult processRunning(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        TaskType type = event.getContext().getTaskInfo().getType();
        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        Long scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
        long dataTime = event.getContext().getTaskInfo().getDataTime();
        logger.info("Task running [" + scheduleId + "] . Schedule time is: " + scheduleTime + ", data time is: "
                + dataTime);
        try {
            task = (JobNaviTask) Class.forName(type.getMain()).newInstance();
            task.run(TaskEventConvertor.toCommonEvent(event));
        } catch (Throwable e) {
            TaskStatus status = TaskStatus.failed;
            if (e instanceof AbstractJobNaviTaskException) {
                status = TaskStatus.valueOf(((AbstractJobNaviTaskException) e).taskStatus().name());
                logger.info("change task status to " + status.name());
            }
            logger.error("Running task [" + scheduleId + "] error", e);
            String errorMsg = e.getMessage();
            result.setProcessInfo("Task running error: " + errorMsg);
            event.setChangeStatus(status);
            event.setEventName(status.toString());
            event.getContext().setEventInfo(errorMsg);
            List<TaskEvent> events = new ArrayList<>();
            events.add(event);
            result.setNextEvents(events);
            return result;
        }
        event.setChangeStatus(TaskStatus.finished);
        event.setEventName(TaskStatus.finished.toString());
        List<TaskEvent> events = new ArrayList<>();
        events.add(event);
        result.setSuccess(true);
        result.setNextEvents(events);
        return result;
    }

    private TaskEventResult processTaskEvent(TaskEvent event, String eventName) throws RequestException {
        TaskEventResult result = new TaskEventResult();
        result.setSuccess(false);
        if (task == null) {
            logger.error("task may not start.");
            RequestException e = new RequestException();
            e.setReason("task may not start.");
            throw e;
        }

        String scheduleId = event.getContext().getTaskInfo().getScheduleId();
        logger.info("Task [" + scheduleId + "] process event " + eventName + ".");
        EventListener listener = task.getEventListener(eventName);
        if (listener == null) {
            result.setSuccess(false);
            result.setProcessInfo("no listener for event:" + eventName);
            logger.error(result.getProcessInfo());
            return result;
        }
        try {
            return TaskEventConvertor.toThriftResult(listener.doEvent(TaskEventConvertor.toCommonEvent(event)));
        } catch (Throwable e) {
            logger.error("process event " + eventName + " error.", e);
            result.setSuccess(false);
            result.setProcessInfo("process event " + eventName + " error. The reason is: " + e.getMessage());
            return result;
        }
    }

    @Override
    public void processEvent(TaskEvent event, AsyncMethodCallback<TaskEventResult> resultHandler) throws TException {
        String eventName = event.getEventName();
        TaskEventResult result;
        if (TaskStatus.running.toString().equals(eventName)) {
            result = processRunning(event);
            logger.info("Running task finish");
        } else {
            result = processTaskEvent(event, eventName);
        }
        resultHandler.onComplete(result);
    }
}
