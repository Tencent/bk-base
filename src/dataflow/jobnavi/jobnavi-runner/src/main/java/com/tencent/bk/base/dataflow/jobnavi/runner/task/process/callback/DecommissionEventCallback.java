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

import com.tencent.bk.base.dataflow.jobnavi.runner.decommission.DecommissionTaskManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.decommission.DecommissionUtil;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.SchedulerManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventResult;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;

public class DecommissionEventCallback extends AbstractTaskEventProcessCallback {

    private static final Logger LOGGER = Logger.getLogger(DecommissionEventCallback.class);

    public DecommissionEventCallback(Configuration conf, TaskEventInfo eventInfo, TNonblockingTransport transport,
            TAsyncClientManager clientManager) {
        super(conf, eventInfo, transport, clientManager);
    }

    @Override
    public void complete(TaskEventResult taskEventResult) {
        try {
            LOGGER.info("Task decommission complete, result is :" + taskEventResult.toJson());
            doCallBack(DecommissionEventStatus.decommissioned);
        } catch (Exception e) {
            LOGGER.error("Task decommission error.", e);
        }
    }

    @Override
    public void error(Exception e) {
        try {
            LOGGER.error("Task decommission error.", e);
            DecommissionEventStatus status = DecommissionEventStatus.failed;
            if (e instanceof java.util.concurrent.TimeoutException && e.getMessage().contains("timed out")) {
                status = DecommissionEventStatus.lost;
            }
            doCallBack(status);
        } catch (Exception exp) {
            LOGGER.error("Task decommission error.", exp);
        }
    }

    private void doCallBack(DecommissionEventStatus status) {
        try {
            long executeId = getEvent().getContext().getExecuteInfo().getId();
            TaskProcessManager.unregisterTask(executeId);
            DecommissionEventResult eventResult = DecommissionTaskManager.getEventResult(executeId);
            eventResult.setStatus(status);
            if (DecommissionUtil.isTaskNeedRecovery(eventResult.getStatus())) {
                eventResult.setRecovery(SchedulerManager.recoveryTask(eventResult.getTaskInfo()));
                eventResult.setStatus(DecommissionEventStatus.decommissioned);
                SchedulerManager.sendEvent(DefaultTaskEventBuilder
                        .changeEvent(getEvent(), getEvent().getEventName(), TaskStatus.decommissioned));
            } else {
                SchedulerManager.sendEvent(DefaultTaskEventBuilder.changeEvent(getEvent(), TaskStatus.failed));
            }
        } catch (Exception exp) {
            LOGGER.error("Task decommission error.", exp);
        }
    }

}
