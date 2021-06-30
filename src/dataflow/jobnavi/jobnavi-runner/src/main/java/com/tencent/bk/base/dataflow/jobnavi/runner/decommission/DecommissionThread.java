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

package com.tencent.bk.base.dataflow.jobnavi.runner.decommission;

import com.tencent.bk.base.dataflow.jobnavi.runner.Main;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.TaskUtil;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventResult;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import java.util.Map;
import org.apache.log4j.Logger;

public class DecommissionThread implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(DecommissionThread.class);
    private final TaskEventInfo taskEventInfo;

    public DecommissionThread(TaskEventInfo taskEventInfo) {
        this.taskEventInfo = taskEventInfo;
    }

    @Override
    public void run() {
        for (Map.Entry<Long, TaskInfo> entry : TaskUtil.getAllRunningTask().entrySet()) {
            TaskEvent event = taskEventInfo.getTaskEvent();
            try {
                TaskEvent toSendEvent =
                        DefaultTaskEventBuilder.changeEvent(event, "decommission", TaskStatus.decommissioned);
                toSendEvent.getContext().setTaskInfo(entry.getValue());
                toSendEvent.getContext().getExecuteInfo().setId(entry.getKey());
                LOGGER.info(String.format("decommissioning report: send event:%s to task", toSendEvent.toJson()));
                String decommissionTimeout = entry.getValue().getDecommissionTimeout();
                if (decommissionTimeout == null) {
                    Configuration conf = Main.getConfig();
                    decommissionTimeout = conf.getString(
                            Constants.JOBNAVI_RUNNER_DECOMMISSION_MAX_TIME,
                            Constants.JOBNAVI_RUNNER_DECOMMISSION_MAX_TIME_DEFAULT);
                }
                TaskUtil.sendTaskEvent(toSendEvent, CronUtil.parsePeriodStringToMills(decommissionTimeout));
                DecommissionTaskManager.setDecommissionTask(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOGGER.error("Send decommission event to task failed", e);
            }
        }
        Map<Long, DecommissionEventResult> tasks = DecommissionTaskManager.getAllDecommissionTask();
        boolean allFinished;
        do {
            LOGGER.info(String.format("decommissioning report: all decommissioning task - %s", tasks.toString()));
            allFinished = true;
            for (Map.Entry<Long, DecommissionEventResult> entry : tasks.entrySet()) {
                DecommissionEventResult result = entry.getValue();
                DecommissionEventStatus taskStatus = result.getStatus();
                allFinished = allFinished
                        && (taskStatus != DecommissionEventStatus.decommissioning)
                        && (result.isRecovery() || !DecommissionUtil.isTaskNeedRecovery(taskStatus));
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error("Decommission thread sleep interrupt", e);
            }
        } while (!allFinished && Main.getRunnerInfo().getStatus() == RunnerStatus.decommissioning);
        if (allFinished) {
            Main.getRunnerInfo().setStatus(RunnerStatus.decommissioned);
        }
    }

}
