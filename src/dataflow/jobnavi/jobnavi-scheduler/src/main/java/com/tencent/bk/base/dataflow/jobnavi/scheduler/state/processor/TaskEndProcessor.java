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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event.RunningTaskEventBuffer;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.recovery.RunnerTaskRecoveryManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.RunningTaskCounterService;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;

public class TaskEndProcessor implements TaskProcessor {

    @Override
    public TaskEventResult process(TaskEventInfo taskEventInfo) {
        TaskEvent event = taskEventInfo.getTaskEvent();
        TaskInfo taskInfo = event.getContext().getTaskInfo();
        String scheduleId = taskInfo.getScheduleId();
        RunningTaskCounterService.minusRunningTask(scheduleId);
        ExecuteInfo executeInfo = event.getContext().getExecuteInfo();
        RunnerTaskRecoveryManager.unregisterTask(executeInfo.getHost(), executeInfo.getId());
        RunnerInfo runnerInfo = HeartBeatManager.getNodeInfos().get(executeInfo.getHost());
        if (runnerInfo != null) {
            TaskMode taskMode = taskInfo.getType().getTaskMode();
            switch (taskMode) {
                case process:
                    runnerInfo.setTaskNum(runnerInfo.getTaskNum() - 1);
                    break;
                case thread:
                    runnerInfo.setTaskThreadNum(runnerInfo.getTaskThreadNum() - 1);
                    break;
                default:
                    break;
            }
            HeartBeatManager.updateHeartBeat(runnerInfo, false);
        }
        RunningTaskEventBuffer.checkAndRecover();
        return new TaskEventResult();
    }
}
