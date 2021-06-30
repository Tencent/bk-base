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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskDecommissionedProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskDisabledProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskFailedProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskFailedSucceededProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskFinishedProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskKillProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskLostProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskPatchingProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskPreparingFailedProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskPreparingKillProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskPreparingProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskRecoveringProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskRunningProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskSkippedProcessor;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.processor.TaskZombieProcessor;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskStateMachineFactory {

    private static final Map<TaskStatus, Map<String, TaskProcessor>> statusMachineMaps = new ConcurrentHashMap<>();

    static {
        addTransition(TaskStatus.none, "preparing", new TaskPreparingProcessor());
        addTransition(TaskStatus.none, "recovering", new TaskRecoveringProcessor());

        addTransition(TaskStatus.preparing, "running", new TaskRunningProcessor());
        addTransition(TaskStatus.preparing, "failed", new TaskPreparingFailedProcessor());
        addTransition(TaskStatus.preparing, "disabled", new TaskDisabledProcessor());
        addTransition(TaskStatus.preparing, "skipped", new TaskSkippedProcessor());
        addTransition(TaskStatus.preparing, "kill", new TaskPreparingKillProcessor());
        addTransition(TaskStatus.preparing, "patching", new TaskPatchingProcessor());

        addTransition(TaskStatus.running, "finished", new TaskFinishedProcessor());
        addTransition(TaskStatus.running, "failed", new TaskFailedProcessor());
        addTransition(TaskStatus.running, "decommission", new TaskDecommissionedProcessor());
        addTransition(TaskStatus.running, "kill", new TaskKillProcessor());
        addTransition(TaskStatus.running, "failed_succeeded", new TaskFailedSucceededProcessor());
        addTransition(TaskStatus.running, "lost", new TaskLostProcessor());

        addTransition(TaskStatus.recovering, "running", new TaskRunningProcessor());
        addTransition(TaskStatus.recovering, "failed", new TaskFailedProcessor());
        addTransition(TaskStatus.recovering, "disabled", new TaskDisabledProcessor());
        addTransition(TaskStatus.recovering, "skipped", new TaskSkippedProcessor());
        addTransition(TaskStatus.recovering, "kill", new TaskKillProcessor());
        addTransition(TaskStatus.recovering, "patching", new TaskPatchingProcessor());

        addTransition(TaskStatus.finished, "zombie", new TaskZombieProcessor());
        addTransition(TaskStatus.failed, "zombie", new TaskZombieProcessor());
        addTransition(TaskStatus.decommissioned, "zombie", new TaskZombieProcessor());
        addTransition(TaskStatus.killed, "zombie", new TaskZombieProcessor());
        addTransition(TaskStatus.failed_succeeded, "zombie", new TaskZombieProcessor());
        addTransition(TaskStatus.lost, "zombie", new TaskZombieProcessor());
    }

    /**
     * add state transition processor
     *
     * @param preStatus
     * @param eventName
     * @param processor
     */
    public static void addTransition(TaskStatus preStatus, String eventName, TaskProcessor processor) {
        if (statusMachineMaps.get(preStatus) != null) {
            statusMachineMaps.get(preStatus).put(eventName, processor);
        } else {
            Map<String, TaskProcessor> eventMap = new ConcurrentHashMap<>();
            eventMap.put(eventName, processor);
            statusMachineMaps.put(preStatus, eventMap);
        }
    }

    /**
     * do transition
     *
     * @param preStatus
     * @param taskEventInfo
     */
    public static TaskEventResult doTransition(TaskStatus preStatus, TaskEventInfo taskEventInfo) throws NaviException {
        Map<String, TaskProcessor> eventMap = statusMachineMaps.get(preStatus);
        TaskEvent event = taskEventInfo.getTaskEvent();
        if (eventMap == null || eventMap.get(event.getEventName()) == null) {
            TaskEventResult result = new TaskEventResult();
            result.setProcessInfo(
                    "[ERROR] Can not process event " + event.getEventName() + " when task status is " + preStatus
                            .toString());
            result.setSuccess(false);
            return result;
        }
        TaskProcessor processor = eventMap.get(event.getEventName());
        return processor.process(taskEventInfo);
    }
}
