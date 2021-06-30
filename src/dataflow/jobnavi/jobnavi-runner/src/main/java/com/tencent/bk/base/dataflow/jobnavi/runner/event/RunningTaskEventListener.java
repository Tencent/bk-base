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

package com.tencent.bk.base.dataflow.jobnavi.runner.event;

import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.TaskThreadManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import org.apache.log4j.Logger;


public class RunningTaskEventListener implements TaskEventListener {

    private static final Logger LOGGER = Logger.getLogger(RunningTaskEventListener.class);

    @Override
    public TaskEventResult doEvent(TaskEventInfo taskEventInfo) {
        TaskEvent event = taskEventInfo.getTaskEvent();
        if (TaskType.TASK_TYPE_DEFAULT.equals(event.getContext().getTaskInfo().getType().getName())) {
            TaskEventResult result = new TaskEventResult();
            //default task, do noting.
            LOGGER.info("Running task: " + event.getContext().getTaskInfo().toJson());
            result.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.finished));
            return result;
        }

        if (event.getContext().getTaskInfo().getType().getTaskMode() == TaskMode.process) {
            LOGGER.info("process task: " + event.getContext().getTaskInfo().toJson());
            TaskEventResult result = new TaskEventResult();
            try {
                TaskProcessManager.run(taskEventInfo);
            } catch (Exception e) {
                LOGGER.error("Process run failed.", e);
                result.setSuccess(false);
                result.setProcessInfo(e.getMessage());
            }
            return result;
        } else if (event.getContext().getTaskInfo().getType().getTaskMode() == TaskMode.thread) {
            LOGGER.info("thread task: " + event.getContext().getTaskInfo().toJson());
            TaskEventResult result = new TaskEventResult();
            try {
                TaskThreadManager.run(taskEventInfo);
            } catch (Exception e) {
                LOGGER.error("thread run failed.", e);
                result.setSuccess(false);
                result.setProcessInfo(e.getMessage());
            }
            return result;
        } else {
            TaskEventResult result = new TaskEventResult();
            result.setSuccess(false);
            result.setProcessInfo("task mode not support.");
            return result;
        }
    }

}
