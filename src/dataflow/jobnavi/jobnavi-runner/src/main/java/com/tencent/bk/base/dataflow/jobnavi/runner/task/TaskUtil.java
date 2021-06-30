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

package com.tencent.bk.base.dataflow.jobnavi.runner.task;

import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.TaskThreadManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import java.util.Map;

public class TaskUtil {

    public static Map<Long, TaskInfo> getAllRunningTask() {
        Map<Long, TaskInfo> allTasks = TaskProcessManager.getExecuteTasks();
        allTasks.putAll(TaskThreadManager.getExecuteTasks());
        return allTasks;
    }

    public static void sendTaskEvent(TaskEventInfo taskEventInfo, Long timeout) throws NaviException {
        TaskMode mode = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getType().getTaskMode();
        switch (mode) {
            case process:
                TaskProcessManager.sendEvent(taskEventInfo, timeout);
                break;
            case thread:
                TaskThreadManager.sendEvent(taskEventInfo);
                break;
            default:
                throw new NaviException("not support yet mode");
        }
    }

    public static void sendTaskEvent(TaskEvent event, Long timeout) throws NaviException {
        TaskMode mode = event.getContext().getTaskInfo().getType().getTaskMode();
        switch (mode) {
            case process:
                TaskProcessManager.sendEvent(event, timeout);
                break;
            case thread:
                TaskThreadManager.sendEvent(event);
                break;
            default:
                throw new NaviException("not support yet mode");
        }
    }
}
