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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import java.util.List;
import org.apache.log4j.Logger;

public class TaskRecoveringProcessor implements TaskProcessor {

    private static final Logger LOGGER = Logger.getLogger(TaskRecoveringProcessor.class);

    @Override
    public TaskEventResult process(TaskEventInfo taskEventInfo) {
        TaskEventResult result = new TaskEventResult();
        TaskEvent event = taskEventInfo.getTaskEvent();
        try {
            MetaDataManager.getJobDao().addExecute(event);
            result.setSuccess(true);
            result.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.running));
        } catch (Throwable e) {
            LOGGER.error("Task recovering process error.", e);
        }
        //generate execute status cache
        try {
            String scheduleId = event.getContext().getTaskInfo().getScheduleId();
            long dataTime = event.getContext().getTaskInfo().getDataTime();
            List<TaskInfo> childrenTaskInfo = DependencyManager
                    .generateChildrenTaskInfo(event.getContext().getTaskInfo().getScheduleId(),
                            event.getContext().getTaskInfo().getDataTime());
            if (childrenTaskInfo != null && childrenTaskInfo.size() > 0) {
                ExecuteStatusCache.putStatus(scheduleId, dataTime, TaskStatus.recovering);
                for (TaskInfo taskInfo : childrenTaskInfo) {
                    String childScheduleId = taskInfo.getScheduleId();
                    long childScheduleTime = taskInfo.getScheduleTime();
                    long childDataTime = taskInfo.getDataTime();
                    ExecuteStatusCache
                            .putReference(scheduleId, dataTime, childScheduleId, childScheduleTime, childDataTime);
                }
            }
        } catch (Throwable e) {
            LOGGER.warn("Failed to generate status cache for execute:" + event.getContext().getExecuteInfo().getId(),
                    e);
        }
        return result;
    }

}
