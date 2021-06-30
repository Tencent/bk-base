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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

public class StopJobV2 {

    private static final Logger logger = Logger.getLogger(StopJobV2.class);

    /**
     * 停止flink任务
     *
     * @param event 停止任务事件
     * @param task 任务所在的task信息
     * @throws Exception 停止任务异常
     */
    public void stop(TaskEvent event, FlinkSubmitTask task) throws Exception {
        logger.info("Flink Job stoping...");
        String applicationId = task.getApplicationId();
        Configuration config = task.getConfig();

        List<String> argsArray = new ArrayList<>();
        argsArray.add("stop");
        argsArray.add("-yid");
        argsArray.add(applicationId);
        String eventInfo = event.getContext().getEventInfo();
        String typeName = event.getContext().getTaskInfo().getType().getName();
        FlinkOptionsV2 flinkOptions = new FlinkOptionsV2(eventInfo, config, typeName);
        String jobName = flinkOptions.getJobName().getValue().toString();
        Map<String, String> jobNameAndIdMaps = SyncJob.sync(task);
        String jobId = null;
        if (jobNameAndIdMaps != null) {
            jobId = jobNameAndIdMaps.get(jobName);
        }
        String savePointPath = flinkOptions.getSavepointRootPath();

        argsArray.add("-p");
        argsArray.add(savePointPath);
        if (null != jobId) {
            argsArray.add("-d");
            argsArray.add(jobId);
        }

        StringBuilder argStr = new StringBuilder();
        for (String arg : argsArray) {
            argStr.append(arg).append(" ");
        }
        logger.info("args is: " + argStr);

        CliCommandV2.run(argStr.toString(), event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName());
        logger.info("stop job, application is: " + applicationId);
        Long submitId = task.getSubmitIdMap().get(jobName);
        if (ResourceUtil.updateJobSubmitStatus(submitId, TaskStatus.finished.name()) == null) {
            logger.warn("Failed to update status of job submit:" + submitId);
        }
    }
}
