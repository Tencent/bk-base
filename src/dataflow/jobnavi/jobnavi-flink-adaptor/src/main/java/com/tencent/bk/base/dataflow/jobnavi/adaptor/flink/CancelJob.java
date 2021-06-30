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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

public class CancelJob {

    /**
     * cancel flink job
     *
     * @param event
     * @param task
     * @throws Exception
     */
    public void cancel(TaskEvent event, FlinkSubmitTask task) throws Exception {
        final Logger logger = task.getLogger();
        logger.info("Flink Job canceling...");
        String applicationId = task.getApplicationId();
        Configuration config = task.getConfig();

        String eventInfo = event.getContext().getEventInfo();
        List<String> argsArray = new ArrayList<>();
        argsArray.add("cancel");
        argsArray.add("-yid");
        argsArray.add(applicationId);

        FlinkOptions flinkOptions = new FlinkOptions(eventInfo, config);

        String targetDirectory = flinkOptions.getSavepointRootPath();
        boolean useSavepoint = flinkOptions.getUseSavepoint().getValue() != null && Boolean
                .parseBoolean(flinkOptions.getUseSavepoint().getValue().toString());
        if (useSavepoint) {
            argsArray.add("-s");
            argsArray.add(targetDirectory);
        }
        Map<String, String> jobNameAndIdMaps = SyncJob.syncRunningJob(task);
        Collections.addAll(argsArray, flinkOptions.toFinkArgs(jobNameAndIdMaps, false));

        StringBuilder argStr = new StringBuilder();
        for (String arg : argsArray) {
            argStr.append(arg).append(" ");
        }
        logger.info("args is: " + argStr);

        String jobName = flinkOptions.getJobName().getValue().toString();
        CliCommand.run(argStr.toString(), event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName());
        if (task.getJobNameAndIdMaps().get(jobName) != null) {
            String jobId = task.getJobNameAndIdMaps().get(jobName);
            String checkpoint = config.getString("state.checkpoints.dir", "hdfs:///app/flink/checkpoints/") + jobId;
            logger.info("clean check point: " + checkpoint);
            HdfsUtil.delete(checkpoint);
        }
        task.getJobNameAndIdMaps().remove(jobName);
        String oldSavepoint = FlinkSavepoint.getSavepoint(targetDirectory);
        if (oldSavepoint != null && oldSavepoint.length() > 0) {
            logger.info("delete save point: " + oldSavepoint);
            FlinkSavepoint.deleteSavepoint(oldSavepoint);
        }
        Long submitId = task.getSubmitIdMap().get(jobName);
        if (ResourceUtil.updateJobSubmitStatus(submitId, TaskStatus.finished.name()) == null) {
            logger.warn("Failed to update status of job submit:" + submitId);
        }
    }
}
