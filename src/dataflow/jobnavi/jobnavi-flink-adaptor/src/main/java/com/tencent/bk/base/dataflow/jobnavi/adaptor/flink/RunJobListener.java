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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.log4j.Logger;

public class RunJobListener implements EventListener {

    protected final Logger logger;

    protected String applicationId;
    protected Configuration config;
    protected String configurationDirectory;

    protected FlinkSubmitTask task;

    RunJobListener(FlinkSubmitTask task) {
        this.applicationId = task.getApplicationId();
        this.config = task.getConfig();
        this.configurationDirectory = task.getConfigurationDirectory();
        this.task = task;
        this.logger = task.getLogger();
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        logger.info("Flink Job running...");
        String eventInfo = event.getContext().getEventInfo();
        List<String> argsArray = new ArrayList<>();
        try {
            FlinkOptions options = new FlinkOptions(eventInfo, config);
            ApplicationReport applicationReport = YarnUtil.getApplication(applicationId);
            if (applicationReport == null) {
                throw new NaviException("application " + applicationId + " is not exist");
            }
            validateYarnModeResource(options, applicationReport.getQueue());
            argsArray.add("--allowNonRestoredState");
            argsArray.add("-yid");
            if (options.getOption("application_id").getValue() != null) {
                argsArray.add(options.getOption("application_id").getValue().toString());
            } else {
                argsArray.add(applicationId);
            }

            Collections.addAll(argsArray, options.toFinkArgs(task.getJobNameAndIdMaps(), true));
            String[] args = argsArray.toArray(new String[argsArray.size()]);
            StringBuilder runJobArg = new StringBuilder();
            for (String arg : args) {
                runJobArg.append(arg).append(" ");
            }
            logger.info("runjob args: " + runJobArg.toString());
            String jobId = RunJobCommand.run(runJobArg.toString(), options.getJarFileName().getValue().toString(),
                    event.getContext().getTaskInfo().getScheduleId(), event.getContext().getExecuteInfo().getId(),
                    event.getContext().getTaskInfo().getType().getEnv(),
                    event.getContext().getTaskInfo().getType().getName(), configurationDirectory);
            if (task.getJobNameAndIdMaps() == null) {
                task.setJobNameAndIdMaps(new HashMap<String, String>());
            }
            String jobName = options.getJobName().getValue().toString();
            task.getJobNameAndIdMaps().put(jobName, jobId);
            //register job submit resource info
            try {
                task.registerJobSubmitInstances(jobName, applicationId);
                task.doJobNaviSavepoint();
            } catch (Exception e) {
                logger.warn("Failed to register job submit resource info");
            }
        } catch (Throwable e) {
            result.setSuccess(false);
            result.setProcessInfo(e.getMessage());
            logger.error("run job error.", e);
        }
        return result;
    }

    protected void validateYarnModeResource(FlinkOptions options, String queueName) throws NaviException {
        Long resourceCheck;
        Object resourceCheckObj = options.getResourceCheck().getValue();
        if (resourceCheckObj == null) {
            int containerNum = Integer.parseInt(options.getParallelism().getValue().toString());
            logger.info("check resource may start " + containerNum + " container.");
            Long taskManagerMemory = options.getTaskManagerMemory().getValue() == null ? 3 * 1024 :
                    Long.parseLong(options.getTaskManagerMemory().getValue().toString());
            resourceCheck = taskManagerMemory * containerNum;
        } else {
            resourceCheck = Long.parseLong(resourceCheckObj.toString());
        }
        if (!YarnUtil.isValidSubmitApplication(queueName, resourceCheck)) {
            String msg = "queue [" + queueName + "] has no avaliable resource.";
            String returnValue = "{\"code\":" + FlinkSubmitTask.INSUFFICIENT_RESOURCES + ",\"message\":\"" + msg
                    + "\"}";
            throw new NaviException(returnValue);
        }
    }
}
