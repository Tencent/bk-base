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
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

public class RunJobListenerV2 extends RunJobListener {

    RunJobListenerV2(FlinkSubmitTask task) {
        super(task);
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        logger.info("Flink Job running...");
        String eventInfo = event.getContext().getEventInfo();
        List<String> argsArray = new ArrayList<>();
        try {
            String typeName = event.getContext().getTaskInfo().getType().getName();
            FlinkOptionsV2 options = new FlinkOptionsV2(eventInfo, config, typeName);

            logger.info("auto generate jars from code...");
            String env = event.getContext().getTaskInfo().getType().getEnv();
            CustomJarsUtils.autoGenJarsAndMerge(options, options.getJobName().getValue().toString(), env);

            ApplicationReport applicationReport = YarnUtil.getApplication(applicationId);
            if (applicationReport == null) {
                throw new NaviException("application " + applicationId + " is not exist");
            }
            validateYarnModeResource(options, applicationReport.getQueue());
            argsArray.add("run");
            argsArray.add("--allowNonRestoredState");
            argsArray.add("-yd");
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

            String jobId = RunJobMainV2.run(
                    runJobArg.toString(),
                    event.getContext().getExecuteInfo().getId(),
                    env,
                    event.getContext().getTaskInfo().getType().getName()
            );

            if (task.getJobNameAndIdMaps() == null) {
                task.setJobNameAndIdMaps(new HashMap<>());
            }
            String jobName = options.getJobName().getValue().toString();
            task.getJobNameAndIdMaps().put(jobName, jobId);
            //register job submit resource info
            task.registerJobSubmitInstances(jobName, applicationId);
        } catch (Throwable e) {
            result.setSuccess(false);
            result.setProcessInfo(e.getMessage());
            logger.error("run job error.", e);
        }
        return result;
    }
}
