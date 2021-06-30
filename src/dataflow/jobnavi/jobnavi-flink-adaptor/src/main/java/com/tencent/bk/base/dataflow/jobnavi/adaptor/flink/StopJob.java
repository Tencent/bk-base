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

import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

public class StopJob {

    protected final Logger logger;

    protected FlinkSubmitTask task;

    StopJob(FlinkSubmitTask task) {
        this.task = task;
        this.logger = task.getLogger();
    }

    /**
     * stop flink job
     *
     * @param event
     * @throws Exception
     */
    public void stop(TaskEvent event) throws Exception {
        logger.info("Flink Job stoping...");
        String applicationId = task.getApplicationId();
        Configuration config = task.getConfig();

        List<String> argsArray = new ArrayList<>();
        argsArray.add("stop");
        argsArray.add("-yid");
        argsArray.add(applicationId);
        String eventInfo = event.getContext().getEventInfo();
        FlinkOptions flinkOptions = new FlinkOptions(eventInfo, config);
        Map<String, String> jobNameAndIdMaps = SyncJob.sync(task);
        Collections.addAll(argsArray, flinkOptions.toFinkArgs(jobNameAndIdMaps, false));

        StringBuilder argStr = new StringBuilder();
        for (String arg : argsArray) {
            argStr.append(arg).append(" ");
        }
        logger.info("args is: " + argStr);

        CliCommand.run(argStr.toString(), event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName());
        logger.info("called stop job, application is: " + applicationId);
    }


    /**
     * yarn cluster 模式停止job
     *
     * @param applicationId
     * @param jobId
     * @param event
     * @throws Exception
     */
    public void stopForYarnCluster(String applicationId, String jobId, TaskEvent event) throws Exception {
        logger.info("Flink Job stoping...");
        List<String> argsArray = new ArrayList<>();
        argsArray.add("stop");
        argsArray.add("-yid");
        argsArray.add(applicationId);
        argsArray.add(jobId);

        StringBuilder argStr = new StringBuilder();
        for (String arg : argsArray) {
            argStr.append(arg).append(" ");
        }
        logger.info("args is: " + argStr);
        CliCommand.run(argStr.toString(), event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName());
        logger.info("called stop job, application is: " + applicationId);
    }

}
