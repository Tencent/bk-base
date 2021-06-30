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
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.log4j.Logger;

public class FlinkSubmitTaskV2 extends FlinkSubmitTask {

    private Logger logger1 = Logger.getLogger(FlinkSubmitTaskV2.class);

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        logger1 = getTaskLogger();
        String path = System.getProperty("JOBNAVI_HOME");
        String envName = event.getContext().getTaskInfo().getType().getEnv();
        configurationDirectory = path + "/env/" + envName + "/conf";
        File configDirectory = new File(configurationDirectory);
        logger1.info("Using configuration directory " + configDirectory.getAbsolutePath());
        config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        String extraInfo = event.getContext().getTaskInfo().getExtraInfo();
        String typeName = event.getContext().getTaskInfo().getType().getName();
        FlinkOptionsV2 options = new FlinkOptionsV2(extraInfo, config, typeName);
        mode = options.getMode().getValue() == null ? YarnMode.YARN_SESSION
                : YarnMode.fromModeValue(options.getMode().getValue().toString());

        execId = event.getContext().getExecuteInfo().getId();
        getScheduleInstance().setInstId(Long.toString(execId));
        Map<String, String> jobnaviSavepoint = getScheduleSavepoint();
        ApplicationReport report = getFlinkYarnApplication(jobnaviSavepoint);
        logger1.info("our running mode is " + mode.getValue());
        if (report != null) {
            logger1.info("flink application is running...");
            loadFromJobNaviSavepoint(report, jobnaviSavepoint);
            status = FlinkYarnApplicationStatus.deployed;
            //reload yarn instance info from savepoint
            flinkYarnInstance = getFlinkYarnInstance(options, jobnaviSavepoint);
            if (flinkYarnInstance.getInstId() == null) {
                //yarn instance info not found, register new job submit and write it to savepoint
                flinkYarnInstance.setInstId(applicationId);
                doJobNaviSavepoint();
                if (mode == YarnMode.YANRN_CLUSTER) {
                    String jobName = scheduleId;
                    registerJobSubmitInstances(jobName, applicationId);
                }
            } else {
                //register schedule instance again
                for (Map.Entry<String, Long> entry : submitIdMap.entrySet()) {
                    Long submitId = entry.getValue();
                    registerScheduleInstance(submitId);
                }
            }
        } else {
            //get available yarn queue
            flinkYarnInstance = getFlinkYarnInstance(options, null);
            options.getYarnQueue().setValue(flinkYarnInstance.getClusterName());
            if (mode == YarnMode.YANRN_CLUSTER) {
                report = YarnUtil.getApplicationByName(event.getContext().getTaskInfo().getScheduleId());
                if (null == report || !"FLINK".equalsIgnoreCase(report.getApplicationType())) {
                    submitYarn(options, event);
                } else {
                    throw new NaviException(" yarn-cluster app name "
                            + event.getContext().getTaskInfo().getScheduleId() + " already exists.");
                }
            } else {
                options.setModeValue(YarnMode.YARN_SESSION.getValue());
                submitYarnSession(options, event);
            }
        }
        while (!this.isFinish) {
            Thread.sleep(5000);
        }
        logger1.info("stop process...");
    }

    @Override
    public EventListener getEventListener(String eventName) {
        if ("run_job".equals(eventName)) {
            return new RunJobListenerV2(this);
        } else if ("stop_job".equals(eventName)) {
            return new StopJobListenerV2(this);
        } else if ("sync_job".equals(eventName)) {
            return new SyncJobListener(this);
        } else if ("job_resource".equals(eventName)) {
            return new JobResourceListener(this);
        } else if ("application_status".equals(eventName)) {
            return new ApplicationStatusListener(this);
        } else if ("application_exist".equals(eventName)) {
            return new ApplicationExistListener(this);
        } else if ("kill_application".equals(eventName)) {
            return new KillApplicationListener(this);
        } else if ("job_exception".equals(eventName)) {
            return new JobExceptionListener(this);
        } else if ("cluster_overview".equals(eventName)) {
            return new ClusterOverviewListener(this);
        }
        return null;
    }

    private void submitYarn(FlinkOptions options, TaskEvent event) throws Exception {
        try {
            validateYarnModeResource(options);
            logger1.info("auto generate jars from code...");
            String env = event.getContext().getTaskInfo().getType().getEnv();
            CustomJarsUtils.autoGenJarsAndMerge(options, scheduleId, env);
            logger1.info("start flink yarn...");

            options.getJobName().setValue(event.getContext().getTaskInfo().getScheduleId());
            List<String> argsArray = new ArrayList<>();
            argsArray.add("run");
            argsArray.add("-ynm");
            argsArray.add(scheduleId);
            argsArray.add("--allowNonRestoredState");
            argsArray.add("-yd");
            argsArray.add("-m");
            argsArray.add("yarn-cluster");
            Collections.addAll(argsArray, options.toFinkArgs(this.getJobNameAndIdMaps(), true));
            String[] args = argsArray.toArray(new String[argsArray.size()]);
            StringBuilder runJobArg = new StringBuilder();
            for (String arg : args) {
                runJobArg.append(arg).append(" ");
            }
            logger1.info("runjob args: " + runJobArg.toString());

            applicationId = YarnClusterCommandV2.run(
                    runJobArg.toString(),
                    event.getContext().getExecuteInfo().getId(),
                    env,
                    event.getContext().getTaskInfo().getType().getName()
            );
            // get report wait some seconds
            ApplicationReport report = reportForSubmitYarn(applicationId);
            webInterfaceUrl = getWebInterfaceURL(report);
            logger1.info("web interface: " + webInterfaceUrl);
            logger1.info("application id: " + applicationId);
            status = FlinkYarnApplicationStatus.deployed;
            //register job submit resource info
            String jobName = scheduleId;
            registerJobSubmitInstances(jobName, applicationId);
            doJobNaviSavepoint();
        } catch (Throwable e) {
            logger1.error("submit flink yarn error.", e);
            throw e;
        }
    }

    private void submitYarnSession(FlinkOptions flinkOptions, TaskEvent event) throws Exception {
        validateYarnSessionModeResource(flinkOptions);
        logger1.info("start yarn session...");
        StringBuilder runJobArg = new StringBuilder();
        // detached
        runJobArg.append("-d").append(" ");
        runJobArg.append("-nm").append(" ").append(scheduleId).append(" ");
        String[] argsArray = flinkOptions.toFinkArgs(this.getJobNameAndIdMaps(), false);
        for (String arg : argsArray) {
            // 去掉yarn的前缀
            if (arg.startsWith("-y") && arg.length() > 2) {
                arg = "-" + arg.substring(2);
            }
            runJobArg.append(arg).append(" ");
        }
        logger1.info("runjob args: " + runJobArg.toString());

        List<String> returnValue = YarnSessionCommandV2.run(runJobArg.toString(),
                event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName()
        );
        applicationId = returnValue.get(0);
        // get report wait some seconds
        ApplicationReport report = reportForSubmitYarn(applicationId);
        webInterfaceUrl = getWebInterfaceURL(report);
        doJobNaviSavepoint();
        status = FlinkYarnApplicationStatus.deployed;
    }
}
