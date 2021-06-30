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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparkstreaming;

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.logging.ThreadLoggingFactory;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.CLusterInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.JobSubmitInstance;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceGroupInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;

public class SparkStreamingSubmitTask extends AbstractJobNaviTask {

    //error code
    public static final int APPLICATION_IS_MISSING = 1571024;
    public static final int INSUFFICIENT_RESOURCES = 1571022;
    protected static final Pattern APPLICATION_REGEX = Pattern.compile("application_\\d+_\\d+");
    protected static final String JOBNAVI_SAVEPOINT_RESOURCE_GROUP_ID = "resourceGroupId";
    protected static final String JOBNAVI_SAVEPOINT_GEOG_AREA_CODE = "geogAreaCode";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_ID = "clusterId";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_NAME = "clusterName";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_TYPE = "clusterType";
    protected static final String JOBNAVI_SAVEPOINT_SUBMIT_ID = "submitId";
    private static final String SPARK_RESOURCE_TYPE = "processing";
    private static final String SERVICE_TYPE = "stream";
    private static final String COMPONENT_TYPE = "spark";
    private static final String SPARK_CLUSTER_TYPE_CONF_KEY = "spark.cluster.type";
    private static final String JOBNAVI_SAVEPOINT_APPLICATIONID = "applicationId";
    protected String resourceGroupId;
    protected String geogAreaCode;
    protected long execId;
    protected Long submitId;
    protected JobSubmitInstance sparkYarnInstance;
    private Logger logger = Logger.getLogger(SparkStreamingSubmitTask.class);
    private String configurationDirectory;
    private Properties config;
    private String scheduleId;
    private boolean isFinish = false;
    private String applicationId;
    private List<String> sparkApplicationIds;
    private String webInterfaceUrl;
    private SparkYarnApplicationStatus status = SparkYarnApplicationStatus.under_deploy;

    public String getScheduleId() {
        return scheduleId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public List<String> getSparkApplicationIds() {
        return sparkApplicationIds;
    }

    public void setSparkApplicationIds(List<String> sparkApplicationIds) {
        this.sparkApplicationIds = sparkApplicationIds;
    }

    public String getWebInterfaceUrl() {
        return webInterfaceUrl;
    }

    public SparkYarnApplicationStatus getStatus() {
        return status;
    }

    public Long getSubmitId() {
        return submitId;
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        logger = getTaskLogger();
        String path = System.getProperty("JOBNAVI_HOME");
        String type = event.getContext().getTaskInfo().getType().getName();
        String env = event.getContext().getTaskInfo().getType().getEnv();
        String jobName = event.getContext().getTaskInfo().getScheduleId();
        execId = event.getContext().getExecuteInfo().getId();

        configurationDirectory = path + "/adaptor/" + type + "/conf";
        scheduleId = event.getContext().getTaskInfo().getScheduleId();

        String extraInfo = event.getContext().getTaskInfo().getExtraInfo();
        config = new Properties();
        try (FileInputStream configFile = new FileInputStream(configurationDirectory + "/" + "config.properties")) {
            config.load(configFile);
        } catch (Exception e) {
            logger.error("Failed to config file", e);
            throw e;
        }

        SparkStreamingOptions options = new SparkStreamingOptions(scheduleId, extraInfo, config, type);
        Map<String, String> jobnaviSavepoint = getScheduleSavepoint();
        getScheduleInstance().setInstId(Long.toString(execId));
        ApplicationReport report = getSparkStreamingYarnApplication(jobnaviSavepoint);
        if (report != null) {
            logger.info("sparkStreaming application is running...");
            loadFromJobNaviSavepoint(report, jobnaviSavepoint);
            status = SparkYarnApplicationStatus.deployed;
            //reload yarn instance info from savepoint
            sparkYarnInstance = getSparkYarnInstance(options, jobnaviSavepoint);
            if (sparkYarnInstance.getInstId() == null) {
                //yarn instance info not found, register new job submit and write it to savepoint
                sparkYarnInstance.setInstId(applicationId);
            }
            if (submitId == null) {
                registerJobSubmitInstances(jobName, applicationId);
            }
            doJobNaviSavepoint();
            //register schedule instance again
            registerScheduleInstance(submitId);
        } else {
            //get available yarn queue
            sparkYarnInstance = getSparkYarnInstance(options, null);
            options.getQueue().setValue(sparkYarnInstance.getClusterName());
            submitToYarn(path, type, env, jobName, execId, options);
        }
        while (!this.isFinish) {
            Thread.sleep(5000);
        }
        logger.info("stop process...");
    }

    void registerJobSubmitInstances(String jobName, String applicationId) {
        List<JobSubmitInstance> jobSubmitInstanceList = new ArrayList<>();
        jobSubmitInstanceList.add(getScheduleInstance());
        if (sparkYarnInstance != null) {
            jobSubmitInstanceList.add(sparkYarnInstance);
            sparkYarnInstance.setInstId(applicationId);
        }
        submitId = ResourceUtil.registerInstances(jobName, resourceGroupId, geogAreaCode, jobSubmitInstanceList);
        if (submitId == null) {
            logger.warn("Failed to register job submit instances");
        }
    }

    void registerScheduleInstance(Long submitId) {
        List<JobSubmitInstance> jobSubmitInstanceList = new ArrayList<>();
        jobSubmitInstanceList.add(getScheduleInstance());
        if (ResourceUtil.registerInstances(submitId, jobSubmitInstanceList) == null) {
            logger.warn("Failed to register schedule instances for submit:" + submitId);
        }
    }

    private void submitToYarn(String path, String type, String env, String jobName, long execId,
            SparkStreamingOptions options) throws Exception {

        validateYarnModeResource(options);
        String jobArgs = options.toSparkStreamingArgs();
        String rootLogPath = ThreadLoggingFactory.getLoggerRootPath();
        // 目录结构需要和yarn_cluster_command.sh中的log4j.log.dir一致
        String jobFile = rootLogPath + "/exec_" + execId + "/job-args.info";
        if (StringUtils.isBlank(rootLogPath)) {
            throw new NaviException("rootLogPath is not set.");
        }
        try {
            // 生产任务脚本
            createJobFile(jobArgs, jobFile);
            String shellPath = path + "/adaptor/" + type + "/bin/yarn_cluster_command.sh";

            String startCommand = shellPath + " "
                    + execId + " "
                    + env + " "
                    + rootLogPath + " "
                    + jobName + " "
                    + jobFile;
            logger.info(startCommand);
            // 独立进程启动，防止加载依赖包的问题。
            applicationId = this.runCommand(startCommand);
            ApplicationReport report = YarnUtil.getApplicationInAllState(applicationId);
            int i = 0;
            while (i < 30 && !this.isFinish) {
                i++;
                if (null != report) {
                    if (report.getYarnApplicationState() == YarnApplicationState.RUNNING
                            || report.getYarnApplicationState() == YarnApplicationState.FINISHED
                            || report.getYarnApplicationState() == YarnApplicationState.FAILED
                            || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
                        break;
                    }
                }
                Thread.sleep(1000);
                report = YarnUtil.getApplicationInAllState(applicationId);
            }
            if (report == null || report.getYarnApplicationState() != YarnApplicationState.RUNNING) {
                YarnUtil.killApplication(applicationId);
                throw new NaviException("Failed to submit job:" + jobName + " to yarn");
            }
            webInterfaceUrl = getWebInterfaceURL(report);
            logger.info("web interface: " + webInterfaceUrl);
            logger.info("application id: " + applicationId);
            status = SparkYarnApplicationStatus.deployed;
            registerJobSubmitInstances(jobName, applicationId);
            doJobNaviSavepoint();
        } finally {
            // FileUtils.deleteQuietly(new File(jobFile));
        }
    }

    @Override
    public EventListener getEventListener(String eventName) {
        if ("sync_job".equals(eventName)) {
            return new SyncJobListener(this);
        } else if ("job_resource".equals(eventName)) {
            return new JobExecutorsListener(this);
        } else if ("application_status".equals(eventName)) {
            return new ApplicationStatusListener(this);
        } else if ("application_exist".equals(eventName)) {
            return new ApplicationExistListener(this);
        } else if ("kill_application".equals(eventName)) {
            return new KillApplicationListener(this);
        } else if ("job_executors".equals(eventName)) {
            return new JobExecutorsListener(this);
        }
        return null;
    }

    void reset() throws Exception {
        this.webInterfaceUrl = null;
        this.applicationId = null;
        this.status = SparkYarnApplicationStatus.under_deploy;
        doJobNaviSavepoint();
    }

    void finish() {
        logger.info("set sparkStreaming task finish.");
        this.isFinish = true;
    }

    /**
     * save state data
     *
     * @throws Exception
     */
    public void doJobNaviSavepoint() throws Exception {
        Map<String, String> jobnaviSavepoint = new HashMap<>();
        if (StringUtils.isNotEmpty(applicationId)) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_APPLICATIONID, applicationId);
        }
        if (StringUtils.isNotEmpty(resourceGroupId)) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_RESOURCE_GROUP_ID, resourceGroupId);
        }
        if (StringUtils.isNotEmpty(geogAreaCode)) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_GEOG_AREA_CODE, geogAreaCode);
        }
        if (sparkYarnInstance != null) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_ID, sparkYarnInstance.getClusterId());
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_NAME, sparkYarnInstance.getClusterName());
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_TYPE, sparkYarnInstance.getClusterType());
        }
        if (submitId != null) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_SUBMIT_ID, submitId.toString());
        }
        doScheduleSavepoint(jobnaviSavepoint);
    }

    protected void createJobFile(String jobArgs, String jobFile) throws IOException {
        logger.info("jobArgs:" + jobArgs);
        logger.info("create job file:" + jobFile);
        FileUtils.writeStringToFile(new File(jobFile), jobArgs, StandardCharsets.UTF_8);
    }


    private ApplicationReport getSparkStreamingYarnApplication(Map<String, String> jobnaviSavepoint) throws Exception {
        logger.info("get save point: " + JsonUtils.writeValueAsString(jobnaviSavepoint));
        if (jobnaviSavepoint != null && StringUtils.isNotEmpty(jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID))) {
            String applicationId = jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID);
            logger.info("reload application ID:" + applicationId);
            ApplicationReport report = YarnUtil.getApplicationInAllState(applicationId);
            if (null != report) {
                logger.info("application state:" + report.getYarnApplicationState());
                switch (report.getYarnApplicationState()) {
                    case NEW:
                    case NEW_SAVING:
                    case SUBMITTED:
                    case ACCEPTED:
                    case RUNNING:
                        // 返回属于非结束的状态
                        return report;
                    default:
                        break;
                }
            }
        } else {
            return YarnUtil.getApplicationByName(this.getScheduleId());
        }
        return null;
    }

    private void loadFromJobNaviSavepoint(ApplicationReport report, Map<String, String> jobnaviSavepoint)
            throws Exception {
        applicationId = jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID);
        webInterfaceUrl = getWebInterfaceURL(report);
        logger.info("load from jobnavi savepoint, applicationId: " + applicationId + " ,webInterfaceUrl: "
                + webInterfaceUrl);
        SyncJob.syncRunningJob(this);
        String submitId = jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_SUBMIT_ID);
        if (submitId != null) {
            this.submitId = Long.parseLong(submitId);
        }
    }

    private String getWebInterfaceURL(ApplicationReport report) {
        String trackingURL = report.getTrackingUrl();
        // there seems to be a difference between HD 2.2.0 and 2.6.0
        if (!trackingURL.startsWith("http://")) {
            return "http://" + trackingURL;
        } else {
            return trackingURL;
        }
    }

    private String runCommand(String startCommand) throws Exception {
        final AtomicBoolean completed = new AtomicBoolean(false);
        ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", startCommand);
        Process process = builder.start();
        final BufferedReader isReader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        try {
            final List<String> processList = new ArrayList<>();
            try {
                String line = null;
                while ((line = isReader.readLine()) != null) {
                    logger.info(line);
                    String appId = findAppId(line);
                    if (null != appId) {
                        processList.add(appId);
                    }
                }
            } catch (Exception ioe) {
                logger.warn("Error reading the in stream", ioe);
            }
            process.waitFor();
            completed.set(true);
            int exit = process.exitValue();
            logger.info("process exit value is " + exit);
            if (exit != 0) {
                throw new NaviException("Command " + startCommand + " error.");
            }
            if (processList.isEmpty()) {
                throw new NaviException("Command " + startCommand + " unable to get applicationId error.");
            }
            String applicationId = processList.get(0);
            logger.info("application Id is " + applicationId);
            return applicationId;
        } finally {
            try {
                isReader.close();
            } catch (Exception e) {
                logger.warn("Failed to close reader");
            }
        }
    }

    private String findAppId(String line) {
        Matcher matcher = APPLICATION_REGEX.matcher(line);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    protected JobSubmitInstance getSparkYarnInstance(SparkStreamingOptions sparkStreamingOptions,
            Map<String, String> scheduleSavepoint) {
        JobSubmitInstance sparkYarnInstance;
        if (scheduleSavepoint != null) {
            String clusterId = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_ID);
            if (clusterId != null) {
                //reload yarn cluster instance info from savepoint
                resourceGroupId = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_RESOURCE_GROUP_ID);
                geogAreaCode = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_GEOG_AREA_CODE);
                sparkYarnInstance = new JobSubmitInstance();
                sparkYarnInstance.setClusterId(clusterId);
                String clusterName = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_NAME);
                sparkYarnInstance.setClusterName(clusterName);
                sparkYarnInstance.setResourceType(SPARK_RESOURCE_TYPE);
                String clusterType = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_TYPE);
                sparkYarnInstance.setClusterType(clusterType);
                sparkYarnInstance.setInstId(scheduleSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID));
                return sparkYarnInstance;
            } else {
                return getCustomSparkYarnInstance((String) sparkStreamingOptions.getQueue().getValue());
            }
        }
        resourceGroupId = (String) sparkStreamingOptions.getResourceGroupId().getValue();
        geogAreaCode = (String) sparkStreamingOptions.getGeogAreaCode().getValue();
        if (resourceGroupId == null || geogAreaCode == null) {
            String clusterGroupId = (String) sparkStreamingOptions.getClusterGroupId().getValue();
            ResourceGroupInfo resourceGroupInfo = ResourceUtil.retrieveResourceGroupInfo(clusterGroupId);
            if (resourceGroupInfo != null) {
                resourceGroupId = resourceGroupInfo.getResourceGroupId();
                geogAreaCode = resourceGroupInfo.getGeogAreaCode();
            }
        }
        if (resourceGroupId != null && geogAreaCode != null) {
            logger.info(String.format("Resource group ID:%s, geog area code:%s", resourceGroupId, geogAreaCode));
            CLusterInfo cLusterInfo = ResourceUtil.listClusterInfo(resourceGroupId, geogAreaCode,
                    SPARK_RESOURCE_TYPE, SERVICE_TYPE, COMPONENT_TYPE, config.getProperty(SPARK_CLUSTER_TYPE_CONF_KEY));
            if (cLusterInfo != null && cLusterInfo.getClusterName() != null
                    && cLusterInfo.getClusterName().equals(sparkStreamingOptions.getQueue().getValue())) {
                sparkYarnInstance = new JobSubmitInstance();
                sparkYarnInstance.setClusterId(cLusterInfo.getClusterId());
                sparkYarnInstance.setClusterName(cLusterInfo.getClusterName());
                sparkYarnInstance.setResourceType(SPARK_RESOURCE_TYPE);
                sparkYarnInstance.setClusterType(cLusterInfo.getClusterType());
                return sparkYarnInstance;
            }
        }
        return getCustomSparkYarnInstance((String) sparkStreamingOptions.getQueue().getValue());
    }

    private JobSubmitInstance getCustomSparkYarnInstance(String yarnQueue) {
        //user defined yarn queue
        JobSubmitInstance sparkYarnInstance = new JobSubmitInstance();
        final String RESOURCE_GROUP_ID_CUSTOM = "default.custom";
        final String GEOG_AREA_CODE_DEFAULT = "default";
        final String SPARK_STREAMING_CLUSTER_ID_CUSTOM = "spark.streaming.cluster.custom";
        final String RESOURCE_TYPE = "processing";
        final String CLUSTER_TYPE = config.getProperty(SPARK_CLUSTER_TYPE_CONF_KEY);
        resourceGroupId = RESOURCE_GROUP_ID_CUSTOM;
        geogAreaCode = GEOG_AREA_CODE_DEFAULT;
        sparkYarnInstance.setClusterId(SPARK_STREAMING_CLUSTER_ID_CUSTOM);
        sparkYarnInstance.setClusterName(yarnQueue);
        sparkYarnInstance.setResourceType(RESOURCE_TYPE);
        sparkYarnInstance.setClusterType(CLUSTER_TYPE);
        return sparkYarnInstance;
    }

    protected void validateYarnModeResource(SparkStreamingOptions options) throws NaviException {
        Long resourceCheck;
        Object resourceCheckObj = options.getResourceCheck().getValue();
        if (resourceCheckObj == null) {
            long executorNum = 0L;
            if (options.getNumExecutors().getValue() != null) {
                String executorNumStr = options.getNumExecutors().getValue().toString();
                executorNum = Long.parseLong(executorNumStr);
            }
            long executorMem = 0L;
            if (options.getExecutorMemory().getValue() != null) {
                String executorMemStr = options.getExecutorMemory().getValue().toString();
                executorMemStr = executorMemStr.replaceAll("[^0-9]", "");
                executorMem = Long.parseLong(executorMemStr);
            }
            long driverMem = 0L;
            if (options.getDriverMemory().getValue() != null) {
                String driverMemStr = options.getDriverMemory().getValue().toString();
                driverMemStr = driverMemStr.replaceAll("[^0-9]", "");
                driverMem = Long.parseLong(driverMemStr);
            }
            logger.info("check resource may start " + executorNum + " executor container.");
            resourceCheck = executorMem * executorNum + driverMem;
        } else {
            resourceCheck = Long.parseLong(resourceCheckObj.toString());
        }
        String queue = options.getQueue().getValue().toString();
        if (!YarnUtil.isValidSubmitApplication(queue, resourceCheck)) {
            String msg = "queue [" + queue + "] has no avaliable resource.";
            String returnValue = "{\"code\":" + SparkStreamingSubmitTask.INSUFFICIENT_RESOURCES + ",\"message\":\""
                    + msg + "\"}";
            throw new NaviException(returnValue);
        }
    }

}
