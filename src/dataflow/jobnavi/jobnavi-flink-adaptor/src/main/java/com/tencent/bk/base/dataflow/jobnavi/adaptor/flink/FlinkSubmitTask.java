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

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.metric.MetricReportUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.CLusterInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.JobSubmitInstance;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceGroupInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

public class FlinkSubmitTask extends AbstractJobNaviTask {

    //error code
    public static final int INSUFFICIENT_RESOURCES = 1571022;
    public static final int JOB_IS_MISSING = 1571023;
    public static final int APPLICATION_IS_MISSING = 1571024;
    public static final int APPLICATION_IS_END = 1571025;
    public static final int APPLICATION_TIMEOUT_KILLERD = 1571026;
    public static final int CHECK_YARN_CLUSTER_APP_STATE_WAIT_TIME_MS = 60000;
    protected static final String JOBNAVI_SAVEPOINT_APPLICATIONID = "applicationId";
    protected static final String JOBNAVI_SAVEPOINT_RESOURCE_GROUP_ID = "resourceGroupId";
    protected static final String JOBNAVI_SAVEPOINT_GEOG_AREA_CODE = "geogAreaCode";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_ID = "clusterId";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_NAME = "clusterName";
    protected static final String JOBNAVI_SAVEPOINT_CLUSTER_TYPE = "clusterType";
    protected static final String JOBNAVI_SAVEPOINT_SUBMIT_ID_MAP = "submitIds";
    private static final String FLINK_RESOURCE_TYPE = "processing";
    private static final String SERVICE_TYPE = "stream";
    private static final String COMPONENT_TYPE = "flink";
    private static final int JOBNAVI_SAVEPOINT_LOAD_MAX_RETRY_TIMES = 3;
    private static final String JOBNAVI_FLINK_ERROR_METRIC_TABLE = "jobnavi_flink_error";
    protected Map<String, String> jobNameAndIdMaps;
    protected String applicationId;
    protected String webInterfaceUrl;
    protected Configuration config;
    protected String configurationDirectory;
    protected YarnMode mode;
    protected String scheduleId;
    protected String resourceGroupId;
    protected String geogAreaCode;
    protected long execId;
    //key:flink job name, value:job submit ID on resource center
    protected Map<String, Long> submitIdMap = new HashMap<>();
    protected JobSubmitInstance flinkYarnInstance;
    protected FlinkYarnApplicationStatus status = FlinkYarnApplicationStatus.under_deploy;
    protected boolean isFinish = false;
    private Logger logger = Logger.getLogger(FlinkSubmitTask.class);
    private long webInterfaceExpireTimestamp = 0;

    public Logger getLogger() {
        return logger;
    }

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        logger = getTaskLogger();
        String path = System.getProperty("JOBNAVI_HOME");
        String envName = event.getContext().getTaskInfo().getType().getEnv();
        configurationDirectory = path + "/env/" + envName + "/conf";
        File configDirectory = new File(configurationDirectory);
        logger.info("Using configuration directory " + configDirectory.getAbsolutePath());
        config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        execId = event.getContext().getExecuteInfo().getId();
        String extraInfo = event.getContext().getTaskInfo().getExtraInfo();
        FlinkOptions options = new FlinkOptions(extraInfo, config);
        mode = options.getMode().getValue() == null ? YarnMode.YARN_SESSION
                : YarnMode.fromModeValue(options.getMode().getValue().toString());

        Map<String, String> jobnaviSavepoint = getScheduleSavepoint();
        getScheduleInstance().setInstId(Long.toString(execId));
        ApplicationReport report = getFlinkYarnApplication(jobnaviSavepoint);
        if (report != null) {
            logger.info("flink application is running...");
            loadFromJobNaviSavepoint(report, jobnaviSavepoint);
            status = FlinkYarnApplicationStatus.deployed;
            //reload yarn instance info from savepoint
            flinkYarnInstance = getFlinkYarnInstance(options, jobnaviSavepoint);
            if (flinkYarnInstance.getInstId() == null) {
                //yarn instance info not found, register new job submit and write it to savepoint
                flinkYarnInstance.setInstId(applicationId);
            }
            //register lost job submit resource info
            if (mode == YarnMode.YANRN_CLUSTER) {
                if (submitIdMap.isEmpty()) {
                    String jobName = scheduleId;
                    registerJobSubmitInstances(jobName, applicationId);
                }
            } else {
                for (String jobName : jobNameAndIdMaps.keySet()) {
                    if (!submitIdMap.containsKey(jobName)) {
                        registerJobSubmitInstances(jobName, getApplicationId());
                    }
                }
            }
            doJobNaviSavepoint();
            //register schedule instance again
            for (Map.Entry<String, Long> entry : submitIdMap.entrySet()) {
                Long submitId = entry.getValue();
                registerScheduleInstance(submitId);
            }
        } else {
            //get available yarn queue
            flinkYarnInstance = getFlinkYarnInstance(options, null);
            options.getYarnQueue().setValue(flinkYarnInstance.getClusterName());
            if (mode == YarnMode.YANRN_CLUSTER) {
                submitYarn(options, event);
            } else {
                options.setModeValue(YarnMode.YARN_SESSION.getValue());
                submitYarnSession(options, event);
            }
        }
        while (!this.isFinish) {
            Thread.sleep(5000);
        }
        logger.info("stop process...");
    }

    @Override
    public EventListener getEventListener(String eventNameStr) {
        ConstantVar.EventName eventName = ConstantVar.EventName.valueOf(eventNameStr);
        switch (eventName) {
            case run_job:
                return new RunJobListener(this);
            case cancel_job:
                return new CancelJobListener(this);
            case sync_job:
                return new SyncJobListener(this);
            case job_resource:
                return new JobResourceListener(this);
            case application_status:
                return new ApplicationStatusListener(this);
            case application_exist:
                return new ApplicationExistListener(this);
            case kill_application:
                return new KillApplicationListener(this);
            case job_exception:
                return new JobExceptionListener(this);
            case cluster_overview:
                return new ClusterOverviewListener(this);
            case get_resources:
                return new GetResourcesListener(this);
            case jobs_overview:
                return new JobsOverviewListener(this);
            default:
                return null;
        }
    }

    /**
     * save job state data
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
        if (flinkYarnInstance != null) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_ID, flinkYarnInstance.getClusterId());
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_NAME, flinkYarnInstance.getClusterName());
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_CLUSTER_TYPE, flinkYarnInstance.getClusterType());
        }
        if (!submitIdMap.isEmpty()) {
            jobnaviSavepoint.put(JOBNAVI_SAVEPOINT_SUBMIT_ID_MAP, JsonUtils.writeValueAsString(submitIdMap));
        }
        doScheduleSavepoint(jobnaviSavepoint);
    }

    Map<String, String> getJobNameAndIdMaps() {
        return jobNameAndIdMaps;
    }

    void setJobNameAndIdMaps(Map<String, String> jobNameAndIdMaps) {
        this.jobNameAndIdMaps = jobNameAndIdMaps;
    }

    String getApplicationId() {
        return applicationId;
    }

    Configuration getConfig() {
        return config;
    }

    String getConfigurationDirectory() {
        return configurationDirectory;
    }

    /**
     * 当获取 web interface url 为 http://host:port 时，需要重新获取
     * 当获取 web interface url 为 http://host:port/proxy/appication_id/ 时，不需要重新获取
     *
     * @return web interface url
     */
    String getWebInterfaceUrl() {
        final long WEB_INTERFACE_URL_CACHE_LIFESPAN_SECOND = 60;
        if (!webInterfaceUrl.contains("application") || System.currentTimeMillis() >= webInterfaceExpireTimestamp) {
            try {
                ApplicationReport report = YarnUtil.getApplication(applicationId);
                if (report != null) {
                    webInterfaceUrl = getWebInterfaceURL(report);
                    webInterfaceExpireTimestamp = System.currentTimeMillis() + WEB_INTERFACE_URL_CACHE_LIFESPAN_SECOND;
                }
            } catch (IOException | YarnException e1) {
                logger.warn("Failed to get web interface url.", e1);
            }
        }

        // 当 url 为 http://host:port 时，需要在末尾增加 /
        if (!webInterfaceUrl.endsWith("/")) {
            webInterfaceUrl = webInterfaceUrl + "/";
        }

        return webInterfaceUrl;
    }

    public String getResourceGroupId() {
        return resourceGroupId;
    }

    public String getGeogAreaCode() {
        return geogAreaCode;
    }

    public Map<String, Long> getSubmitIdMap() {
        return submitIdMap;
    }

    void registerJobSubmitInstances(String jobName, String applicationId) {
        List<JobSubmitInstance> jobSubmitInstanceList = new ArrayList<>();
        jobSubmitInstanceList.add(getScheduleInstance());
        if (flinkYarnInstance != null) {
            jobSubmitInstanceList.add(flinkYarnInstance);
            flinkYarnInstance.setInstId(applicationId);
        }
        Long submitId = ResourceUtil.registerInstances(jobName, resourceGroupId, geogAreaCode, jobSubmitInstanceList);
        if (submitId != null) {
            submitIdMap.put(jobName, submitId);
        } else {
            logger.warn("Failed to register job submit instances");
        }
    }

    void registerScheduleInstance(Long submitId) {
        List<JobSubmitInstance> jobSubmitInstanceList = new ArrayList<>();
        jobSubmitInstanceList.add(getScheduleInstance());
        if (ResourceUtil.registerInstances(submitId, jobSubmitInstanceList) == null) {
            logger.warn("Failed to register schedule instances");
        }
    }

    void reset() throws Exception {
        this.webInterfaceUrl = null;
        this.applicationId = null;
        this.status = FlinkYarnApplicationStatus.under_deploy;
        doJobNaviSavepoint();
    }

    void finish() {
        logger.info("set flink task finish.");
        this.isFinish = true;
    }

    /**
     * get job name by job ID
     *
     * @param jobId
     * @return
     */
    String getJobNameById(String jobId) {
        for (Map.Entry<String, String> entrySet : jobNameAndIdMaps.entrySet()) {
            if (entrySet.getValue().equals(jobId)) {
                return entrySet.getKey();
            }
        }
        return null;
    }

    protected ApplicationReport getFlinkYarnApplication(Map<String, String> jobnaviSavepoint) throws Exception {
        logger.info("get save point: " + JsonUtils.writeValueAsString(jobnaviSavepoint));
        if (jobnaviSavepoint != null && StringUtils.isNotEmpty(jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID))) {
            String applicationId = jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID);
            return YarnUtil.getApplication(applicationId);
        }
        return null;
    }

    protected void loadFromJobNaviSavepoint(ApplicationReport report,
            Map<String, String> jobnaviSavepoint) throws Exception {
        applicationId = jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID);
        webInterfaceUrl = getWebInterfaceURL(report);
        logger.info("load from jobnavi savepoint, applicationId: " + applicationId + " ,webInterfaceUrl: "
                + webInterfaceUrl);
        int retryTimes = 0;
        while (true) {
            try {
                SyncJob.syncRunningJob(this);
                break;
            } catch (Exception e) {
                if (++retryTimes > JOBNAVI_SAVEPOINT_LOAD_MAX_RETRY_TIMES) {
                    logger.error("failed to load jobnavi savepoint (retry times:" + retryTimes + "), detail:" + e);
                    //report recover error to alert
                    reportError("recover");
                    throw e;
                } else {
                    logger.error("failed to load jobnavi savepoint (retry times:" + retryTimes + "), detail:" + e);
                    Thread.sleep(10000 * retryTimes);
                }
            }
        }
        //reload job name -> submit ID map from savepoint
        if (jobnaviSavepoint.containsKey(JOBNAVI_SAVEPOINT_SUBMIT_ID_MAP)
                && JsonUtils.validateJson(jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_SUBMIT_ID_MAP))) {
            Map<String, Object> submitIdMap = JsonUtils.readMap(jobnaviSavepoint.get(JOBNAVI_SAVEPOINT_SUBMIT_ID_MAP));
            for (Map.Entry<String, Object> entry : submitIdMap.entrySet()) {
                if (entry.getValue() instanceof Integer || entry.getValue() instanceof Long) {
                    this.submitIdMap.put(entry.getKey(), Long.parseLong(entry.getValue().toString()));
                }
            }
        }
    }

    protected String getWebInterfaceURL(ApplicationReport report) {
        String trackingURL = report.getTrackingUrl();
        // there seems to be a difference between HD 2.2.0 and 2.6.0
        if (!trackingURL.startsWith("http://")) {
            return "http://" + trackingURL;
        } else {
            return trackingURL;
        }
    }

    private void submitYarn(FlinkOptions options, TaskEvent event) throws Exception {
        try {
            validateYarnModeResource(options);
            logger.info("start flink yarn...");

            options.getJobName().setValue(event.getContext().getTaskInfo().getScheduleId());
            List<String> argsArray = new ArrayList<>();
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
            logger.info("runjob args: " + runJobArg.toString());
            //applicationId = runJob.getApplicationId();
            applicationId = YarnClusterCommand.run(runJobArg.toString(), options.getJarFileName().getValue().toString(),
                    event.getContext().getTaskInfo().getScheduleId(), event.getContext().getExecuteInfo().getId(),
                    event.getContext().getTaskInfo().getType().getEnv(),
                    event.getContext().getTaskInfo().getType().getName(), configurationDirectory);
            // get report wait some seconds
            ApplicationReport report = reportForSubmitYarn(applicationId);
            // 停止yarn cluster 模式重复的app
            stopDuplicateYarnClusterApp(applicationId, report, event);
            webInterfaceUrl = getWebInterfaceURL(report);
            logger.info("web interface: " + webInterfaceUrl);
            logger.info("application id: " + applicationId);
            status = FlinkYarnApplicationStatus.deployed;
            //register job submit resource
            String jobName = scheduleId;
            registerJobSubmitInstances(jobName, applicationId);
            doJobNaviSavepoint();
        } catch (Throwable e) {
            logger.error("submit flink yarn error.", e);
            throw e;
        }
    }

    /**
     * 验证Application状态，然后返回ApplicationReport。
     * 1、如果application在 60 s内都没有达到running，则killed任务，然后抛出任务结束异常
     * 2、如果任务是结束状态，抛出任务结束异常。
     *
     * @param applicationId
     * @return yarn application report
     * @throws IOException
     * @throws YarnException
     * @throws NaviException
     * @throws InterruptedException
     */
    protected ApplicationReport reportForSubmitYarn(String applicationId)
            throws IOException, YarnException, NaviException, InterruptedException {
        ApplicationReport report = YarnUtil.getApplicationInAllState(applicationId);
        long startTime = System.currentTimeMillis();
        boolean appIsRunning = false;
        while (!appIsRunning && (System.currentTimeMillis() - startTime) < CHECK_YARN_CLUSTER_APP_STATE_WAIT_TIME_MS) {
            if (null != report) {
                switch (report.getYarnApplicationState()) {
                    case NEW:
                    case NEW_SAVING:
                    case SUBMITTED:
                    case ACCEPTED: {
                        // 未执行继续重试
                        break;
                    }
                    case RUNNING: {
                        // 运行中，
                        appIsRunning = true;
                        break;
                    }
                    default: {
                        // 其他状态，已经结束
                        String msg = "application is end.";
                        String returnValue = "{\"code\":" + FlinkSubmitTask.APPLICATION_IS_END + ",\"message\":\"" + msg
                                + "\"}";
                        throw new NaviException(returnValue);
                    }
                }
            }
            if (!appIsRunning) {
                Thread.sleep(1000);
                report = YarnUtil.getApplicationInAllState(applicationId);
            }
        }
        if (null == report || report.getYarnApplicationState() != YarnApplicationState.RUNNING) {
            YarnUtil.killApplication(applicationId);
            String msg = "submit application time out to killed.";
            String returnValue = "{\"code\":" + FlinkSubmitTask.APPLICATION_TIMEOUT_KILLERD + ",\"message\":\"" + msg
                    + "\"}";
            throw new NaviException(returnValue);
        }
        return report;
    }


    /**
     * 停止YarnCluster模式重复的app<br/>
     * 因为在某些负载（或网络）问题下，可能提交到yarn中任务成功，但是暂时检测不到，后来yarn中又运行起来了。<br/>
     * 处理过程：<br/>
     * 1、先使用flink 的stop命令，关闭source（uc扩展实现的功能）<br/>
     * 2、stop 执行之后5秒，开始kill app。（stop flink 不会修改job状态）<br/>
     *
     * @param applicationId
     * @param report
     * @param event
     * @throws IOException
     * @throws YarnException
     */
    private void stopDuplicateYarnClusterApp(String applicationId, ApplicationReport report, TaskEvent event)
            throws IOException, YarnException {
        if (report != null) {
            String name = report.getName().trim();
            while (true) {
                // 如果运行中、检查yarn是否存在同名的任务，若存在 则 stop + kill 另外一个。
                ApplicationReport other = YarnUtil.getApplicationByName(name, applicationId);
                if (null != other) {
                    String jobId = getJobIdForYarnCluster(getWebInterfaceURL(other));
                    String appId = other.getApplicationId().toString();
                    if (StringUtils.isNotBlank(jobId)) {
                        try {
                            logger.info("to stop duplicate app, appId is " + appId + ", jobId is " + jobId);
                            StopJob stop = new StopJob(this);
                            stop.stopForYarnCluster(appId, jobId, event);
                            // 等待5秒，stop flink 不会修改job状态，无法确认是否执行完成stop。
                            Thread.sleep(1000 * 5);
                        } catch (Exception e) {
                            logger.warn("stop job error, appiId is " + appId + ", jobId is " + jobId);
                        }
                    }
                    logger.info("to kill duplicate app, id is " + other.getApplicationId().toString());
                    YarnUtil.killApplication(other.getApplicationId().toString());
                } else {
                    break;
                }
            }
        }
    }

    protected void validateYarnModeResource(FlinkOptions options) throws NaviException {
        Long resourceCheck;
        Object resourceCheckObj = options.getResourceCheck().getValue();
        if (resourceCheckObj == null) {
            int containerNum = (int) Math
                    .ceil(Double.parseDouble(options.getParallelism().getValue().toString()) / Double
                            .parseDouble(options.getSlot().getValue().toString()));
            logger.info("check resource may start " + containerNum + " container.");
            resourceCheck = Long.parseLong(options.getTaskManagerMemory().getValue().toString()) * containerNum
                    + Long.parseLong(options.getJobManangerMemory().getValue().toString());
        } else {
            resourceCheck = Long.parseLong(resourceCheckObj.toString());
        }
        String queue = options.getYarnQueue().getValue().toString();
        if (!YarnUtil.isValidSubmitApplication(queue, resourceCheck)) {
            String msg = "queue [" + queue + "] has no avaliable resource.";
            String returnValue = "{\"code\":" + FlinkSubmitTask.INSUFFICIENT_RESOURCES + ",\"message\":\"" + msg
                    + "\"}";
            throw new NaviException(returnValue);
        }
    }

    private void submitYarnSession(FlinkOptions flinkOptions, TaskEvent event) throws Exception {
        validateYarnSessionModeResource(flinkOptions);
        logger.info("start yarn session...");
//    FlinkYarnSessionCli cli = new FlinkYarnSessionCli(config, configurationDirectory, "y", "yarn");
//
//    Options options = new Options();
//    cli.addGeneralOptions(options);
//    cli.addRunOptions(options);
//
//    CommandLineParser parser = new PosixParser();
//    CommandLine cmd;
//    try {
//      cmd = parser.parse(options, flinkOptions.toFinkArgs(this.getJobNameAndIdMaps(), false));
//    } catch (Exception e) {
//      logger.error("parse param error.", e);
//      throw e;
//    }
//    AbstractYarnClusterDescriptor yarnDescriptor = cli.createClusterDescriptor(cmd);
//    yarnDescriptor.setName(event.getContext().getTaskInfo().getScheduleId());
//    final ClusterSpecification clusterSpecification = cli.getClusterSpecification(cmd);
//    ClusterClient client = yarnDescriptor.deploySessionCluster(clusterSpecification);
//    final LeaderConnectionInfo connectionInfo = client.getClusterConnectionInfo();
//    logger.info("Flink JobManager is now running on " + connectionInfo.getHostname() +
//        ':' + connectionInfo.getPort() + " with leader id " + connectionInfo.getLeaderSessionID() + '.');
//    logger.info("JobManager Web Interface: " + client.getWebInterfaceURL());
//    client.waitForClusterToBeReady();
        StringBuilder runJobArg = new StringBuilder();
        String[] argsArray = flinkOptions.toFinkArgs(this.getJobNameAndIdMaps(), false);
        for (String arg : argsArray) {
            runJobArg.append(arg).append(" ");
        }
        logger.info("runjob args: " + runJobArg.toString());

        List<String> returnValue = YarnSessionCommand.run(runJobArg.toString(),
                event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getExecuteInfo().getId(),
                event.getContext().getTaskInfo().getType().getEnv(),
                event.getContext().getTaskInfo().getType().getName(), configurationDirectory);
        applicationId = returnValue.get(0);
        webInterfaceUrl = returnValue.get(1);
        doJobNaviSavepoint();
        status = FlinkYarnApplicationStatus.deployed;
    }

    protected void validateYarnSessionModeResource(FlinkOptions options) throws NaviException {
        Long resourceCheck;
        Object resourceCheckObj = options.getResourceCheck().getValue();
        if (resourceCheckObj == null) {
            resourceCheck = Long.parseLong(options.getJobManangerMemory().getValue().toString());
        } else {
            resourceCheck = Long.parseLong(resourceCheckObj.toString());
        }
        String queue = options.getYarnQueue().getValue().toString();
        if (!YarnUtil.isValidSubmitApplication(queue, resourceCheck)) {
            throw new NaviException("queue [" + queue + "] has no avaliable resource.");
        }
    }

    /**
     * 获取flink yarn cluster模式的jobId
     *
     * @param webInterfaceUrl
     * @return
     */
    private String getJobIdForYarnCluster(String webInterfaceUrl) {
        try {
            String jobURL = webInterfaceUrl + "/jobs";
            logger.info("get jobs: " + jobURL);
            String jobInfo = HttpUtils.get(jobURL);
            logger.info("jobinfo: " + jobInfo);
            Map<String, Object> json = JsonUtils.readMap(jobInfo);
            if (json != null && json.size() > 0) {
                List<Map<String, String>> jobInfos = (List<Map<String, String>>) json.get("jobs");
                for (Map<String, String> job : jobInfos) {
                    // yarn cluster pre job
                    String runningJobId = job.get("id");
                    return runningJobId;
                }
            }
        } catch (Throwable e) {
            logger.error("get job id error.", e);
        }
        return null;
    }

    public FlinkYarnApplicationStatus getStatus() {
        return status;
    }

    public void setStatus(FlinkYarnApplicationStatus status) {
        this.status = status;
    }

    public YarnMode getMode() {
        return mode;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    private void reportError(String type) {
        try {
            if (!MetricReportUtil.isInitialized()) {
                MetricReportUtil.init();
            }
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("error_count", 1);
            Map<String, Object> tags = new HashMap<>();
            tags.put("schedule_id", scheduleId);
            tags.put("error_type", type);
            MetricReportUtil.report(JOBNAVI_FLINK_ERROR_METRIC_TABLE, metrics, tags);
        } catch (Exception e) {
            logger.error("failed to report error, detail:" + e);
        }
    }

    protected JobSubmitInstance getFlinkYarnInstance(FlinkOptions flinkOptions, Map<String, String> scheduleSavepoint) {
        JobSubmitInstance flinkYarnInstance;
        if (scheduleSavepoint != null) {
            String clusterId = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_ID);
            if (clusterId != null) {
                //reload yarn cluster instance info from savepoint
                resourceGroupId = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_RESOURCE_GROUP_ID);
                geogAreaCode = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_GEOG_AREA_CODE);
                flinkYarnInstance = new JobSubmitInstance();
                flinkYarnInstance.setClusterId(clusterId);
                String clusterName = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_NAME);
                flinkYarnInstance.setClusterName(clusterName);
                flinkYarnInstance.setResourceType(FLINK_RESOURCE_TYPE);
                String clusterType = scheduleSavepoint.get(JOBNAVI_SAVEPOINT_CLUSTER_TYPE);
                flinkYarnInstance.setClusterType(clusterType);
                flinkYarnInstance.setInstId(scheduleSavepoint.get(JOBNAVI_SAVEPOINT_APPLICATIONID));
                return flinkYarnInstance;
            } else {
                return getCustomFlinkYarnInstance((String) flinkOptions.getYarnQueue().getValue());
            }
        }
        resourceGroupId = (String) flinkOptions.getResourceGroupId().getValue();
        geogAreaCode = (String) flinkOptions.getGeogAreaCode().getValue();
        if (resourceGroupId == null || geogAreaCode == null) {
            String clusterGroupId = (String) flinkOptions.getClusterGroupId().getValue();
            ResourceGroupInfo resourceGroupInfo = ResourceUtil.retrieveResourceGroupInfo(clusterGroupId);
            if (resourceGroupInfo != null) {
                resourceGroupId = resourceGroupInfo.getResourceGroupId();
                geogAreaCode = resourceGroupInfo.getGeogAreaCode();
            }
        }
        if (resourceGroupId != null && geogAreaCode != null) {
            logger.info(String.format("Resource group ID:%s, geog area code:%s", resourceGroupId, geogAreaCode));
            CLusterInfo cLusterInfo = ResourceUtil.listClusterInfo(resourceGroupId, geogAreaCode,
                    FLINK_RESOURCE_TYPE, SERVICE_TYPE, COMPONENT_TYPE, getMode().getValue());
            if (cLusterInfo != null && cLusterInfo.getClusterName() != null
                    && cLusterInfo.getClusterName().equals(flinkOptions.getYarnQueue().getValue())) {
                flinkYarnInstance = new JobSubmitInstance();
                flinkYarnInstance.setClusterId(cLusterInfo.getClusterId());
                flinkYarnInstance.setClusterName(cLusterInfo.getClusterName());
                flinkYarnInstance.setResourceType(FLINK_RESOURCE_TYPE);
                flinkYarnInstance.setClusterType(cLusterInfo.getClusterType());
                return flinkYarnInstance;
            }
        }
        return getCustomFlinkYarnInstance((String) flinkOptions.getYarnQueue().getValue());
    }

    private JobSubmitInstance getCustomFlinkYarnInstance(String yarnQueue) {
        //user defined yarn queue
        JobSubmitInstance flinkYarnInstance = new JobSubmitInstance();
        final String RESOURCE_GROUP_ID_CUSTOM = "default.custom";
        final String GEOG_AREA_CODE_DEFAULT = "default";
        final String FLINK_CLUSTER_ID_CUSTOM = "flink.cluster.custom";
        final String RESOURCE_TYPE = "processing";
        final String CLUSTER_TYPE = getMode().getValue();
        resourceGroupId = RESOURCE_GROUP_ID_CUSTOM;
        geogAreaCode = GEOG_AREA_CODE_DEFAULT;
        flinkYarnInstance.setClusterId(FLINK_CLUSTER_ID_CUSTOM);
        flinkYarnInstance.setClusterName(yarnQueue);
        flinkYarnInstance.setResourceType(RESOURCE_TYPE);
        flinkYarnInstance.setClusterType(CLUSTER_TYPE);
        return flinkYarnInstance;
    }
}
