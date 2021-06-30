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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparksql;

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.JobNaviNoDataException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.CLusterInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.JobSubmitInstance;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceGroupInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkSQLSubmitTask extends AbstractJobNaviTask {

    private static final String CONF_PATH_PATTERN = "%s/adaptor/%s/conf";
    private static final String CONF_FILE_NAME = "config.properties";
    private static final String SPARK_DEFAULT_CONF_FILE_NAME = "spark-defaults.conf";
    private static final String SPARK_SESSION_CONF_URL_CONF_KEY = "spark.session.conf.url";
    private static final String SPARK_CLUSTER_TYPE_CONF_KEY = "spark.cluster.type";
    private static final String SPARK_SQL_RESOURCE_TYPE = "processing";
    private static final String SERVICE_TYPE = "batch";
    private static final String COMPONENT_TYPE = "spark";
    private static final String DEPLOY_MASTER = "yarn";
    private static final String DEPLOY_MODE = "client";
    private static final String UC_ENTRYPOINT_CLASS_CONF_KEY = "uc.entrypoint.class";
    private static final String UC_ENTRYPOINT_METHOD_CONF_KEY = "uc.entrypoint.method";
    private static final String UC_EXCEPTION_NO_DATA_CLASS_CONF_KEY = "uc.exception.no_data.class";
    private static Logger LOGGER = Logger.getLogger(SparkSQLSubmitTask.class);
    private Properties config;
    private SparkSession sparkSession;
    private String scheduleId;
    private long scheduleTime;
    private String resourceGroupId;
    private String geogAreaCode;
    private Long submitId;
    private String status = TaskStatus.killed.name();

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        LOGGER = getTaskLogger();
        LOGGER.info("Task event: " + event.toJson());
        String path = System.getProperty("JOBNAVI_HOME");
        String type = event.getContext().getTaskInfo().getType().getName();
        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        scheduleTime = event.getContext().getTaskInfo().getScheduleTime();

        //load config
        String configurationDirectory = String.format(CONF_PATH_PATTERN, path, type);
        File configDirectory = new File(configurationDirectory);
        LOGGER.info("Using configuration directory " + configDirectory.getAbsolutePath());
        config = new Properties();
        try (FileInputStream configFile = new FileInputStream(configurationDirectory + "/" + CONF_FILE_NAME)) {
            config.load(configFile);
        }

        //create spark session
        LOGGER.info("application ID: " + sparkSession.sparkContext().applicationId());
        //register job submit instances
        long execId = event.getContext().getExecuteInfo().getId();
        Map<String, Object> extraInfo = JsonUtils.readMap(event.getContext().getTaskInfo().getExtraInfo());
        JobSubmitInstance sparkClusterInstance = createSparkSession(extraInfo);
        List<JobSubmitInstance> jobSubmitInstanceList = new ArrayList<>();
        getScheduleInstance().setInstId(Long.toString(execId));
        jobSubmitInstanceList.add(getScheduleInstance());
        jobSubmitInstanceList.add(sparkClusterInstance);
        submitId = ResourceUtil.registerInstances(scheduleId, resourceGroupId, geogAreaCode, jobSubmitInstanceList);

        //execute spark sql task
        Class<?> entryPointClass = Class.forName(config.getProperty(UC_ENTRYPOINT_CLASS_CONF_KEY));
        Object entryPointObject = entryPointClass.newInstance();
        Method entryPointMethod = entryPointClass
                .getDeclaredMethod(config.getProperty(UC_ENTRYPOINT_METHOD_CONF_KEY), String.class);
        try {
            entryPointMethod.invoke(entryPointObject, event.toJson());
            status = TaskStatus.finished.name();
        } catch (InvocationTargetException e) {
            Class<?> noDataExceptionClass = Class.forName(config.getProperty(UC_EXCEPTION_NO_DATA_CLASS_CONF_KEY));
            Throwable throwable = e.getTargetException();
            //convert uc NoDataException to JobNaviNoDataException
            if (noDataExceptionClass.isInstance(throwable)) {
                status = TaskStatus.failed_succeeded.name();
                throw new JobNaviNoDataException(throwable.getMessage());
            }
            status = TaskStatus.failed.name();
            throw throwable;
        } finally {
            onShutdown();
        }
    }

    private SparkConf buildSparkConf(String jobName, String queue, Map<String, Object> taskParams) throws Exception {
        SparkConf sparkConf = new SparkConf();
        //set spark deploy mode
        sparkConf.setMaster(DEPLOY_MASTER);
        sparkConf.set("spark.submit.deployMode", DEPLOY_MODE);
        //load spark-defaults.conf
        try {
            Config defaultConf = ConfigFactory.parseResources(this.getClass(), "/" + SPARK_DEFAULT_CONF_FILE_NAME);
            for (Map.Entry<String, ConfigValue> entry : defaultConf.entrySet()) {
                sparkConf.set(entry.getKey(), entry.getValue().unwrapped().toString());
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to load spark-defaults.conf", e);
        }
        String sparkSQLWarehouseDir = sparkConf.get("spark.bkdata.warehouse.dir");
        sparkConf.set("spark.sql.warehouse.dir",
                      String.format("%s/%s/%s", sparkSQLWarehouseDir, UUID.randomUUID(), jobName));
        sparkConf.set("spark.yarn.queue", queue);
        //load user define engine config
        if (taskParams.containsKey("engine_conf_path")) {
            final String sparkSessionConfUrl = config.getProperty(SPARK_SESSION_CONF_URL_CONF_KEY) + taskParams
                    .get("engine_conf_path");
            LOGGER.info("Getting user engine config from:" + sparkSessionConfUrl);
            String response = HttpUtils.get(sparkSessionConfUrl);
            Map<String, Object> confMap = JsonUtils.readMap(response);
            if (confMap.containsKey("data") && confMap.get("data") instanceof Map<?, ?>) {
                Map<?, ?> engineConf = (Map<?, ?>) confMap.get("data");
                for (Map.Entry<?, ?> entry : engineConf.entrySet()) {
                    if (entry.getKey() instanceof String) {
                        sparkConf.set((String) entry.getKey(), (String) entry.getValue());
                    }
                }
            }
        }
        //set schedule time in spark config
        sparkConf.set("spark.bkdata.scheduletime", Long.toString(scheduleTime));
        LOGGER.info("spark session configs:");
        for (Tuple2<String, String> conf : sparkConf.getAll()) {
            LOGGER.info(conf._1 + "=" + conf._2);
        }
        return sparkConf;
    }

    private JobSubmitInstance createSparkSession(Map<String, Object> taskParams) throws Exception {
        String queue = (String) taskParams.get("queue");
        resourceGroupId = (String) taskParams.get("resource_group_id");
        geogAreaCode = (String) taskParams.get("geog_area_code");
        if ((resourceGroupId == null || geogAreaCode == null) && taskParams.containsKey("cluster_group_id")) {
            String clusterGroupId = (String) taskParams.get("cluster_group_id");
            //get available yarn queue from resource center
            ResourceGroupInfo resourceGroupInfo = ResourceUtil.retrieveResourceGroupInfo(clusterGroupId);
            if (resourceGroupInfo != null) {
                resourceGroupId = resourceGroupInfo.getResourceGroupId();
                geogAreaCode = resourceGroupInfo.getGeogAreaCode();
            }
        }
        JobSubmitInstance sparkClusterInstance;
        if (resourceGroupId != null && geogAreaCode != null) {
            LOGGER.info(String.format("Resource group ID:%s, geog area code:%s", resourceGroupId, geogAreaCode));
            //retrieve corresponding cluster info if exist
            sparkClusterInstance = getSparkClusterInstance(resourceGroupId, geogAreaCode);
            if (sparkClusterInstance == null || !sparkClusterInstance.getClusterName().equals(queue)) {
                sparkClusterInstance = getCustomSparkClusterInstance(queue);
            }
        } else {
            sparkClusterInstance = getCustomSparkClusterInstance(queue);
        }
        //create spark session and get yarn application ID
        String jobName = scheduleId;
        SparkConf sparkConf = buildSparkConf(jobName, queue, taskParams);
        sparkSession = SparkSession
                .builder()
                .appName(jobName)
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
        String applicationId = sparkSession.sparkContext().applicationId();
        sparkClusterInstance.setInstId(applicationId);
        return sparkClusterInstance;
    }

    private JobSubmitInstance getSparkClusterInstance(String resourceGroupId, String geogAreaCode) {
        CLusterInfo cLusterInfo = ResourceUtil.listClusterInfo(resourceGroupId, geogAreaCode,
                                                               SPARK_SQL_RESOURCE_TYPE, SERVICE_TYPE, COMPONENT_TYPE,
                                                               config.getProperty(SPARK_CLUSTER_TYPE_CONF_KEY));
        if (cLusterInfo != null && cLusterInfo.getClusterName() != null) {
            JobSubmitInstance sparkClusterInstance;
            sparkClusterInstance = new JobSubmitInstance();
            sparkClusterInstance.setClusterId(cLusterInfo.getClusterId());
            sparkClusterInstance.setClusterName(cLusterInfo.getClusterName());
            sparkClusterInstance.setResourceType(SPARK_SQL_RESOURCE_TYPE);
            sparkClusterInstance.setClusterType(cLusterInfo.getClusterType());
            return sparkClusterInstance;
        }
        return null;
    }

    private JobSubmitInstance getCustomSparkClusterInstance(String queue) {
        //user defined queue
        JobSubmitInstance sparkClusterInstance = new JobSubmitInstance();
        final String RESOURCE_GROUP_ID_CUSTOM = "default.custom";
        final String GEOG_AREA_CODE_DEFAULT = "default";
        final String SPARK_CLUSTER_ID_CUSTOM = "spark.yarn.cluster.custom";
        final String RESOURCE_TYPE = "processing";
        final String CLUSTER_TYPE = config.getProperty(SPARK_CLUSTER_TYPE_CONF_KEY);
        resourceGroupId = RESOURCE_GROUP_ID_CUSTOM;
        geogAreaCode = GEOG_AREA_CODE_DEFAULT;
        sparkClusterInstance.setClusterId(SPARK_CLUSTER_ID_CUSTOM);
        sparkClusterInstance.setClusterName(queue);
        sparkClusterInstance.setResourceType(RESOURCE_TYPE);
        sparkClusterInstance.setClusterType(CLUSTER_TYPE);
        return sparkClusterInstance;
    }

    @Override
    public EventListener getEventListener(String eventName) {
        return null;
    }

    @Override
    protected void onShutdown() {
        if (submitId != null && ResourceUtil.updateJobSubmitStatus(submitId, status) == null) {
            LOGGER.warn("Failed to update status of job submit:" + submitId);
        }
        super.onShutdown();
    }
}
