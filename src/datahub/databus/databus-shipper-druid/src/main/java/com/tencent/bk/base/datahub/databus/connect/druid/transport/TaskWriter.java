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

package com.tencent.bk.base.datahub.databus.connect.druid.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.tencent.bk.base.datahub.databus.connect.druid.DruidSinkConfig;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.config.TaskConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskWriter {

    private static final Logger log = LoggerFactory.getLogger(TaskWriter.class);
    private static final String RUNNING_STATUS = "RUNNING";
    private static final String DRUID_TASK_SETUP_URL_PATH = "/druid/indexer/v1/task";
    private static final String DRUID_VERSION_V2_TASK_TEMPLATE =
            "/com/tencent/bk/base/datahub/databus/connect/druid/transport/config/task-config-v2.json.vm";
    private static final String DRUID_VERSION_V1_TASK_TEMPLATE =
            "/com/tencent/bk/base/datahub/databus/connect/druid/transport/config/task-config-v1.json.vm";
    public final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RequestConfig httpConfig;
    private final CuratorFramework curator;
    private final TaskConfig taskConfig;
    private HttpClient httpClient;
    // druid数据入口相关
    private String taskId;
    private String location;
    private String serviceName;
    private String pushEventRequest;
    private String shutdownTaskRequest;
    private String overlordZkPath;
    //写数据相关, 一级缓存listCache
    private ArrayList<Map<String, Object>> listCache;

    /**
     * TaskWriter类对象构造函数
     *
     * @param taskConfig druid index task配置
     * @throws ConnectException 异常
     */
    public TaskWriter(TaskConfig taskConfig) throws ConnectException {
        LogUtils.info(log, "{}: creating TaskWrite: {} ", taskConfig.getDataSourceName(), taskConfig);
        this.taskConfig = taskConfig;
        int httpTimeout = taskConfig.getShipperHttpRequestTimeout();
        httpConfig = RequestConfig.custom()
                .setConnectTimeout(httpTimeout)
                .setConnectionRequestTimeout(httpTimeout)
                .setSocketTimeout(httpTimeout).build();
        curator = CuratorFrameworkFactory.newClient(taskConfig.getzookeeperConnect(),
                new ExponentialBackoffRetry(1000, 3));
        curator.start();
        try {
            curator.blockUntilConnected(30, TimeUnit.SECONDS);
            createEndpoint();
            LogUtils.info(log, "{}: created endpoint, taskID is: {}, {}, {}", taskConfig.getDataSourceName(),
                    taskId, location, serviceName);

            listCache = new ArrayList<>(taskConfig.getShipperCacheSize());
            stopwatch.start();
        } catch (InterruptedException | IllegalStateException | NumberFormatException e) {
            LogUtils.info(log, "{}: failed to create taskWriter", taskConfig.getDataSourceName());
            throw new ConnectException(e);
        }

    }

    /**
     * 创建druid index task
     *
     * @param overlordUrl overlor master 地址
     * @return index task的taskId
     */
    private String createSingleTask(URL overlordUrl) {
        String remoteTaskConfigText;

        if (DruidSinkConfig.DRUID_VERSION_V1.equals(taskConfig.getDruidVersion())) {
            remoteTaskConfigText = createRemoteTaskConfigText(1);
        } else {
            remoteTaskConfigText = createRemoteTaskConfigTextV2();
        }
        LogUtils.info(log, "{}: created index task config for 0.16: {}",
                taskConfig.getDataSourceName(), remoteTaskConfigText);

        try {
            HttpPost post = new HttpPost(URI.create(overlordUrl.toString() + DRUID_TASK_SETUP_URL_PATH));
            post.setConfig(httpConfig);
            post.setEntity(new StringEntity(remoteTaskConfigText, ContentType.APPLICATION_JSON));
            HttpResponse response = httpClient.execute(post);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = String.format("%s: get endpoint failed for task %s during creation druid index task",
                        taskConfig.getDataSourceName(), taskId);
                ConnectException e = new ConnectException(msg);
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
                throw e;
            }

            //踩坑：千萬不要對response執行兩次response.getEntity()，會報錯IOException
            JSONObject jsonObject = JSONObject.fromObject(EntityUtils.toString(response.getEntity()));
            String taskId = jsonObject.getString("task");
            LogUtils.info(log, "{} taskId : {} ", taskConfig.getDataSourceName(), taskId);
            Preconditions.checkNotNull(taskId, "taskID");
            return taskId;
        } catch (IOException | NullPointerException e) {
            String msg = String.format("%s: create task failed, overlord %s, config %s",
                    taskConfig.getDataSourceName(), overlordUrl, remoteTaskConfigText);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException(msg, e);
        }

    }

    /**
     * 查询overlord leader的url地址
     *
     * @return 一个指向overlord leader的URL对象
     */
    private URL getOverlordLeader() {
        try {
            List<String> children = curator.getChildren().forPath(overlordZkPath);
            String overlordMasterRequest = "http://" + children.get(0) + "/druid/indexer/v1/leader";
            HttpGet get = new HttpGet(URI.create(overlordMasterRequest));
            get.setConfig(httpConfig);
            HttpResponse response = httpClient.execute(get);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = String.format("%s: get overlord leader for task %s failed.",
                        taskConfig.getDataSourceName(), taskId);
                ConnectException e = new ConnectException(msg);
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
                throw e;
            }
            String leader = EntityUtils.toString(response.getEntity());
            return new URL(leader);
        } catch (Exception e) {
            String msg = String.format("[%s] failed to get overlord master", taskConfig.getDataSourceName());
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException("failed to get overlord master ", e);
        }

    }

    /**
     * 为低版本（主要指0.11）druid创建一个task config的json串，用于向druid发起一个index task
     *
     * @param partitionNum 分区数量
     * @return druid task config的json串
     */
    private String createRemoteTaskConfigText(int partitionNum) {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();
        VelocityContext context = new VelocityContext();
        context.put("objectMapper", objectMapper);
        context.put("availabilityGroup", "availability-group-" + UUID.randomUUID().toString().substring(0, 8));
        context.put("dataSourceName", taskConfig.getDataSourceName());
        context.put("metrics", taskConfig.getMetrics());
        context.put("segmentGranularity", taskConfig.getSegmentGranularity());
        context.put("timestampColumn", taskConfig.getTimestampColumn());
        context.put("dimensions", taskConfig.getDimensions());
        context.put("dimensionExclusions", taskConfig.getDimensionExclusions());
        context.put("serviceName", serviceName);
        context.put("windowPeriod", taskConfig.getWindowPeriod());
        context.put("partitionNum", partitionNum);
        context.put("context", taskConfig.getContext());
        context.put("requiredCapacity", taskConfig.getRequiredCapacity());
        context.put("maxIdleTime", taskConfig.getMaxIdleTime());
        StringWriter writer = new StringWriter();
        ve.getTemplate(DRUID_VERSION_V1_TASK_TEMPLATE).merge(context, writer);
        return writer.toString();
    }

    /**
     * 为高版本druid创建一个task config的json串，用于向druid发起一个index task
     *
     * @return druid task config的json串
     */
    private String createRemoteTaskConfigTextV2() {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();
        VelocityContext context = new VelocityContext();
        context.put("objectMapper", objectMapper);
        context.put("dataSourceName", taskConfig.getDataSourceName());
        context.put("metrics", taskConfig.getMetrics());
        context.put("segmentGranularity", "HOUR");
        context.put("timestampColumn", taskConfig.getTimestampColumn());
        context.put("dimensions", taskConfig.getDimensions());
        context.put("dimensionExclusions", taskConfig.getDimensionExclusions());
        context.put("serviceName", serviceName);
        context.put("bufferSize", taskConfig.getBufferSize());
        context.put("maxIdleTime", taskConfig.getMaxIdleTime());
        context.put("intermediateHandoffPeriod", taskConfig.getIntermediateHandoffPeriod());
        context.put("maxRowsPerSegment", taskConfig.getMaxRowsPerSegment());
        context.put("maxRowsInMemory", taskConfig.getMaxRowsInMemory());
        context.put("maxTotalRows", taskConfig.getMaxTotalRows());
        context.put("intermediatePersistPeriod", taskConfig.getIntermediatePersistPeriod());

        StringWriter writer = new StringWriter();
        ve.getTemplate(DRUID_VERSION_V2_TASK_TEMPLATE)
                .merge(context, writer);
        return writer.toString();
    }

    /**
     * 在druid中创建一个index task，获取index task端口信息
     *
     * @throws ConnectException 必须跑出的异常异常
     */
    private void createEndpoint() throws ConnectException {
        try {
            httpClient = HttpClients.createDefault();
            // 初始化
            if (DruidSinkConfig.DRUID_VERSION_V1.equals(taskConfig.getDruidVersion())) {
                overlordZkPath = "/druid/internal-discovery/overlord";
            } else {
                overlordZkPath = "/druid/internal-discovery/OVERLORD";
            }
            serviceName = taskConfig.getDataSourceName() + "_" + UUID.randomUUID().toString().substring(0, 8);

            URL leader = getOverlordLeader();
            LogUtils.info(log, "{}: get overlord leader: {}", taskConfig.getDataSourceName(), leader.toString());
            taskId = createSingleTask(leader);

            Stopwatch waitTaskRun = Stopwatch.createStarted();
            while (true) {
                Thread.sleep(1_000);
                leader = getOverlordLeader();
                if (RUNNING_STATUS.equals(getTaskStatus(leader))) {
                    //踩坑：虽然task状态为running，但并不是立即可写，加了延时后才成功写入
                    Thread.sleep(10_000);
                    break;
                }

                if (waitTaskRun.elapsed(TimeUnit.MILLISECONDS) > taskConfig.getShipperHttpRequestTimeout()) {
                    shutdownDruidIndexTaskForce(leader, taskId);
                    throw new ConnectException("create druid index task failed, stop it");
                }
            }

            if (DruidSinkConfig.DRUID_VERSION_V1.equals(taskConfig.getDruidVersion())) {
                createLocationForVersionV1(leader);
            } else {
                createLocationForVersionV2(leader);
            }
            pushEventRequest = location + "/druid/worker/v1/chat/" + serviceName + "/push-events";
            shutdownTaskRequest = location + "/druid/worker/v1/chat/" + serviceName + "/shutdown";
            LogUtils.info(log, "{}: created endpoint: {}, {}, {}, {}", taskConfig.getDataSourceName(),
                    taskId, location, pushEventRequest, shutdownTaskRequest);
        } catch (InterruptedException | ConnectException | IOException | NullPointerException e) {
            String msg = String
                    .format("%s create task %s failed, will stop it.", taskConfig.getDataSourceName(), taskId);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException(msg);
        }

    }


    /**
     * 用于创建druid index task 后，使用task ID获取index task的HTTP url
     *
     * @param overlordLeader Overlord角色的leader
     * @throws ConnectException IOException 异常
     */
    private void createLocationForVersionV1(URL overlordLeader) throws ConnectException, IOException {
        String runningTasksRequest = overlordLeader.toString() + "/druid/indexer/v1/runningTasks";
        HttpGet get = new HttpGet(runningTasksRequest);
        get.setConfig(httpConfig);
        HttpResponse runningTasksResponse = httpClient.execute(get);
        if (runningTasksResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            String msg = String
                    .format("[%s]: failed to get endpoint for task [%s]", taskConfig.getDataSourceName(), taskId);
            ConnectException e = new ConnectException(msg);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw e;
        }

        JSONArray jsonArray = JSONArray.fromObject(EntityUtils.toString(runningTasksResponse.getEntity()));
        Map<String, String> runningMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject json = jsonArray.getJSONObject(i);
            String location = "http://" + json.getJSONObject("location").getString("host") + ":"
                    + json.getJSONObject("location").getInt("port");
            runningMap.put(json.getString("id"), location);
        }

        if (runningMap.containsKey(taskId)) {
            location = runningMap.get(taskId);
            LogUtils.info(log, "{}: get task endpoint: {}", taskConfig.getDataSourceName(), location);
        } else {
            String msg = String
                    .format("[%s] failed to get endpoint for task [%s]", taskConfig.getDataSourceName(), taskId);
            ConnectException e = new ConnectException(msg);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw e;
        }
    }

    /**
     * 用于创建druid index task 后，使用task ID获取index task的HTTP url
     *
     * @param overlordLeader Overlord角色的leader
     * @throws ConnectException IOException 异常
     */
    private void createLocationForVersionV2(URL overlordLeader) throws ConnectException, IOException {
        String statusRequest = overlordLeader.toString() + "/druid/indexer/v1/task/" + taskId + "/status";
        HttpGet get = new HttpGet(statusRequest);
        get.setConfig(httpConfig);
        HttpResponse statusResponse = httpClient.execute(new HttpGet(statusRequest));
        if (statusResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            String msg = String
                    .format("[%s] failed to get endpoint for task [%s]", taskConfig.getDataSourceName(), taskId);
            ConnectException e = new ConnectException(msg);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw e;
        }

        JSONObject jsonObject = JSONObject.fromObject(EntityUtils.toString(statusResponse.getEntity()));
        if (!jsonObject.containsKey("status") || !jsonObject.getJSONObject("status").containsKey("status")) {
            String msg = String
                    .format("[%s]: failed to find eventCount in http response ", taskConfig.getDataSourceName());
            IOException e = new IOException(msg);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw e;
        }

        String status = jsonObject.getJSONObject("status").getString("status");
        LogUtils.info(log, "{}:  get task status: {}", status);
        if (RUNNING_STATUS.equals(status)) {
            String host = jsonObject.getJSONObject("status").getJSONObject("location").getString("host");
            String port = jsonObject.getJSONObject("status").getJSONObject("location").getString("port");
            LogUtils.info(log, "{}:  get task status: {}, {}, {}", status, host, port);
            if (host.isEmpty() || "-1".equals(port)) {
                String msg = String
                        .format("[%s] failed to get endpoint for task [%s]", taskConfig.getDataSourceName(), taskId);
                ConnectException e = new ConnectException(msg);
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
                throw e;
            }
            location = "http://" + host + ":" + port;
            LogUtils.info(log, "{}:  get task endpoint: {}", taskConfig.getDataSourceName(), location);
        } else {
            String msg = String
                    .format("[%s] failed to get endpoint for task [%s]", taskConfig.getDataSourceName(), taskId);
            ConnectException e = new ConnectException(msg);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw e;
        }
    }

    /**
     * 用于创建druid index task 后，使用task ID获取index task的运行状态
     *
     * @param overlordLeader Overlord角色的leader
     * @throws ConnectException 异常
     */
    private String getTaskStatus(URL overlordLeader) throws ConnectException {
        try {
            if (taskId == null || taskId.isEmpty()) {
                return "";
            }
            String statusRequest = overlordLeader.toString() + "/druid/indexer/v1/task/" + taskId + "/status";
            HttpGet get = new HttpGet(statusRequest);
            get.setConfig(httpConfig);
            HttpResponse statusResponse = httpClient.execute(get);
            if (statusResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = String
                        .format("[%s] failed to get status for task: [%s]", taskConfig.getDataSourceName(), taskId);
                IOException e = new IOException(msg);
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
                throw e;
            }

            JSONObject jsonObject = JSONObject.fromObject(EntityUtils.toString(statusResponse.getEntity()));
            String status;
            if (DruidSinkConfig.DRUID_VERSION_V1.equals(taskConfig.getDruidVersion())) {
                status = jsonObject.getJSONObject("status").getString("status");
            } else {
                status = jsonObject.getJSONObject("status").getString("runnerStatusCode");
            }
            LogUtils.info(log, "{}:  get task status: {}", taskConfig.getDataSourceName(), status);
            return status;
        } catch (IOException e) {
            String msg = String.format("[%s]:  get task status failed", taskConfig.getDataSourceName());
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException(msg, e);
        }

    }

    /**
     * 写数据到一级缓存listCache
     *
     * @param event 单条待写入记录
     * @throws ConnectException 写cache或者flush异常
     */
    public void write(Map<String, Object> event) throws ConnectException { // sync method
        try {
            listCache.add(event);
            if (listCache.size() >= taskConfig.getShipperFlushSizeThreshold()
                    || stopwatch.elapsed(TimeUnit.MILLISECONDS) > taskConfig.getShipperFlushOvertimeThreshold()) {
                flush();
                stopwatch.reset();
                stopwatch.start();
            }
        } catch (ConnectException e) {
            String msg = String.format("[%s]:  write cache failed", taskConfig.getDataSourceName());
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException(msg, e);
        }
    }


    /**
     * 写一批数据到Druid
     *
     * @throws ConnectException writeDruid失败异常
     */
    private void writeDruid() throws ConnectException {
        try {
            String event = objectMapper.writeValueAsString(listCache);
            HttpPost httpPost = new HttpPost(pushEventRequest);
            httpPost.setConfig(httpConfig);
            httpPost.setEntity(new StringEntity(event, ContentType.APPLICATION_JSON));
            HttpResponse response = httpClient.execute(httpPost);
            JSONObject jsonObject = JSONObject.fromObject(EntityUtils.toString(response.getEntity()));
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = String.format(
                        "%s: bad http response. StatusCode %s, Data %s", taskConfig.getDataSourceName(),
                        response.getStatusLine().getStatusCode(), jsonObject.toString());
                IOException e = new IOException(msg);
                throw e;
            }

            if (!jsonObject.containsKey("eventCount")) {
                String msg = String
                        .format("%s: failed to find eventCount in http response.", taskConfig.getDataSourceName());
                IOException e = new IOException(msg);
                throw e;
            }

            int count = jsonObject.getInt("eventCount");
            if (count != listCache.size()) {
                LogUtils.info(log, "{}:  write druid failed", taskConfig.getDataSourceName());
                throw new IOException(
                        String.format("warning: sent batch size %d, succeed size %d", listCache.size(), count));
            }
            listCache.clear();
        } catch (UnsupportedCharsetException | IOException | ParseException e) {
            String msg = String
                    .format("%s: write to druid index task %s failed", taskConfig.getDataSourceName(), taskId);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            throw new ConnectException(msg, e);
        }
    }

    /**
     * 尽力将数据写入druid, 如果异常，则捕获并尽力关闭druid index task
     *
     * @throws ConnectException write失败异常
     */
    void bestEffortWrite() throws ConnectException { // sync method
        URL overlordLeader = getOverlordLeader();
        if (RUNNING_STATUS.equals(getTaskStatus(overlordLeader))) {
            try {
                writeDruid();
                return;
            } catch (ConnectException e1) {
                shutdownDruidIndexTask(shutdownTaskRequest);
            }
        } else {
            shutdownDruidIndexTaskForce(overlordLeader, taskId);
        }
        LogUtils.info(log,
                "{}: druid index task is not running, second write druid failed:{} , now create new endpoint",
                taskConfig.getDataSourceName(), taskId);
        createEndpoint();
        writeDruid();
    }

    /**
     * 从二级缓存取一批数据写入druid
     *
     * @throws ConnectException flush失败异常
     */
    public void flush() throws ConnectException { // sync method
        if (listCache.isEmpty()) {
            return;
        }

        try {
            writeDruid();
        } catch (ConnectException e) {
            try {
                bestEffortWrite();
            } catch (ConnectException e2) {
                URL overlordLeader = getOverlordLeader();
                if (RUNNING_STATUS.equals(getTaskStatus(overlordLeader))) {
                    shutdownDruidIndexTask(shutdownTaskRequest);
                } else {
                    shutdownDruidIndexTaskForce(overlordLeader, taskId);
                }
                String msg = String.format("%s: last time, druid index task %s is idling, will stop it",
                        taskConfig.getDataSourceName(), taskId);
                LogUtils.info(log, msg);
                throw new ConnectException(msg, e2);
            }
        }
    }


    /**
     * 尽力flush 剩余的缓存数据，关闭druid index task，zookeeper连接
     */
    public void close() {
        try {
            flush();
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF,
                    "flush data error when closing taskWriter for " + taskId, e);
        } finally {
            shutdownDruidIndexTask(shutdownTaskRequest);
            if (curator != null) {
                curator.close();
            }
        }
        LogUtils.info(log, "{}:  remote task " + taskId + "has been closed", taskConfig.getDataSourceName());
    }


    /**
     * 尽力关闭druid index task, 不等待结果返回，优雅关闭
     */
    public void shutdownDruidIndexTask(String shutdownRequest) {
        if (shutdownRequest == null || shutdownRequest.isEmpty()) {
            return;
        }
        try {
            HttpPost post = new HttpPost(shutdownRequest);
            RequestConfig config = RequestConfig.custom()
                    .setConnectTimeout(1000)
                    .setConnectionRequestTimeout(1000)
                    .setSocketTimeout(1000).build();
            post.setConfig(config);
            httpClient.execute(post);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF,
                    "close druid realtime task error for " + taskId, e);
        }
    }


    /**
     * 尽力关闭druid index task, 不等待结果返回, 强制关闭
     */
    public void shutdownDruidIndexTaskForce(URL overlordLeader, String taskId) {
        if (taskId == null || taskId.isEmpty() || overlordLeader == null) {
            return;
        }
        try {
            HttpPost post = new HttpPost(overlordLeader.toString() + "/druid/indexer/v1/task/" + taskId + "/shutdown");
            RequestConfig config = RequestConfig.custom()
                    .setConnectTimeout(1000)
                    .setConnectionRequestTimeout(1000)
                    .setSocketTimeout(1000).build();
            post.setConfig(config);
            httpClient.execute(post);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF,
                    "close druid realtime task error for " + taskId, e);
        }
    }

}