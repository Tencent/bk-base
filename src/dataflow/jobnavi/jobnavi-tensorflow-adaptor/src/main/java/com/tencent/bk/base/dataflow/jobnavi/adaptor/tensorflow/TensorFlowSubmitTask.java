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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.tensorflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.util.ClientBuilder;
import net.minidev.json.JSONObject;
import org.apache.log4j.Logger;

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

public class TensorFlowSubmitTask extends AbstractJobNaviTask {

    private static Logger LOGGER = Logger.getLogger(TensorFlowSubmitTask.class);
    private static String CRD_GROUP = "kubeflow.org";
    private static String CRD_VERSION = "v1";
    private static String CRD_PLURAL = "tfjobs";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String STRATEGY_MULTI_WORKER = "mwm";

    private String namespace;
    private String scheduleId;
    private String jobName;
    private ApiClient apiClient;
    private String imageUrl;
    private List<String> commandList;
    private String strategy;
    private Map<String, Object> resources;

    private void init(TaskEvent event) throws Exception {
        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        jobName = String.format("tensorflow-%s", this.scheduleId.replace("_", "-"));
        Map<String, Object> extraInfo = JsonUtils.readMap(event.getContext().getTaskInfo().getExtraInfo());
        Map<String, Object> properties = (Map<String, Object>)extraInfo.get("properties");
        namespace = DEFAULT_NAMESPACE;
        if (properties.containsKey("namespace")) {
            namespace = (String) properties.get("namespace");
        } else {
            namespace = DEFAULT_NAMESPACE;
        }
        if (properties.containsKey("strategy")) {
            strategy = properties.get("strategy").toString();
        } else {
            //  默认为MultiWorkerMirroredStrategy
            strategy = STRATEGY_MULTI_WORKER;
        }
        resources = new HashMap<>();
        if (properties.containsKey("resources")) {
            resources = (Map<String, Object>)properties.get("resources");
        }

        imageUrl = properties.get("image.url").toString();
        commandList = (List<String>)properties.get("command.list");
        commandList.add(String.valueOf(event.getContext().getTaskInfo().getScheduleTime()));
        String clusterBaseUrl = String.valueOf(properties.get("cluster.url"));
        apiClient = ClientBuilder.defaultClient().setBasePath(clusterBaseUrl).setVerifyingSsl(false);
        apiClient.setApiKeyPrefix("Bearer");
        String apiToken = String.valueOf(properties.get("api.token"));
        apiClient.setApiKey(apiToken);
    }

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        LOGGER.info("Task event: " + event.toJson());
        this.init(event);
        Map<String, Object> yamlData = this.getYamlData();
        String yamlString = JSONObject.toJSONString(yamlData);
        LOGGER.info("yaml data:" + yamlString);
        CustomObjectsApi api = new CustomObjectsApi(apiClient);
        try {
            Map<String, Object> createResponse = (Map<String, Object>)api.createNamespacedCustomObject(
                    CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL, yamlData, null);
            LOGGER.info(String.format("Created tensorflow job:%s", JSONObject.toJSONString(createResponse)));
        } catch (Exception e) {
            LOGGER.error("Start tensorflow job error", e);
            throw new NaviException(String.format("Start tensroflow job error:%s", e.getMessage()));
        }
        JSONObject statusObject = this.status();
        boolean checkResult = Boolean.valueOf(statusObject.get("result").toString());
        JSONObject statusInfo = ((JSONObject)statusObject.get("data"));
        while (checkResult && String.valueOf(statusInfo.get("status")).equalsIgnoreCase("running")) {
            // 请求失败或是仍旧在运行
            Thread.sleep(5 * 1000);
            statusObject = this.status();
            checkResult = Boolean.valueOf(statusObject.get("result").toString());
            statusInfo = ((JSONObject)statusObject.get("data"));
        }
        if (!checkResult) {
            throw new Exception("Failed to get tensorflow job status:" + statusInfo.get("info"));
        } else {
            this.stop();
            if (!String.valueOf(statusInfo.get("status")).equalsIgnoreCase("success")) {
                throw new NaviException(String.format("Tensorflow job failed:%s", statusInfo.get("info").toString()));
            }
        }

    }

    Map<String, Object> getReplicaObject(int workerNumber, double cpu, String memory) {
        Map<String, Object> workerObject = new HashMap<>();
        workerObject.put("replicas", workerNumber);
        workerObject.put("restartPolicy", "Never");
        Map<String, Object> templateObject = new HashMap<>();
        workerObject.put("template", templateObject);
        Map<String, Object> templateSpecObject = new HashMap<>();
        templateObject.put("spec", templateSpecObject);
        List<Map<String, Object>> containerList = new ArrayList<>();
        Map<String, Object> containerObject = new HashMap<>();
        containerList.add(containerObject);
        templateSpecObject.put("containers", containerList);
        containerObject.put("name", "tensorflow");
        containerObject.put("image", imageUrl);
        containerObject.put("command", this.commandList);
        List<Map<String, Object>> portsList = new ArrayList<>();
        Map<String, Object> portObject = new HashMap<>();
        portsList.add(portObject);
        containerObject.put("ports", portsList);
        portObject.put("name", "tfjob-port");
        portObject.put("containerPort", 2222);
        containerObject.put("imagePullPolicy", "Always");
        Map<String, Object> resourceObject = new HashMap<>();
        containerObject.put("resources", resourceObject);
        Map<String, Object> resourceLimitObject = new HashMap<>();
        resourceObject.put("limits", resourceLimitObject);
        resourceLimitObject.put("cpu", cpu);
        resourceLimitObject.put("memory", memory);
        return workerObject;
    }

    Map<String, Object> getYamlData() {
        int workerNumber = Integer.valueOf(resources.getOrDefault("worker", "2").toString());
        double cpu = Double.valueOf(resources.getOrDefault("cpu", "1.0").toString());
        String memory = resources.getOrDefault("memory", "1.0Gi").toString();
        Map<String, Object> yamlData = new LinkedHashMap<String, Object>();
        yamlData.put("apiVersion", "kubeflow.org/v1");
        yamlData.put("kind", "TFJob");

        Map<String, String> metaObject = new HashMap<>();
        metaObject.put("name", jobName);
        metaObject.put("namespace", this.namespace);
        yamlData.put("metadata", metaObject);
        Map<String, Object> specObject = new HashMap<>();
        yamlData.put("spec", specObject);
        Map<String, Object> tfReplicaSpecs = new HashMap<>();
        specObject.put("tfReplicaSpecs", tfReplicaSpecs);
        if (STRATEGY_MULTI_WORKER.equalsIgnoreCase(this.strategy)) {
            tfReplicaSpecs.put("Worker", this.getReplicaObject(workerNumber, cpu, memory));
        } else {
            // ParameterServerStrategy下，目前设置一个ps,一个chief，用户仅定义worker数目
            tfReplicaSpecs.put("Worker", this.getReplicaObject(workerNumber, cpu, memory));
            tfReplicaSpecs.put("Ps", this.getReplicaObject(1, cpu, memory));
            tfReplicaSpecs.put("Chief", this.getReplicaObject(1, cpu, memory));
        }
        return yamlData;
    }

    @Override
    public EventListener getEventListener(String eventName) {
        switch (eventName) {
            case "status":
                return new StatusEventListener(this);
            case "stop":
                return new StopEventListener(this);
            default:
                return null;
        }
    }

    /**
     * 停止正在执行的任务
     * */
    public JSONObject stop() {
        CustomObjectsApi api = new CustomObjectsApi(apiClient);
        V1DeleteOptions deleteOptions = new V1DeleteOptions();
        //deleteOptions.setApiVersion("kubeflow.org/v1");
        //deleteOptions.setKind("TFJob");
        JSONObject deleteResultObject = new JSONObject();
        try {
            api.deleteNamespacedCustomObject(CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL,
                    this.jobName, deleteOptions, 0, null,
                    "Background");
            deleteResultObject.put("result", true);
            deleteResultObject.put("data", "delete job successfully");
        } catch (ApiException e) {
            LOGGER.error("Exception when stop yaml resource");
            LOGGER.error("Status code: " + e.getCode());
            LOGGER.error("Reason: " + e.getResponseBody());
            LOGGER.error("Response headers: " + e.getResponseHeaders());
            deleteResultObject.put("result", false);
            deleteResultObject.put("data", e.getResponseBody());
        } catch (Exception e) {
            LOGGER.error(e);
            deleteResultObject.put("result", false);
            deleteResultObject.put("data", e.getMessage());
        }
        return deleteResultObject;
    }

    /**
     * 获取正在执行任务状态
     * */
    public JSONObject status() {
        CustomObjectsApi api = new CustomObjectsApi(apiClient);
        JSONObject statusResultObject = new JSONObject();
        JSONObject statusInfo = new JSONObject();
        try {
            Map<String, Object> getResponse = (Map<String, Object>)api.getNamespacedCustomObjectStatus(
                    CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL, this.jobName);
            LOGGER.info("status result:" + JSONObject.toJSONString(getResponse));
            statusResultObject.put("result", true);
            if (!getResponse.containsKey("status")) {
                // 还没有状态信息，即还未开始执行
                statusInfo.put("status", "running");
            } else {
                Map<String, Object> statusObject = (Map<String, Object>)getResponse.get("status");
                if (!statusObject.containsKey("startTime")) {
                    statusInfo.put("status", "running");
                } else {
                    // 检查各个replica执行的状态，active(running), completions, succeeded
                    Map<String, Object> replicaStatusInfo = (Map<String, Object>)statusObject.get("replicaStatuses");
                    Map<String, Object> workerInfo = (Map<String, Object>) replicaStatusInfo.get("Worker");
                    int active = workerInfo.containsKey("active")
                            ? (int)Double.parseDouble(workerInfo.get("active").toString()) : 0;
                    int failed = workerInfo.containsKey("failed")
                            ? (int)Double.parseDouble(workerInfo.get("failed").toString()) : 0;
                    if (failed > 0) {
                        // 已经有失败的，则直接返回失败
                        statusInfo.put("status", "failed");
                        statusInfo.put("info", "Pod already failed");
                    } else if (active > 0) {
                        // 仍旧有在运行的
                        statusInfo.put("status", "running");
                    } else {
                        // 都完成了，且没有失败的
                        statusInfo.put("status", "success");
                    }

                }
            }
            statusResultObject.put("data", statusInfo);
        } catch (ApiException e) {
            LOGGER.error("Exception when get job status");
            LOGGER.error("Status code: " + e.getCode());
            LOGGER.error("Reason: " + e.getResponseBody());
            LOGGER.error("Response headers: " + e.getResponseHeaders());
            statusResultObject.put("result", false);
            statusInfo.put("info", e.getResponseBody());
            statusResultObject.put("data", statusInfo);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            statusResultObject.put("result", false);
            statusInfo.put("info", e.getMessage());
            statusResultObject.put("data", statusInfo);
        }
        LOGGER.info("status result object:" + statusResultObject.toJSONString());
        return statusResultObject;
    }

    static class StopEventListener implements EventListener {

        private final TensorFlowSubmitTask task;

        StopEventListener(TensorFlowSubmitTask task) {
            this.task = task;
        }

        @Override
        public TaskEventResult doEvent(TaskEvent event) {
            JSONObject stopResult = task.stop();
            TaskEventResult result = new TaskEventResult();
            result.setSuccess((boolean) stopResult.get("result"));
            result.setProcessInfo(String.valueOf(stopResult.get("data")));
            return result;
        }
    }

    static class StatusEventListener implements EventListener {

        private final TensorFlowSubmitTask task;

        StatusEventListener(TensorFlowSubmitTask task) {
            this.task = task;
        }

        @Override
        public TaskEventResult doEvent(TaskEvent event) {
            JSONObject statusResult = task.status();
            TaskEventResult result = new TaskEventResult();
            result.setSuccess((boolean) statusResult.get("result"));
            result.setProcessInfo(String.valueOf(statusResult.get("data")));
            return result;
        }
    }
}
