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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.kubernetes;

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.AppsV1beta1Api;
import io.kubernetes.client.apis.AppsV1beta2Api;
import io.kubernetes.client.apis.BatchV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.AppsV1beta1Deployment;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1Job;
import io.kubernetes.client.models.V1JobStatus;
import io.kubernetes.client.models.V1beta2Deployment;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

public class KubernetesTask extends AbstractJobNaviTask {

    //k8s object class -> k8s object kind
    private static final Map<Class<?>, String> kindMap = new HashMap<>();
    //k8s object class -> k8s api class
    private static final Map<Class<?>, Class<?>> apiMap = new HashMap<>();
    //k8s object class -> k8s object create method
    private static final Map<Class<?>, Method> createMethodMap = new HashMap<>();
    //k8s object class -> k8s object delete method
    private static final Map<Class<?>, Method> deleteMethodMap = new HashMap<>();
    private static final String CONF_PATH_PATTERN = "%s/adaptor/%s/conf";
    private static final String CONF_FILE_NAME = "config.properties";
    private static final String YAML_FILE_PATH_PATTERN = "%s/adaptor/%s/yaml";
    private static final String DEFAULT_NAMESPACE = "default";
    private static Logger LOGGER = Logger.getLogger(KubernetesTask.class);

    static {
        //for k8s java client 6.0.1, k8s version 1.14
        kindMap.put(V1Deployment.class, "Deployment");
        kindMap.put(AppsV1beta1Deployment.class, "Deployment");
        kindMap.put(V1beta2Deployment.class, "Deployment");
        kindMap.put(V1Job.class, "Job");
        kindMap.put(V1ConfigMap.class, "ConfigMap");

        apiMap.put(V1Deployment.class, AppsV1Api.class);
        apiMap.put(AppsV1beta1Deployment.class, AppsV1beta1Api.class);
        apiMap.put(V1beta2Deployment.class, AppsV1beta2Api.class);
        apiMap.put(V1Job.class, BatchV1Api.class);
        apiMap.put(V1ConfigMap.class, CoreV1Api.class);

        try {
            createMethodMap.put(V1Deployment.class,
                    AppsV1Api.class.getDeclaredMethod("createNamespacedDeployment", String.class, V1Deployment.class,
                            String.class, String.class, String.class));
            createMethodMap.put(AppsV1beta1Deployment.class,
                    AppsV1beta1Api.class
                            .getDeclaredMethod("createNamespacedDeployment", String.class, AppsV1beta1Deployment.class,
                                    String.class, String.class, String.class));
            createMethodMap.put(V1beta2Deployment.class,
                    AppsV1beta2Api.class
                            .getDeclaredMethod("createNamespacedDeployment", String.class, V1beta2Deployment.class,
                                    String.class, String.class, String.class));
            createMethodMap.put(V1Job.class,
                    BatchV1Api.class.getDeclaredMethod("createNamespacedJob", String.class, V1Job.class, String.class,
                            String.class, String.class));
            createMethodMap.put(V1ConfigMap.class,
                    CoreV1Api.class.getDeclaredMethod("createNamespacedConfigMap", String.class, V1ConfigMap.class,
                            String.class, String.class, String.class));

            deleteMethodMap.put(V1Deployment.class,
                    AppsV1Api.class
                            .getDeclaredMethod("deleteNamespacedDeployment", String.class, String.class, String.class,
                                    V1DeleteOptions.class, String.class, Integer.class, Boolean.class, String.class));
            deleteMethodMap.put(AppsV1beta1Deployment.class,
                    AppsV1beta1Api.class
                            .getDeclaredMethod("deleteNamespacedDeployment", String.class, String.class, String.class,
                                    V1DeleteOptions.class, String.class, Integer.class, Boolean.class, String.class));
            deleteMethodMap.put(V1beta2Deployment.class,
                    AppsV1beta2Api.class
                            .getDeclaredMethod("deleteNamespacedDeployment", String.class, String.class, String.class,
                                    V1DeleteOptions.class, String.class, Integer.class, Boolean.class, String.class));
            deleteMethodMap.put(V1ConfigMap.class,
                    CoreV1Api.class
                            .getDeclaredMethod("deleteNamespacedConfigMap", String.class, String.class, String.class,
                                    V1DeleteOptions.class, String.class, Integer.class, Boolean.class, String.class));
        } catch (Exception e) {
            LOGGER.error("Failed to register k8s object create/delete method");
        }
    }

    private final List<Object> deploymentList = new ArrayList<>();
    private final List<Object> jobList = new ArrayList<>();
    private Properties config;
    private String namespace;
    private String scheduleId;
    private long execId;
    private List<Object> kubernetesObjects;
    private ApiClient apiClient;

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        LOGGER = getTaskLogger();
        LOGGER.info("Task event: " + event.toJson());
        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        execId = event.getContext().getExecuteInfo().getId();
        Map<String, Object> extraInfo = JsonUtils.readMap(event.getContext().getTaskInfo().getExtraInfo());
        namespace = DEFAULT_NAMESPACE;
        if (extraInfo.containsKey("namespace")) {
            namespace = (String) extraInfo.get("namespace");
        } else {
            namespace = DEFAULT_NAMESPACE;
        }

        //load config
        String path = System.getProperty("JOBNAVI_HOME");
        String type = event.getContext().getTaskInfo().getType().getName();
        String configurationDirectory = String.format(CONF_PATH_PATTERN, path, type);
        File configDirectory = new File(configurationDirectory);
        LOGGER.info("Using configuration directory " + configDirectory.getAbsolutePath());
        config = new Properties();
        try (FileInputStream configFile = new FileInputStream(configurationDirectory + "/" + CONF_FILE_NAME)) {
            config.load(configFile);
        }

        String clusterBasePath = config.getProperty("cluster.url");
        String apiToken = config.getProperty("api.token");
        apiClient = ClientBuilder.defaultClient().setBasePath(clusterBasePath).setVerifyingSsl(false);
        apiClient.setApiKeyPrefix("Bearer");
        apiClient.setApiKey(apiToken);
        File yamlFileDirectory = new File(String.format(YAML_FILE_PATH_PATTERN, path, type));
        loadKubernetesObjects(yamlFileDirectory);
        if (deploymentList.size() > 0) {
            Thread.currentThread().join();
        } else if (jobList.size() > 0) {
            submit();
            if (jobList.size() > 0) {
                try {
                    //wait till all jobs are completed
                    for (Object job : jobList) {
                        String jobName = getJobName(job);
                        BatchV1Api apiInstance = new BatchV1Api(apiClient);
                        V1Job result = apiInstance.readNamespacedJob(jobName, namespace, "ture", null, null);
                        V1JobStatus status = result.getStatus();
                        while (status == null || status.getStartTime() == null) {
                            Thread.sleep(1000);
                            result = apiInstance.readNamespacedJob(jobName, namespace, "ture", null, null);
                            status = result.getStatus();
                        }
                        int running = status.getActive() == null ? 0 : status.getActive();
                        while (running > 0) {
                            result = apiInstance.readNamespacedJob(jobName, namespace, "ture", null, null);
                            status = result.getStatus();
                            running = status.getActive() == null ? 0 : status.getActive();
                        }
                        logger.info(result);
                        int total = result.getSpec().getCompletions();
                        int succeeded = status.getSucceeded() == null ? 0 : status.getSucceeded();
                        logger.info("job result:" + succeeded + "/" + total);
                        if (succeeded < total) {
                            throw new NaviException("k8s job failed");
                        }
                    }
                } catch (ApiException e) {
                    logger.error("Exception when calling AppsV1Api#readNamespacedJob");
                    logger.error("Status code: " + e.getCode());
                    logger.error("Reason: " + e.getResponseBody());
                    logger.error("Response headers: " + e.getResponseHeaders());
                    logger.error(e);
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * submit k8s objects
     */
    public void submit() {
        if (kubernetesObjects != null) {
            for (Object object : kubernetesObjects) {
                try {
                    logger.info("object:" + object.getClass());
                    Object apiInstance = apiMap.get(object.getClass()).getDeclaredConstructor(ApiClient.class)
                            .newInstance(apiClient);
                    logger.info("API:" + apiInstance.getClass());
                    Method createMethod = createMethodMap.get(object.getClass());
                    Object result = createMethod.invoke(apiInstance, namespace, object, null, null, null);
                    logger.info(result);
                } catch (InvocationTargetException e) {
                    if (e.getTargetException() instanceof ApiException) {
                        ApiException targetException = (ApiException) e.getTargetException();
                        logger.error("Exception when submitting yaml resource");
                        logger.error("Status code: " + targetException.getCode());
                        logger.error("Reason: " + targetException.getResponseBody());
                        logger.error("Response headers: " + targetException.getResponseHeaders());
                    }
                    logger.error(e);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
            if (deploymentList.size() > 0) {
                try {
                    for (Object object : deploymentList) {
                        Object apiInstance = apiMap.get(object.getClass()).getDeclaredConstructor(ApiClient.class)
                                .newInstance(apiClient);
                        Method readMethod = apiInstance.getClass()
                                .getDeclaredMethod("readNamespacedDeployment", String.class, String.class, String.class,
                                        Boolean.class, Boolean.class);
                        String deploymentName = getDeploymentName(object);
                        Object result = readMethod.invoke(apiInstance, deploymentName, namespace, null, null, null);
                        while (getDeploymentReplica(result) == null) {
                            Thread.sleep(1000);
                            result = readMethod.invoke(apiInstance, deploymentName, namespace, null, null, null);
                        }
                        logger.info(result);
                    }
                } catch (InvocationTargetException e) {
                    if (e.getTargetException() instanceof ApiException) {
                        ApiException targetException = (ApiException) e.getTargetException();
                        logger.error("Exception when getting deployment replica");
                        logger.error("Status code: " + targetException.getCode());
                        logger.error("Reason: " + targetException.getResponseBody());
                        logger.error("Response headers: " + targetException.getResponseHeaders());
                    }
                    logger.error(e);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * delete k8s objects
     */
    public void stop() {
        if (kubernetesObjects != null) {
            for (Object object : kubernetesObjects) {
                try {
                    Object apiInstance = apiMap.get(object.getClass()).getDeclaredConstructor(ApiClient.class)
                            .newInstance(apiClient);
                    String name;
                    switch (kindMap.get(object.getClass())) {
                        case "Deployment":
                            name = getDeploymentName(object);
                            break;
                        case "ConfigMap":
                            name = getConfigMapName(object);
                            break;
                        case "Job":
                            name = getJobName(object);
                            break;
                        default:
                            name = null;
                    }
                    Method deleteMethod = deleteMethodMap.get(object.getClass());
                    Object result = deleteMethod
                            .invoke(apiInstance, name, namespace, null, null, null, null, null, null);
                    logger.info(result);
                } catch (InvocationTargetException e) {
                    if (e.getTargetException() instanceof ApiException) {
                        ApiException targetException = (ApiException) e.getTargetException();
                        logger.error("Exception when submitting yaml resource");
                        logger.error("Status code: " + targetException.getCode());
                        logger.error("Reason: " + targetException.getResponseBody());
                        logger.error("Response headers: " + targetException.getResponseHeaders());
                    }
                    logger.error(e);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        }
    }

    @Override
    public EventListener getEventListener(String eventName) {
        switch (eventName) {
            case "submit":
                return new SubmitEventListener(this);
            case "stop":
                return new StopEventListener(this);
            default:
                return null;
        }
    }

    /**
     * load k8s object from yaml files
     *
     * @param yamlFileDirectory
     * @throws Throwable
     */
    private void loadKubernetesObjects(File yamlFileDirectory) throws Exception {
        if (yamlFileDirectory.isDirectory()) {
            File[] yamlFileList = yamlFileDirectory.listFiles();
            if (yamlFileList != null) {
                List<Object> kubernetesObjects = new ArrayList<>();
                for (File yamlFile : yamlFileList) {
                    List<Object> objects = Yaml.loadAll(yamlFile);
                    for (Object object : objects) {
                        String kind = kindMap.get(object.getClass());
                        if (kind != null) {
                            switch (kind) {
                                case "Deployment":
                                    deploymentList.add(object);
                                    break;
                                case "Job":
                                    jobList.add(object);
                                    break;
                                default:
                                    break;
                            }
                            kubernetesObjects.add(object);
                        }
                    }
                }
                if (deploymentList.size() > 0 && jobList.size() > 0) {
                    throw new NaviException("Deployment and Job can not be submitted simultaneously");
                }
                this.kubernetesObjects = kubernetesObjects;
            }
        }
    }

    private String getDeploymentName(Object object) {
        if (object instanceof V1Deployment) {
            return ((V1Deployment) object).getMetadata().getName();
        } else if (object instanceof AppsV1beta1Deployment) {
            return ((AppsV1beta1Deployment) object).getMetadata().getName();
        } else if (object instanceof V1beta2Deployment) {
            return ((V1beta2Deployment) object).getMetadata().getName();
        }
        return null;
    }

    private Integer getDeploymentReplica(Object object) {
        if (object instanceof V1Deployment) {
            return ((V1Deployment) object).getStatus() != null ? ((V1Deployment) object).getStatus().getReplicas()
                    : null;
        } else if (object instanceof AppsV1beta1Deployment) {
            return ((AppsV1beta1Deployment) object).getStatus() != null ? ((AppsV1beta1Deployment) object).getStatus()
                    .getReplicas() : null;
        } else if (object instanceof V1beta2Deployment) {
            return ((V1beta2Deployment) object).getStatus() != null ? ((V1beta2Deployment) object).getStatus()
                    .getReplicas() : null;
        }
        return null;
    }

    private String getConfigMapName(Object object) {
        if (object instanceof V1ConfigMap) {
            return ((V1ConfigMap) object).getMetadata().getName();
        }
        return null;
    }

    private String getJobName(Object object) {
        if (object instanceof V1Job) {
            return ((V1Job) object).getMetadata().getName();
        }
        return null;
    }

    static class SubmitEventListener implements EventListener {

        private final KubernetesTask task;

        SubmitEventListener(KubernetesTask task) {
            this.task = task;
        }

        @Override
        public TaskEventResult doEvent(TaskEvent event) {
            task.submit();
            TaskEventResult result = new TaskEventResult();
            result.setSuccess(true);
            return result;
        }
    }

    static class StopEventListener implements EventListener {

        private final KubernetesTask task;

        StopEventListener(KubernetesTask task) {
            this.task = task;
        }

        @Override
        public TaskEventResult doEvent(TaskEvent event) {
            task.stop();
            TaskEventResult result = new TaskEventResult();
            result.setSuccess(true);
            return result;
        }
    }
}
