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

package com.tencent.bk.base.dataflow.jobnavi.util.resource;

import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class ResourceUtil {

    private static final Logger LOGGER = Logger.getLogger(ResourceUtil.class);
    private static final String LIST_RESOURCE_GROUP_URL_PATTER = "%s?cluster_group=%s";
    private static final String LIST_CLUSTER_URL_PATTER
            = "%s?resource_group_id=%s&geog_area_code=%s&resource_type=%s&service_type=%s&component_type=%s";
    private static final String QUERY_INSTANCE_URL_PATTER = "%s?inst_id=%s";
    private static String requestUrl;
    private static String listResourceGroupPath;
    private static String listClusterPath;
    private static String retrieveClusterPath;
    private static String registerInstancesPath;
    private static String updateJobSubmitPath;
    private static String queryInstancePath;
    private static String retrieveInstancePath;
    private static int retryTimes;

    static {
        try {
            Configuration config = new Configuration(true);
            requestUrl = config.getString(Constants.BKDATA_API_RESOURCECENTER_URL);
            listResourceGroupPath = config.getString(Constants.BKDATA_API_RESOURCECENTER_RESOURCE_GROUP_LIST_PATH);
            listClusterPath = config.getString(Constants.BKDATA_API_RESOURCECENTER_CLUSTER_LIST_PATH);
            retrieveClusterPath = config.getString(Constants.BKDATA_API_RESOURCECENTER_CLUSTER_RETRIEVE_PATH);
            registerInstancesPath = config
                    .getString(Constants.BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_REGISTER_PATH);
            updateJobSubmitPath = config.getString(Constants.BKDATA_API_RESOURCECENTER_JOB_SUBMIT_UPDATE_PATH);
            queryInstancePath = config.getString(Constants.BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_QUERY_PATH);
            retrieveInstancePath = config
                    .getString(Constants.BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_RETRIEVE_PATH);
            retryTimes = config.getInt(Constants.BKDATA_API_RESOURCECENTER_RETRY_TIMES,
                    Constants.BKDATA_API_RESOURCECENTER_RETRY_TIMES_DEFAULT);
        } catch (IOException e) {
            LOGGER.error("Failed to initialize resource util");
        }
    }

    /**
     * retrieve resource group info by cluster group ID through resource center api:
     *
     * <p><pre>{@code
     * apiPath {get} /resourcecenter/resource_geog_area_cluster_group/
     *
     * apiParam {string} [cluster_group] cluster group ID
     * apiParamExample {json}:
     * {
     *     "cluster_group": "default"
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *      "errors": null,
     *     "message": "ok",
     *     "code": "1500200",
     *     "data": [
     *         {
     *             "geog_area_code": "inland",
     *             "resource_group_id": "default",
     *             "created_at": "2020-02-28T17:17:51",
     *             "updated_at": "2020-02-28T17:17:51",
     *             "created_by": "xx",
     *             "cluster_group": "default"
     *         }
     *     ],
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param clusterGroupId unique cluster group ID
     * @return matched resource group info map
     *     {
     *         "geog_area_code": "inland",
     *         "resource_group_id": "default",
     *     }
     */
    public static ResourceGroupInfo retrieveResourceGroupInfo(String clusterGroupId) {
        if (clusterGroupId == null) {
            return null;
        }
        String fullRequestURL = String
                .format(LIST_RESOURCE_GROUP_URL_PATTER, requestUrl + listResourceGroupPath, clusterGroupId);
        String response = requestWithRetry(HttpRequestMethod.GET, fullRequestURL, null, retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, List.class);
        if (responseData != null && !((List<?>) responseData).isEmpty()
                && ((List<?>) responseData).get(0) instanceof Map<?, ?>) {
            ResourceGroupInfo resourceGroupInfo = new ResourceGroupInfo();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) ((List<?>) responseData).get(0)).entrySet()) {
                if (entry.getKey() instanceof String) {
                    switch ((String) entry.getKey()) {
                        case "resource_group_id":
                            if (entry.getValue() instanceof String) {
                                resourceGroupInfo.setResourceGroupId((String) entry.getValue());
                            }
                            break;
                        case "geog_area_code":
                            if (entry.getValue() instanceof String) {
                                resourceGroupInfo.setGeogAreaCode((String) entry.getValue());
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            return resourceGroupInfo;
        }
        return null;
    }

    /**
     * list cluster info through resource center api:
     *
     * <p><pre>{@code
     * apiPath {get} /resourcecenter/clusters/
     * apiParam {string} [resource_group_id]
     * apiParam {string} [geog_area_code]
     * apiParam {string} [resource_type]: processing/storage/schedule
     * apiParam {string} [service_type]: batch/stream/hdfs/...
     * apiParam {string} [component_type]: spark/flink/hdfs/...
     * apiParam {string} [cluster_type]: yarn/jobnavi/...
     *
     * apiParamExample {json}:
     * {
     *     "resource_group_id": "default",
     *     "geog_area_code": "inland",
     *     "resource_type": "processing",
     *     "service_type": "batch",
     *     "component_type": "spark",
     *     "cluster_type": "yarn",
     *     "active": 1
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "errors": null,
     *     "message": "ok",
     *     "code": "1500200",
     *     "data": [
     *         {
     *             "resource_group_id": "default_test_00",
     *             "updated_at": "2020-02-20T14:33:02",
     *             "available_cpu": 10000.0,
     *             "cluster_id": "yarn-sz-01",
     *             "created_by": "xx",
     *             "disk": 0.0,
     *             "updated_by": null,
     *             "slot": 0.0,
     *             "cluster_name": "yarn1",
     *             "available_memory": 100000000.0,
     *             "cluster_type": "yarn",
     *             "available_gpu": 0.0,
     *             "available_slot": 0.0,
     *             "available_net": 0.0,
     *             "memory": 100000000.0,
     *             "gpu": 0.0,
     *             "net": 0.0,
     *             "description": "yarn1",
     *             "splitable": "1",
     *             "active": "1",
     *             "geog_area_code": "inland",
     *             "available_disk": 0.0,
     *             "component_type": "yarn",
     *             "created_at": "2020-02-20T14:33:02",
     *             "cpu": 10000.0
     *         }
     *     ],
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param resourceGroupId resource_group_id
     * @param geogAreaCode geog_area_code
     * @param resourceType resource_type
     * @param serviceType service_type
     * @param componentType component_type
     * @param clusterType cluster_type
     * @return matched cluster info map
     *     {
     *         "cluster_id": "yarn-sz-01",
     *         "cluster_name": "yarn1",
     *         "cluster_type": "yarn",
     *         "resource_type": "processing"
     *     }
     */
    public static CLusterInfo listClusterInfo(String resourceGroupId, String geogAreaCode,
            String resourceType, String serviceType,
            String componentType, String clusterType) {
        String fullRequestURL = String.format(LIST_CLUSTER_URL_PATTER, requestUrl + listClusterPath,
                resourceGroupId, geogAreaCode, resourceType, serviceType, componentType);
        String response = requestWithRetry(HttpRequestMethod.GET, fullRequestURL, null, retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, List.class);
        if (responseData != null && !((List<?>) responseMap.get("data")).isEmpty()) {
            for (Object info : (List<?>) responseData) {
                if (info instanceof Map<?, ?>) {
                    //if cluster type given, retrieve the matched one
                    if (clusterType != null && ! clusterType.equals(((Map<?, ?>) info).get("cluster_type"))) {
                        continue;
                    }
                    CLusterInfo cLusterInfo = new CLusterInfo();
                    cLusterInfo.setResourceType(resourceType);
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) ((List<?>) responseData).get(0))
                            .entrySet()) {
                        if (!(entry.getKey() instanceof String)) {
                            continue;
                        }
                        switch ((String) entry.getKey()) {
                            case "cluster_id":
                                if (entry.getValue() instanceof String) {
                                    cLusterInfo.setClusterId((String) entry.getValue());
                                }
                                break;
                            case "cluster_name":
                                if (entry.getValue() instanceof String) {
                                    cLusterInfo.setClusterName((String) entry.getValue());
                                }
                                break;
                            case "cluster_type":
                                if (entry.getValue() instanceof String) {
                                    cLusterInfo.setClusterType((String) entry.getValue());
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    return cLusterInfo;
                }
            }
        }
        return null;
    }

    /**
     * retrieve cluster info through resource center api:
     *
     * <p><pre>{@code
     * apiPath {get} /resourcecenter/clusters/:cluster_id/
     * apiParam {string} cluster_id
     *
     * apiParamExample {json}:
     * {
     *     "cluster_id": "cluster_123",
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "errors": null,
     *     "message": "ok",
     *     "code": "1500200",
     *     "data": [
     *         {
     *             "updated_at": "2020-02-20T13:55:55",
     *             "available_cpu": 5000.0,
     *             "cluster_id": "flink-session-sz-01",
     *             "created_by": "xx",
     *             "disk": 0.0,
     *             "updated_by": "xx",
     *             "slot": 0.0,
     *             "cluster_name": "深圳flink-session集群1",
     *             "available_memory": 50000000.0,
     *             "cluster_type": "flink-session",
     *             "available_gpu": 0.0,
     *             "available_slot": 0.0,
     *             "available_net": 0.0,
     *             "memory": 100000000.0,
     *             "gpu": 0.0,
     *             "net": 0.0,
     *             "description": "深圳yarn1",
     *             "splitable": "1",
     *             "active": "1",
     *             "geog_area_code": "inland",
     *             "resource_group_id": "default",
     *             "resource_type": "processing",
     *             "service_type": "stream",
     *             "src_cluster_id": "123456",
     *             "available_disk": 0.0,
     *             "component_type": "yarn",
     *             "created_at": "2020-02-20T13:55:55",
     *             "cpu": 10000.0
     *         }
     *     ],
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param clusterId cluster_id
     * @return matched cluster info map
     *     {
     *         "cluster_id": "yarn-sz-01",
     *         "cluster_name": "yarn1",
     *         "cluster_type": "yarn",
     *         "resource_type": "processing"
     *     }
     */
    public static CLusterInfo retrieveClusterInfo(String clusterId) {
        String fullRequestURL = String.format(requestUrl + retrieveClusterPath, clusterId);
        String response = requestWithRetry(HttpRequestMethod.GET, fullRequestURL, null, retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, Map.class);
        if (responseData != null && responseMap.get("data") instanceof Map) {
            CLusterInfo cLusterInfo = new CLusterInfo();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) responseData).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    continue;
                }
                switch ((String) entry.getKey()) {
                    case "cluster_id":
                        if (entry.getValue() instanceof String) {
                            cLusterInfo.setClusterId((String) entry.getValue());
                        }
                        break;
                    case "cluster_name":
                        if (entry.getValue() instanceof String) {
                            cLusterInfo.setClusterName((String) entry.getValue());
                        }
                        break;
                    case "cluster_type":
                        if (entry.getValue() instanceof String) {
                            cLusterInfo.setClusterType((String) entry.getValue());
                        }
                        break;
                    case "resource_type":
                        if (entry.getValue() instanceof String) {
                            cLusterInfo.setResourceType((String) entry.getValue());
                        }
                        break;
                    default:
                        break;
                }
            }
            return cLusterInfo;
        }
        return null;
    }

    /**
     * register job submit instances through resource center api:
     *
     * <p><pre>{@code
     * apiPath {post} /resourcecenter/job_submit/register_instances/
     * apiParam {string} [job_id]
     * apiParam {string} [resource_group_id]
     * apiParam {string} [geog_area_code]
     * apiParam {string} [status]
     * apiParam {string} cluster_id
     * apiParam {string} cluster_name
     * apiParam {string} resource_type: processing, storage, schedule
     * apiParam {string} cluster_type: yarn/jobnavi/...
     * apiParam {string} inst_id: exec_id/yarn application ID/...
     * apiParamExample {json}:
     * {
     *     "job_id": "123_test_job",
     *     "resource_group_id": "default",
     *     "geog_area_code": "inland",
     *     "status": "submitted",
     *     "instances": [
     *         {
     *             "cluster_id": "default",
     *             "cluster_name": null,
     *             "resource_type": "schedule",
     *             "cluster_type": "jobnavi",
     *             "inst_id": "123456"
     *         },
     *         {
     *             "cluster_id": "default",
     *             "cluster_name": "root.dataflow.batch.default",
     *             "resource_type": "processing",
     *             "cluster_type": "yarn",
     *             "inst_id": "application_123_456"
     *         }
     *     ]
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "message": "ok",
     *     "code": "1500200"
     *     "data": {
     *         "submit_id": 10086
     *     },
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param jobId job_id
     * @param resourceGroupId resource_group_id
     * @param geogAreaCode geog_area_code
     * @return submit ID
     */
    public static Long registerInstances(String jobId, String resourceGroupId, String geogAreaCode,
            List<JobSubmitInstance> instances) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("job_id", jobId);
        requestBody.put("resource_group_id", resourceGroupId);
        requestBody.put("geog_area_code", geogAreaCode);
        requestBody.put("status", "running");
        List<Map<String, Object>> instanceList = new ArrayList<>();
        for (JobSubmitInstance instance : instances) {
            Map<String, Object> instanceMap = new HashMap<>();
            instanceMap.put("cluster_id", instance.getClusterId());
            instanceMap.put("cluster_name", instance.getClusterName());
            instanceMap.put("resource_type", instance.getResourceType());
            instanceMap.put("cluster_type", instance.getClusterType());
            instanceMap.put("inst_id", instance.getInstId());
            instanceList.add(instanceMap);
        }
        requestBody.put("instances", instanceList);
        String response = requestWithRetry(HttpRequestMethod.POST, requestUrl + registerInstancesPath, requestBody,
                retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, Map.class);
        if (responseData != null) {
            Object submitId = ((Map<?, ?>) responseData).get("submit_id");
            if (submitId instanceof Long || submitId instanceof Integer) {
                return Long.parseLong(submitId.toString());
            }
        }
        return null;
    }

    /**
     * register job submit instances through resource center api:
     *
     * <p><pre>{@code
     * apiPath {post} /resourcecenter/job_submit/register_instances/
     * apiParam {long} [submit_id]
     * apiParam {string} cluster_id
     * apiParam {string} cluster_name
     * apiParam {string} resource_type: processing, storage, schedule
     * apiParam {string} cluster_type: yarn/jobnavi/...
     * apiParam {string} inst_id: exec_id/yarn application ID/...
     * apiParamExample {json}:
     * {
     *     "submit_id": 10086,
     *     "instances": [
     *         {
     *             "cluster_id": "default",
     *             "cluster_name": null,
     *             "resource_type": "schedule",
     *             "cluster_type": "jobnavi",
     *             "inst_id": "123456"
     *         },
     *         {
     *             "cluster_id": "default",
     *             "cluster_name": "root.dataflow.batch.default",
     *             "resource_type": "processing",
     *             "cluster_type": "yarn",
     *             "inst_id": "application_123_456"
     *         }
     *     ]
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "message": "ok",
     *     "code": "1500200"
     *     "data": {
     *         "submit_id": 10086
     *     },
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param submitId submit_id
     * @return submit ID
     */
    public static Long registerInstances(Long submitId, List<JobSubmitInstance> instances) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("submit_id", submitId);
        List<Map<String, Object>> instanceList = new ArrayList<>();
        for (JobSubmitInstance instance : instances) {
            Map<String, Object> instanceMap = new HashMap<>();
            instanceMap.put("cluster_id", instance.getClusterId());
            instanceMap.put("cluster_name", instance.getClusterName());
            instanceMap.put("resource_type", instance.getResourceType());
            instanceMap.put("cluster_type", instance.getClusterType());
            instanceMap.put("inst_id", instance.getInstId());
            instanceList.add(instanceMap);
        }
        requestBody.put("instances", instanceList);
        String response = requestWithRetry(HttpRequestMethod.POST, requestUrl + registerInstancesPath, requestBody,
                retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, Map.class);
        if (responseData != null) {
            Object responseSubmitId = ((Map<?, ?>) responseData).get("submit_id");
            if (responseSubmitId instanceof Long || responseSubmitId instanceof Integer
                    || responseSubmitId instanceof String) {
                return Long.parseLong(submitId.toString());
            }
        }
        return null;
    }

    /**
     * register job submit instances through resource center api:
     *
     * <p><pre>{@code
     * apiPath {patch} /resourcecenter/job_submit/:submit_id/
     * apiParam {string} job_id
     * apiParam {string} resource_group_id
     * apiParam {string} geog_area_code
     * apiParam {string} status
     * apiParamExample {json}:
     * {
     *     "status": "submitted",
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "message": "ok",
     *     "code": "1500200"
     *     "data": {
     *         "submit_id": 10086
     *     },
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param submitId submit_id
     * @param status status
     * @return submit ID
     */
    public static Long updateJobSubmitStatus(Long submitId, String status) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("status", status);
        String response = requestWithRetry(HttpRequestMethod.PATCH,
                String.format(requestUrl + updateJobSubmitPath, submitId), requestBody, retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, Map.class);
        if (responseData != null) {
            Object responseSubmitId = ((Map<?, ?>) responseData).get("submit_id");
            if (responseSubmitId instanceof Long || responseSubmitId instanceof Integer
                    || responseSubmitId instanceof String) {
                return Long.parseLong(responseSubmitId.toString());
            }
        }
        return null;
    }

    /**
     * query job submit instances by inst_id through resource center api:
     *
     * <p><pre>{@code
     * apiPath {get} /resourcecenter/job_submit/query_instances/
     * apiParam {string} [cluster_id]
     * apiParam {string} [cluster_name]
     * apiParam {string} [resource_type]：processing/storage/schedule
     * apiParam {string} [cluster_type]：yarn/jobnavi/...
     * apiParam {string} [inst_id]：exec_id/yarn application ID/...
     * apiParamExample {json}:
     * {
     *     "inst_id": 123456,
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "message": "ok",
     *     "code": "1500200"
     *     "data": [
     *         {
     *             "submit_id": 10086,
     *             "cluster_id": "default",
     *             "cluster_name": null,
     *             "resource_type": "schedule",
     *             "cluster_type": "jobnavi",
     *             "inst_id": "123456"
     *         }
     *     ],
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param instId inst_id
     * @return matched job submit instance list
     */
    public static List<JobSubmitInstance> queryJobSubmitInstances(String instId) {
        String fullRequestURL = String.format(QUERY_INSTANCE_URL_PATTER, requestUrl + queryInstancePath, instId);
        String response = requestWithRetry(HttpRequestMethod.GET, fullRequestURL, null, retryTimes);
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, List.class);
        if (responseData != null && !((List<?>) responseData).isEmpty()) {
            List<JobSubmitInstance> instanceList = new ArrayList<>();
            for (Object instance : (List<?>) responseData) {
                if (instance instanceof Map<?, ?>) {
                    JobSubmitInstance jobSubmitInstance = new JobSubmitInstance();
                    jobSubmitInstance.setInstId(instId);
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) instance).entrySet()) {
                        if (!(entry.getKey() instanceof String)) {
                            continue;
                        }
                        switch ((String) entry.getKey()) {
                            case "submit_id":
                                if (entry.getValue() instanceof Long) {
                                    jobSubmitInstance.setSubmitId((Long) entry.getValue());
                                }
                                break;
                            case "cluster_id":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterId((String) entry.getValue());
                                }
                                break;
                            case "cluster_name":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterName((String) entry.getValue());
                                }
                                break;
                            case "resource_type":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setResourceType((String) entry.getValue());
                                }
                                break;
                            case "cluster_type":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterType((String) entry.getValue());
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    instanceList.add(jobSubmitInstance);
                }
            }
            return instanceList;
        }
        return null;
    }

    /**
     * retrieve job submit instances by submit_id through resource center api:
     *
     * <p><pre>{@code
     * apiPath {get} /resourcecenter/job_submit/:submit_id/retrieve_instances/
     * apiParam {string} [cluster_name]
     * apiParam {string} [resource_type]：processing/storage/schedule
     * apiParam {string} [cluster_type]：yarn/jobnavi/...
     * apiParamExample {json}:
     * {
     *     "submit_id": 10086
     * }
     * apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {
     *     "message": "ok",
     *     "code": "1500200"
     *     "data": [
     *         {
     *             "submit_id": 10086,
     *             "cluster_id": "default",
     *             "cluster_name": null,
     *             "resource_type": "schedule",
     *             "cluster_type": "jobnavi",
     *             "inst_id": "123456"
     *         }
     *     ],
     *     "result": true
     * }
     * }</pre></p>
     *
     * @param submitId submit_id
     * @return matched job submit instance list
     */
    public static List<JobSubmitInstance> retrieveJobSubmitInstances(Long submitId) {
        String fullRequestURL = String.format(requestUrl + retrieveInstancePath, submitId);
        String response = requestWithRetry(HttpRequestMethod.GET, fullRequestURL, null, retryTimes);
        if (response == null) {
            //request error
            return null;
        }
        Map<String, Object> responseMap = JsonUtils.readMap(response);
        Object responseData = getResponseData(responseMap, List.class);
        if (responseData != null && !((List<?>) responseData).isEmpty()) {
            List<JobSubmitInstance> instanceList = new ArrayList<>();
            for (Object instance : (List<?>) responseData) {
                if (instance instanceof Map<?, ?>) {
                    JobSubmitInstance jobSubmitInstance = new JobSubmitInstance();
                    jobSubmitInstance.setSubmitId(submitId);
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) instance).entrySet()) {
                        if (!(entry.getKey() instanceof String)) {
                            continue;
                        }
                        switch ((String) entry.getKey()) {
                            case "cluster_id":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterId((String) entry.getValue());
                                }
                                break;
                            case "cluster_name":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterName((String) entry.getValue());
                                }
                                break;
                            case "resource_type":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setResourceType((String) entry.getValue());
                                }
                                break;
                            case "cluster_type":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setClusterType((String) entry.getValue());
                                }
                                break;
                            case "inst_id":
                                if (entry.getValue() instanceof String) {
                                    jobSubmitInstance.setInstId((String) entry.getValue());
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    instanceList.add(jobSubmitInstance);
                }
            }
            return instanceList;
        }
        return null;
    }

    private static Object getResponseData(Map<String, Object> responseMap, Class<?> dataType) {
        if (responseMap.containsKey("data") && responseMap.get("data") != null
                && dataType.isInstance(responseMap.get("data"))) {
            return responseMap.get("data");
        }
        return null;
    }

    private static String requestWithRetry(HttpRequestMethod method, String url, Map<String, Object> body,
            int retryTimes) {
        int retryCount = 0;
        while (true) {
            try {
                switch (method) {
                    case GET:
                        return HttpUtils.get(url);
                    case POST:
                        return HttpUtils.post(url, body);
                    case PATCH:
                        return HttpUtils.patch(url, body);
                    default:
                        LOGGER.error("Unsupported HTTP request method:" + method);
                        return null;
                }
            } catch (Exception e) {
                if (retryCount < retryTimes) {
                    ++retryCount;
                    LOGGER.warn("Failed to request URL:" + url + ", retry count:" + retryCount, e);
                    try {
                        Thread.sleep(1000 + retryCount);
                    } catch (InterruptedException interruptedException) {
                        LOGGER.warn("retry sleep interrupted");
                    }
                    continue;
                }
                LOGGER.error("Failed to request URL:" + url, e);
                return null;
            }
        }
    }

    private enum HttpRequestMethod {
        GET, POST, PATCH
    }
}
