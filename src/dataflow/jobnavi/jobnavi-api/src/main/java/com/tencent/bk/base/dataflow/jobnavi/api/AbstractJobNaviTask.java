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

package com.tencent.bk.base.dataflow.jobnavi.api;

import com.tencent.bk.base.dataflow.jobnavi.api.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.logging.ThreadLoggingFactory;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.CLusterInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.JobSubmitInstance;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceGroupInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public abstract class AbstractJobNaviTask implements JobNaviTask {

    protected Logger logger;

    private TaskEvent event;
    private JobSubmitInstance scheduleInstance;

    public Logger getTaskLogger() {
        return logger;
    }

    @Override
    public void run(TaskEvent event) throws Throwable {
        TaskMode mode = event.getContext().getTaskInfo().getType().getTaskMode();
        if (mode == TaskMode.process) {
            logger = Logger.getLogger(AbstractJobNaviTask.class);
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHoodThread(this)));
        } else {
            logger = ThreadLoggingFactory
                    .getLogger(event.getContext().getExecuteInfo().getId(), Thread.currentThread().getName());
        }
        this.event = event;
        startTask(event);
    }

    public abstract void startTask(TaskEvent event) throws Throwable;

    /**
     * save schedule savepoint
     *
     * @param values
     * @throws Exception
     */
    public void doScheduleSavepoint(Map<String, String> values) throws Exception {
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        Savepoint.save(event.getContext().getTaskInfo().getScheduleId(), values);
    }

    /**
     * save schedule savepoint
     *
     * @param values
     * @throws Exception
     */
    public void doSavepoint(Map<String, String> values) throws Exception {
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        Savepoint.save(event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getTaskInfo().getScheduleTime(), values);
    }


    /**
     * delete schedule savepoint
     *
     * @throws Exception
     */
    public void delSavepoint() throws Exception {
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        Savepoint.delete(event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getTaskInfo().getScheduleTime());
    }

    /**
     * delete schedule savepoint
     *
     * @throws Exception
     */
    public void delScheduleSavepoint() throws Exception {
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        Savepoint.delete(event.getContext().getTaskInfo().getScheduleId());
    }

    /**
     * get schedule savepoint
     *
     * @return save point map
     * @throws Exception
     */
    public Map<String, String> getScheduleSavepoint() throws Exception {
        Map<String, String> savepoint = new HashMap<>();
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        String savepointResult = Savepoint.get(event.getContext().getTaskInfo().getScheduleId());
        logger.info("get savepoint result: " + savepointResult);
        if (StringUtils.isNotEmpty(savepointResult)) {
            Map<String, Object> savePointResultMaps = JsonUtils.readMap(savepointResult);
            if (Boolean.parseBoolean(savePointResultMaps.get("result").toString())
                    && savePointResultMaps.get("data") != null) {
                Map<String, Object> readMap = JsonUtils.readMap(savePointResultMaps.get("data").toString());
                for (Map.Entry<String, Object> entry : readMap.entrySet()) {
                    savepoint.put(entry.getKey(), entry.getValue().toString());
                }
                return savepoint;
            }
        }
        return null;
    }

    /**
     * get schedule savepoint
     *
     * @return save point map
     * @throws Exception
     */
    public Map<String, String> getSavepoint() throws Exception {
        Map<String, String> savepoint = new HashMap<>();
        if (event == null) {
            throw new NullPointerException("event may not initial.");
        }
        String savepointStr = Savepoint.get(event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getTaskInfo().getScheduleTime());
        logger.info("savepoint: " + savepointStr);
        if (StringUtils.isNotEmpty(savepointStr)) {
            Map<String, Object> savePointResultMaps = JsonUtils.readMap(savepointStr);
            if (Boolean.parseBoolean(savePointResultMaps.get("result").toString())
                    && savePointResultMaps.get("data") != null) {
                Map<String, Object> readMap = JsonUtils.readMap(savePointResultMaps.get("data").toString());
                for (Map.Entry<String, Object> entry : readMap.entrySet()) {
                    savepoint.put(entry.getKey(), entry.getValue().toString());
                }
                return savepoint;
            }
        }
        return null;
    }

    /**
     * get current schedule cluster instance
     *
     * @return current schedule cluster instance
     */
    public JobSubmitInstance getScheduleInstance() {
        if (scheduleInstance == null) {
            Map<String, Object> extraInfoMap = JsonUtils.readMap(event.getContext().getTaskInfo().getExtraInfo());
            scheduleInstance = new JobSubmitInstance();
            String nodeLabel = event.getContext().getTaskInfo().getNodeLabel();
            String resourceGroupId = (String) extraInfoMap.get("resource_group_id");
            String geogAreaCode = (String) extraInfoMap.get("geog_area_code");
            if ((resourceGroupId == null || geogAreaCode == null) && extraInfoMap.containsKey("cluster_group_id")) {
                String clusterGroupId = (String) extraInfoMap.get("cluster_group_id");
                //get task node label from resource center
                ResourceGroupInfo resourceGroupInfo = ResourceUtil.retrieveResourceGroupInfo(clusterGroupId);
                if (resourceGroupInfo != null) {
                    resourceGroupId = resourceGroupInfo.getResourceGroupId();
                    geogAreaCode = resourceGroupInfo.getGeogAreaCode();
                }
            }
            if (resourceGroupId != null && geogAreaCode != null) {
                String serviceType = (String) extraInfoMap.get("service_type");
                String componentType = (String) extraInfoMap.get("component_type");
                logger.info(String.format(
                        "Resource group ID:%s, geog area code:%s, service type:%s, component_type:%s",
                        resourceGroupId, geogAreaCode, serviceType, componentType));
                //retrieve corresponding cluster info if exist
                scheduleInstance = getScheduleClusterInstance(resourceGroupId, geogAreaCode, serviceType,
                        componentType);
                if (scheduleInstance == null || !scheduleInstance.getClusterName().equals(nodeLabel)) {
                    scheduleInstance = getCustomScheduleClusterInstance(nodeLabel);
                }
            } else {
                scheduleInstance = getCustomScheduleClusterInstance(nodeLabel);
            }
        }
        return scheduleInstance;
    }

    protected JobSubmitInstance getScheduleClusterInstance(String resourceGroupId, String geogAreaCode,
            String serviceType, String componentType) {
        CLusterInfo cLusterInfo = ResourceUtil.listClusterInfo(resourceGroupId, geogAreaCode,
                Constants.JOBNAVI_RESOURCE_TYPE, serviceType, componentType, Constants.JOBNAVI_CLUSTER_TYPE);
        if (cLusterInfo != null && cLusterInfo.getClusterName() != null) {
            JobSubmitInstance sparkClusterInstance;
            sparkClusterInstance = new JobSubmitInstance();
            sparkClusterInstance.setClusterId(cLusterInfo.getClusterId());
            sparkClusterInstance.setClusterName(cLusterInfo.getClusterName());
            sparkClusterInstance.setResourceType(Constants.JOBNAVI_RESOURCE_TYPE);
            sparkClusterInstance.setClusterType(cLusterInfo.getClusterType());
            return sparkClusterInstance;
        }
        return null;
    }

    protected JobSubmitInstance getCustomScheduleClusterInstance(String nodeLabel) {
        JobSubmitInstance scheduleClusterInstance = new JobSubmitInstance();
        final String SPARK_CLUSTER_ID_CUSTOM = Constants.JOBNAVI_SCHEDULE_CLUSTER_ID_CUSTOM;
        final String RESOURCE_TYPE = Constants.JOBNAVI_RESOURCE_TYPE;
        final String CLUSTER_TYPE = Constants.JOBNAVI_CLUSTER_TYPE;
        scheduleClusterInstance.setClusterId(SPARK_CLUSTER_ID_CUSTOM);
        scheduleClusterInstance.setClusterName(nodeLabel);
        scheduleClusterInstance.setResourceType(RESOURCE_TYPE);
        scheduleClusterInstance.setClusterType(CLUSTER_TYPE);
        return scheduleClusterInstance;
    }

    protected void onShutdown() {
        if (logger != null) {
            //release task log file appender
            logger.removeAllAppenders();
        }
    }

    protected static class ShutdownHoodThread implements Runnable {

        private final AbstractJobNaviTask task;

        ShutdownHoodThread(AbstractJobNaviTask task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.onShutdown();
        }
    }
}
