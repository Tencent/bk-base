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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.node;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.conf.TaskInfoConstants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.exception.NotFoundException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.decommission.DecommissionManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.balance.AbstractNodeBalance;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.recovery.RunnerTaskRecoveryManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class HeartBeatManager {

    private static final Logger LOGGER = Logger.getLogger(HeartBeatManager.class);

    private static Map<String, RunnerInfo> nodeInfos;
    private static Map<String, Long> updateTimes;
    private static Thread heartBeatThread;
    private static AbstractNodeBalance nodeBalance;
    private static Map<String, Thread> nodeHealthMonitorThreads;
    private static Map<String, Long> lastActiveTimes;
    private static double cpuUsageThreshold;
    private static double cpuAvgLoadThreshold;
    private static double memoryUsageThreshold;

    private static long maxDecommissionTime;
    private static int heartbeatExpireTime;
    private static int healthExpireTime;

    public static int getHeartbeatExpireTime() {
        return heartbeatExpireTime;
    }

    public static int getHealthExpireTime() {
        return healthExpireTime;
    }

    /**
     * start heartbeat manager
     *
     * @param conf
     */
    public static void start(Configuration conf) throws NaviException {
        heartbeatExpireTime = conf.getInt(Constants.JOBNAVI_SCHEDULER_HEARTBEAT_RUNNER_EXPIRE_SECOND,
                Constants.JOBNAVI_SCHEDULER_HEARTBEAT_RUNNER_EXPIRE_SECOND_DEFAULT);
        healthExpireTime = conf.getInt(Constants.JOBNAVI_HEALTH_RUNNER_EXPIRE_SECOND,
                Constants.JOBNAVI_HEALTH_RUNNER_EXPIRE_SECOND_DEFAULT);
        maxDecommissionTime = conf.getInt(Constants.JOBNAVI_SCHEDULER_DECOMMISSION_MAX_TIME_SECOND,
                Constants.JOBNAVI_SCHEDULER_DECOMMISSION_MAX_TIME_SECOND_DEFAULT);
        String nodeBalanceClassName = conf.getString(Constants.JOBNAVI_SCHEDULER_NODE_BALANCE_CLASS,
                Constants.JOBNAVI_SCHEDULER_NODE_BALANCE_CLASS_DEFAULT);
        //only Runners with load under these thresholds considered available
        cpuUsageThreshold = conf.getDouble(Constants.JOBNAVI_HEALTH_RUNNER_CPU_USAGE_THRESHOLD,
                Constants.JOBNAVI_HEALTH_RUNNER_EXPIRE_CPU_USAGE_THRESHOLD_DEFAULT);
        cpuAvgLoadThreshold = conf.getDouble(Constants.JOBNAVI_HEALTH_RUNNER_CPU_AVG_LOAD_THRESHOLD,
                Constants.JOBNAVI_HEALTH_RUNNER_EXPIRE_CPU_AVG_LOAD_THRESHOLD_DEFAULT);
        memoryUsageThreshold = conf.getDouble(Constants.JOBNAVI_HEALTH_RUNNER_MEMORY_USAGE_THRESHOLD,
                Constants.JOBNAVI_HEALTH_RUNNER_EXPIRE_MEMORY_USAGE_THRESHOLD_DEFAULT);
        try {
            Constructor<? extends AbstractNodeBalance> c = Class.forName(nodeBalanceClassName)
                    .asSubclass(AbstractNodeBalance.class)
                    .getConstructor(Configuration.class);
            nodeBalance = c.newInstance(conf);
        } catch (InstantiationException
                | IllegalAccessException
                | ClassNotFoundException
                | InvocationTargetException
                | NoSuchMethodException e) {
            LOGGER.error("init node balance class error.", e);
            throw new NaviException(e);
        }
        nodeInfos = new ConcurrentHashMap<>();
        updateTimes = new ConcurrentHashMap<>();
        heartBeatThread = new Thread(new HeartBeatThread(), "heartbeat");
        heartBeatThread.start();
        nodeHealthMonitorThreads = new ConcurrentHashMap<>();
        lastActiveTimes = new ConcurrentHashMap<>();
    }

    /**
     * stop heartbeat manager
     */
    public static void stop() {
        if (nodeInfos != null) {
            nodeInfos.clear();
        }
        if (updateTimes != null) {
            updateTimes.clear();
        }
        if (heartBeatThread != null) {
            heartBeatThread.interrupt();
        }
        if (nodeHealthMonitorThreads != null) {
            for (Map.Entry<String, Thread> entry : nodeHealthMonitorThreads.entrySet()) {
                entry.getValue().interrupt();
            }
        }
        if (lastActiveTimes != null) {
            lastActiveTimes.clear();
        }
    }

    /**
     * receive heartbeat
     *
     * @param info
     */
    public static void receiveHeartBeat(RunnerInfo info) throws NaviException {
        if (info.getRunnerId() == null) {
            LOGGER.warn("runner ID of runner is null, ignored");
            return;
        }
        String runnerId = info.getRunnerId();
        if (!nodeInfos.containsKey(runnerId)) {
            LOGGER.info("add new runner: " + info.getRunnerId() + ", status: " + info.getStatus() + ", info:" + info
                    .toJson());
            //register host label on this Runner
            if (info.getLabelSet() != null) {
                for (String label : info.getLabelSet()) {
                    NodeLabelManager.bindHostLabel(label, runnerId, true);
                }
            }
            RunnerTaskRecoveryManager.registerHost(info.getRunnerId());
            //start a thread for each login runner to check health status of it
            startNodeHealthMonitorThread(info.getRunnerId());
        } else {
            updateHostLabel(info);
        }
        doDecommission(info);
        nodeInfos.put(info.getRunnerId(), info);
        updateTimes.put(info.getRunnerId(), System.currentTimeMillis());
        RunnerTaskRecoveryManager.checkTaskStatus(info);
    }

    public static void updateHeartBeat(RunnerInfo info, boolean refreshUpdateTime) {
        if (info.getRunnerId() == null || !nodeInfos.containsKey(info.getRunnerId())) {
            LOGGER.warn("invalid runner, ignored");
            return;
        }
        LOGGER.info("update heartbeat of runner:" + info.getRunnerId());
        nodeInfos.put(info.getRunnerId(), info);
        if (refreshUpdateTime) {
            updateTimes.put(info.getRunnerId(), System.currentTimeMillis());
        }
    }

    private static void updateHostLabel(RunnerInfo newInfo) throws NaviException {
        String runnerId = newInfo.getRunnerId();
        RunnerInfo oldInfo = nodeInfos.get(runnerId);
        Set<String> oldLabelSet = oldInfo.getLabelSet();
        Set<String> newLabelSet = newInfo.getLabelSet();
        if ((oldLabelSet == null || oldLabelSet.isEmpty())) {
            if (newLabelSet != null) {
                for (String label : newLabelSet) {
                    NodeLabelManager.bindHostLabel(label, newInfo.getRunnerId(), true);
                }
            }
        } else if (newLabelSet == null || !oldLabelSet.containsAll(newLabelSet) || !newLabelSet
                .containsAll(oldLabelSet)) {
            //new label set not equal to the old one
            for (String label : oldLabelSet) {
                NodeLabelManager.unbindHostLabel(label, oldInfo.getRunnerId());
            }
            if (newLabelSet != null) {
                for (String label : newLabelSet) {
                    NodeLabelManager.bindHostLabel(label, newInfo.getRunnerId(), true);
                }
            }
        }
    }

    private static void doDecommission(RunnerInfo info) {
        RunnerInfo lastHeartBeatInfo = nodeInfos.get(info.getRunnerId());
        if (lastHeartBeatInfo == null) {
            return;
        }
        RunnerStatus lastRunnerStatus = lastHeartBeatInfo.getStatus();
        if (lastRunnerStatus == RunnerStatus.decommissioning) {
            switch (info.getStatus()) {
                case running:
                    info.setStatus(RunnerStatus.decommissioning);
                    break;
                case decommissioning:
                    long interval = System.currentTimeMillis() - DecommissionManager
                            .getRunnerDecommissionTime(info.getRunnerId());
                    if (interval / 1000 > maxDecommissionTime) {
                        LOGGER.error(String.format("runner(%s) decommission maybe lost, cost %s second .",
                                info.getRunnerId(), maxDecommissionTime));
                        DecommissionManager.rollbackDecommission(info.getRunnerId());
                        DecommissionManager.recoveryExecute(info);
                        info.setStatus(RunnerStatus.running);
                    }
                    break;
                case decommissioned:
                    LOGGER.info(String.format("host(%s) decommission succeed", info.getRunnerId()));
                    break;
                default:
                    LOGGER.error("Unsupported runner status:" + info.getStatus());
            }
        }
    }


    /**
     * remove runner node
     *
     * @param host
     * @throws NaviException
     */
    public static void removeNode(String host) throws NaviException {
        //host remove black list
        NodeBlackListManager.healthy(host);
        expireTaskOnNode(host);
        if (nodeInfos.get(host) != null) {
            RunnerInfo node = nodeInfos.get(host);
            Set<String> labelSet = node.getLabelSet();
            if (labelSet != null && !labelSet.isEmpty()) {
                String runnerId = node.getRunnerId();
                for (String label : labelSet) {
                    NodeLabelManager.unbindHostLabel(label, runnerId);
                }
            }
            nodeInfos.remove(host);
            updateTimes.remove(host);
            stopNodeHealthMonitorThread(host);
            lastActiveTimes.remove(host);
        }
    }

    private static void expireTaskOnNode(String host) throws NaviException {
        Set<Long> executeTasks = RunnerTaskRecoveryManager.getExecuteTasks(host);
        for (long executeId : executeTasks) {
            try {
                //running tasks on Runner expired considered lost
                DefaultTaskEvent event = (DefaultTaskEvent) TaskEventFactory.generateTaskEvent("lost");
                event.setEventName("lost");
                EventContext context = TaskStateManager.generateEventContext(executeId);
                context.setEventInfo(TaskInfoConstants.NODE_EXPIRE_INFO);
                event.setContext(context);
                event.setChangeStatus(TaskStatus.lost);
                TaskStateManager.dispatchEvent(event);
                RunnerTaskRecoveryManager.unregisterTask(host, executeId);
            } catch (NotFoundException e) {
                //schedule info of this execute has already been removed
                LOGGER.error("fail to generate event context of job instance:" + executeId + ", ignored, detail:", e);
            } catch (NaviException e) {
                LOGGER.error("fail to dispatch lost event of job instance:" + executeId + ", detail:", e);
                throw e;
            }
        }
    }

    /**
     * get best node
     *
     * @return RunnerInfo
     */
    public static RunnerInfo getBestNode(String nodeLabel, boolean isProcessTask) {
        if (nodeInfos.size() == 0) {
            return null;
        }
        RunnerInfo bestNode = null;
        StringBuilder runnerQuotaString = new StringBuilder();
        for (RunnerInfo info : nodeInfos.values()) {
            runnerQuotaString.append(info.getQuotaString()).append(" ");
        }
        LOGGER.debug("current runner info is: " + runnerQuotaString.toString());

        List<RunnerInfo> runnerInfos = getAvailableRunner(nodeLabel, isProcessTask);
        LOGGER.info(runnerInfos.size() + " Runners available currently in label:" + nodeLabel);

        if (runnerInfos.size() == 0) {
            LOGGER.debug("Cannot find best node for Runner. Current Runner Info is: ");
            for (RunnerInfo info : nodeInfos.values()) {
                LOGGER.debug(info.getQuotaString());
            }
        } else {
            bestNode = nodeBalance.getBestRunner(runnerInfos);
            LOGGER.info("choose best is : " + bestNode.getRunnerId());
        }
        return bestNode;
    }

    /**
     * get available runner with given node label and task mode
     *
     * @param nodeLabel
     * @param isProcessTask
     * @return available runner list
     */
    public static List<RunnerInfo> getAvailableRunner(String nodeLabel, boolean isProcessTask) {
        List<RunnerInfo> runnerInfoList = new ArrayList<>();
        if (nodeLabel == null || NodeLabelManager.RESERVED_DEFAULT_LABEL.equals(nodeLabel)) { //default label
            for (RunnerInfo runnerInfo : nodeInfos.values()) {
                if (checkAvailable(runnerInfo, isProcessTask)
                        && !NodeLabelManager.hostHasLabel(runnerInfo.getRunnerId())) {
                    runnerInfoList.add(runnerInfo);
                }
            }
        } else {
            Set<String> nodeLabelHosts = NodeLabelManager.getHosts(nodeLabel);
            if (nodeLabelHosts != null) {
                for (RunnerInfo runnerInfo : nodeInfos.values()) {
                    if (checkAvailable(runnerInfo, isProcessTask)
                            && nodeLabelHosts.contains(runnerInfo.getRunnerId())) {
                        runnerInfoList.add(runnerInfo);
                    }
                }
            }
        }
        return runnerInfoList;
    }


    public static Integer getPort(String host) {
        return !(host == null) && nodeInfos.containsKey(host) ? nodeInfos.get(host).getPort() : null;
    }

    public static RunnerStatus getRunnerStatus(String host) {
        RunnerInfo info = nodeInfos.get(host);
        if (info == null) {
            return RunnerStatus.lost;
        }
        return info.getStatus();
    }

    public static void setRunnerStatus(String host, RunnerStatus status) {
        RunnerInfo info = nodeInfos.get(host);
        if (info == null) {
            return;
        }
        info.setStatus(status);
    }

    public static Map<String, RunnerInfo> getNodeInfos() {
        return nodeInfos;
    }

    private static boolean checkAvailable(RunnerInfo runnerInfo, boolean isProcessTask) {
        if (runnerInfo.getStatus() != RunnerStatus.running
                || (isProcessTask && runnerInfo.getMaxTaskNum() <= runnerInfo.getTaskNum())
                || (!isProcessTask && runnerInfo.getMaxThreadTaskNum() <= runnerInfo.getTaskThreadNum())) {
            return false;
        }
        return runnerInfo.getCpuUsage() <= cpuUsageThreshold
                && runnerInfo.getCpuAvgLoad() <= cpuAvgLoadThreshold
                && runnerInfo.getMemoryUsage() <= memoryUsageThreshold
                && !NodeBlackListManager.isBlackNode(runnerInfo.getRunnerId());
    }

    private static synchronized void startNodeHealthMonitorThread(String host) {
        if (!nodeHealthMonitorThreads.containsKey(host)) {
            Thread nodeHealthMonitorThread = new Thread(new NodeHealthMonitorThread(host), "node-health-" + host);
            nodeHealthMonitorThread.start();
            nodeHealthMonitorThreads.put(host, nodeHealthMonitorThread);
        }
    }

    private static synchronized void stopNodeHealthMonitorThread(String host) {
        if (nodeHealthMonitorThreads.containsKey(host)) {
            nodeHealthMonitorThreads.get(host).interrupt();
            nodeHealthMonitorThreads.remove(host);
        }
    }

    static class HeartBeatThread implements Runnable {

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long current = System.currentTimeMillis();
                    for (Map.Entry<String, Long> entry : updateTimes.entrySet()) {
                        String host = entry.getKey();
                        if (current - entry.getValue() > (long) heartbeatExpireTime * 1000) {
                            //Runner heartbeat expired but pass health check
                            if (lastActiveTimes.get(host) != null
                                    && current - lastActiveTimes.get(host) <= (long) heartbeatExpireTime * 1000
                                    && current - entry.getValue() <= (long) healthExpireTime * 1000) {
                                LOGGER.warn("Node:" + entry.getKey()
                                        + " heartbeat expired but still considered active since last health check:"
                                        + CronUtil.getPrettyTime(lastActiveTimes.get(host)));
                                //mark as unavailable until heartbeat received
                                if (getRunnerStatus(host) == RunnerStatus.running) {
                                    setRunnerStatus(host, RunnerStatus.unavailable);
                                }
                            } else {
                                LOGGER.warn("Node:" + entry.getKey() + " expired since last heartbeat:" + CronUtil
                                        .getPrettyTime(entry.getValue()) + ", removed.");
                                removeNode(entry.getKey());
                            }
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error("HeartBeat error.", e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("HeartBeat thread interrupted.");
                    break;
                }
            }
        }
    }

    static class NodeHealthMonitorThread implements Runnable {

        String host;

        NodeHealthMonitorThread(String host) {
            this.host = host;
        }

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    RunnerInfo runnerInfo = nodeInfos.get(host);
                    if (runnerInfo.getHost() != null && runnerInfo.getHealthPort() != null) {
                        String resultStr = HttpUtils
                                .get("http://" + runnerInfo.getHost() + ":" + runnerInfo.getPort() + "/health");
                        Map<String, Object> result = JsonUtils.readMap(resultStr);
                        if (Boolean.parseBoolean(result.get("result").toString()) && result.get("message").toString()
                                .equals("ok")) {
                            //update the last time when the runner is considered active
                            lastActiveTimes.put(host, System.currentTimeMillis());
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error("check health of node:" + host + " error.", e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info(Thread.currentThread().getName() + " thread interrupted.");
                    break;
                }
            }

        }
    }

}
