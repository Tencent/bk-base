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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.recovery;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.exception.NotFoundException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class RunnerTaskRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(RunnerTaskRecoveryManager.class);
    private static final Map<String, Long> lastCheckTimeMills = new HashMap<>();
    private static final Object lastCheckTimeMillsLock = new Object();

    private static boolean isReady;
    private static Map<String, Set<Long>> runnerExecuteTasks;
    private static Map<String, Map<Long, AtomicInteger>> lostTaskMark;
    private static Integer lostTaskMarkThreshold;
    private static Set<Long> lostTasks;
    private static Thread lostTaskRecoveryThread;
    private static Map<String, Map<Long, AtomicInteger>> zombieTaskMark;
    private static Integer zombieTaskMarkThreshold;
    private static Set<Long> zombieTasks;
    private static Thread zombieTaskClearThread;
    private static Thread runnerExecuteTasksClearThread;

    /**
     * start runner task recovery manager
     *
     * @param conf
     */
    public static synchronized void start(Configuration conf) {
        RunnerTaskRecoveryManager.isReady = false;
        runnerExecuteTasks = new ConcurrentHashMap<>();
        lostTaskMark = new ConcurrentHashMap<>();
        lostTaskMarkThreshold = conf.getInt(Constants.JOBNAVI_SCHEDULER_RECOVERY_TASK_LOST_MARK_THRESHOLD,
                Constants.JOBNAVI_SCHEDULER_RECOVERY_TASK_LOST_MARK_THRESHOLD_DEFAULT);
        lostTasks = new ConcurrentSkipListSet<>();
        lostTaskRecoveryThread = new Thread(new LostTaskRecoveryThread(), "lost-task-recovery-thread");
        lostTaskRecoveryThread.start();
        zombieTaskMark = new ConcurrentHashMap<>();
        zombieTaskMarkThreshold = conf.getInt(Constants.JOBNAVI_SCHEDULER_RECOVERY_TASK_ZOMBIE_MARK_THRESHOLD,
                Constants.JOBNAVI_SCHEDULER_RECOVERY_TASK_ZOMBIE_MARK_THRESHOLD_DEFAULT);
        zombieTasks = new ConcurrentSkipListSet<>();
        zombieTaskClearThread = new Thread(new ClearZombieTaskThread(), "zombie-task-clear-thread");
        zombieTaskClearThread.start();
        runnerExecuteTasksClearThread = new Thread(new RunnerExecuteTasksThread(), "runner-task-clear-thread");
        runnerExecuteTasksClearThread.start();
    }

    /**
     * stop runner task recovery manager
     */
    public static void stop() {
        if (zombieTaskClearThread != null) {
            zombieTaskClearThread.interrupt();
        }

        if (runnerExecuteTasksClearThread != null) {
            runnerExecuteTasksClearThread.interrupt();
        }

        synchronized (lastCheckTimeMillsLock) {
            if (lostTaskRecoveryThread != null) {
                lostTaskRecoveryThread.interrupt();
            }
        }

        lastCheckTimeMills.clear();

        if (runnerExecuteTasks != null) {
            runnerExecuteTasks.clear();
        }

        if (lostTaskMark != null) {
            lostTaskMark.clear();
        }

        if (zombieTaskMark != null) {
            zombieTaskMark.clear();
        }
    }

    /**
     * recover runner task recovery manager
     *
     * @param conf
     * @throws NaviException
     */
    public static void recovery(Configuration conf) throws NaviException {
        Map<String, Set<Long>> runnerExecute = MetaDataManager.getJobDao().listRunnerExecute();
        for (Map.Entry<String, Set<Long>> entry : runnerExecute.entrySet()) {
            String host = entry.getKey();
            registerHost(host);
            for (long execId : entry.getValue()) {
                registerTask(host, execId);
                LOGGER.info("Runner(" + host + ") task:" + execId + " recovered");
            }
        }
        RunnerTaskRecoveryManager.isReady = true;
    }

    /**
     * register Runner info when Runner login
     *
     * @param host Runner ID
     */
    public static void registerHost(String host) {
        synchronized (lastCheckTimeMillsLock) {
            if (!lastCheckTimeMills.containsKey(host)) {
                lastCheckTimeMills.put(host, System.currentTimeMillis());
            } else {
                return;
            }
        }
        runnerExecuteTasks.put(host, new ConcurrentSkipListSet<Long>());
        lostTaskMark.put(host, new ConcurrentHashMap<Long, AtomicInteger>());
        zombieTaskMark.put(host, new ConcurrentHashMap<Long, AtomicInteger>());
    }

    /**
     * unregister Runner info when Runner logout
     *
     * @param host
     */
    public static void unregisterHost(String host) {
        runnerExecuteTasks.remove(host);
        lostTaskMark.remove(host);
        zombieTaskMark.remove(host);
    }

    /**
     * register running task info when task launched successfully
     *
     * @param host Runner ID
     * @param execId task execute ID
     */
    public static void registerTask(String host, Long execId) {
        synchronized (lastCheckTimeMillsLock) {
            if (!lastCheckTimeMills.containsKey(host)) {
                return;
            }
        }
        runnerExecuteTasks.get(host).add(execId);
        lostTaskMark.get(host).put(execId, new AtomicInteger(0));
    }

    /**
     * unregister running task info when task is complete
     *
     * @param host Runner ID
     * @param execId task execute ID
     */
    public static void unregisterTask(String host, Long execId) {
        synchronized (lastCheckTimeMillsLock) {
            if (!lastCheckTimeMills.containsKey(host)) {
                return;
            }
        }
        runnerExecuteTasks.get(host).remove(execId);
        lostTaskMark.get(host).remove(execId);
    }

    public static Set<Long> getExecuteTasks(String host) {
        Set<Long> executeTasks = new HashSet<>();
        synchronized (lastCheckTimeMillsLock) {
            if (lastCheckTimeMills.containsKey(host) && runnerExecuteTasks.get(host) != null) {
                executeTasks.addAll(runnerExecuteTasks.get(host));
            }
        }
        return executeTasks;
    }

    /**
     * check task status on Runner when heartbeat received
     *
     * @param runnerInfo info reported by Runner
     */
    public static void checkTaskStatus(RunnerInfo runnerInfo) {
        if (runnerInfo.getExecIdSet() == null || !RunnerTaskRecoveryManager.isReady) {
            //disable Runner task status check if Runner does not report Running executes
            // or RunnerTaskRecoveryManager is recovering running task info
            return;
        }
        String host = runnerInfo.getRunnerId();
        synchronized (lastCheckTimeMillsLock) {
            if (!lastCheckTimeMills.containsKey(host)) {
                registerHost(host);
            } else {
                lastCheckTimeMills.put(host, System.currentTimeMillis());
            }
        }
        Set<Long> executeTasks = runnerExecuteTasks.get(host);
        if (executeTasks == null) {
            return;
        }
        for (Long execId : executeTasks) {
            if (!runnerInfo.getExecIdSet().contains(execId)) {
                //execute which is registered on Scheduler but not exist on corresponding Runner is considered lost
                int markTimes = lostTaskMark.get(host).get(execId).addAndGet(1);
                LOGGER.warn("Runner task:" + execId + " marked as lost for " + markTimes + " times");
                if (markTimes >= lostTaskMarkThreshold) {
                    lostTasks.add(execId);
                    unregisterTask(host, execId);
                }
            }
        }
        for (Long execId : runnerInfo.getExecIdSet()) {
            if (!executeTasks.contains(execId)) {
                //execute which is reported by Runner but not registered on Scheduler is considered zombie task
                if (zombieTaskMark.get(host).get(execId) == null) {
                    zombieTaskMark.get(host).put(execId, new AtomicInteger(0));
                }
                int markTimes = zombieTaskMark.get(host).get(execId).addAndGet(1);
                LOGGER.warn("Runner task:" + execId + " marked as zombie for " + markTimes + " times");
                if (markTimes >= zombieTaskMarkThreshold) {
                    zombieTasks.add(execId);
                    zombieTaskMark.get(host).remove(execId);
                }
            }
        }
        //clear redundant zombie task mark
        for (Long execId : zombieTaskMark.get(host).keySet()) {
            if (!runnerInfo.getExecIdSet().contains(execId)) {
                LOGGER.info("clearing redundant zombie task mark for execute:" + execId);
                zombieTaskMark.get(host).remove(execId);
            }
        }
    }

    static class LostTaskRecoveryThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Iterator<Long> iterator = lostTasks.iterator();
                while (iterator.hasNext()) {
                    Long execId = iterator.next();
                    try {
                        //dispatch lost event to recover recoverable lost task on Runner
                        LOGGER.info("recovering lost runner task:" + execId);
                        EventContext context = TaskStateManager.generateEventContext(execId);
                        DefaultTaskEvent event = (DefaultTaskEvent) TaskEventFactory.generateTaskEvent("lost");
                        event.setEventName("lost");
                        event.setContext(context);
                        event.setChangeStatus(TaskStatus.lost);
                        TaskStateManager.dispatchEvent(event);
                        iterator.remove();
                    } catch (NotFoundException e) {
                        //schedule info of this execute has already been removed
                        LOGGER.error("fail to generate lost event context of Runner task:" + execId + ", detail:", e);
                        iterator.remove();
                    } catch (NaviException e) {
                        LOGGER.error("fail to recover lost Runner task:" + execId + ", detail:", e);
                    }
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOGGER.info("Runner task lost interrupted.");
                    break;
                }
            }
        }
    }

    static class ClearZombieTaskThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Iterator<Long> iterator = zombieTasks.iterator();
                while (iterator.hasNext()) {
                    Long execId = iterator.next();
                    try {
                        //dispatch zombie event to clear zombie task on Runner
                        LOGGER.info("clearing zombie task:" + execId);
                        EventContext context = TaskStateManager.generateEventContext(execId);
                        DefaultTaskEvent event = (DefaultTaskEvent) TaskEventFactory.generateTaskEvent("zombie");
                        event.setEventName("zombie");
                        event.setContext(context);
                        TaskStateManager.dispatchEvent(event);
                        iterator.remove();
                    } catch (NotFoundException e) {
                        //schedule info of this execute has already been removed
                        LOGGER.error(
                                "fail to generate zombie task event context of Runner task:" + execId + ", detail:", e);
                        iterator.remove();
                    } catch (NaviException e) {
                        LOGGER.error("fail to kill zombie task:" + execId + ", detail:", e);
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.info("Clear zombie task thread interrupted.");
                    break;
                }
            }
        }
    }

    static class RunnerExecuteTasksThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (lastCheckTimeMillsLock) {
                    Iterator<Map.Entry<String, Long>> iterator = lastCheckTimeMills.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Long> entry = iterator.next();
                        String host = entry.getKey();
                        Long lastCheckTimeMills = entry.getValue();
                        long now = System.currentTimeMillis();
                        if (now - lastCheckTimeMills > (long) HeartBeatManager.getHealthExpireTime() * 1000 * 2) {
                            Set<Long> lostTasks = runnerExecuteTasks.get(host);
                            LOGGER.info("Runner:" + host + "is removed, " + lostTasks.size()
                                    + " execute tasks are marked lost");
                            RunnerTaskRecoveryManager.lostTasks.addAll(lostTasks);
                            unregisterHost(host);
                            iterator.remove();
                        }
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.info("Clear runner execute tasks thread interrupted.");
                    break;
                }
            }
        }
    }
}
