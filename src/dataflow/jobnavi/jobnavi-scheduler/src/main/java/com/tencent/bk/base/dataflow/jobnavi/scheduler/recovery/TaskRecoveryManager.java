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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class TaskRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(TaskRecoveryManager.class);
    private static final String split = "@_@";
    private static Map<String, TaskInfo> recoveryTasks;
    private static Map<String, Double> recoveryTaskRanks;
    private static Map<String, Long> recoveryRunningTime;
    private static Thread recoveryTaskThread;

    /**
     * start task recovery manager
     * @param conf
     */
    public static synchronized void start(Configuration conf) throws NaviException {
        recoveryTasks = new ConcurrentHashMap<>();
        recoveryTaskRanks = new ConcurrentHashMap<>();
        recoveryRunningTime = new ConcurrentHashMap<>();
        recoveryTaskThread = new Thread(new RecoveryTaskThread(), "task-recovery-thread");
        recoveryTaskThread.start();
    }


    public static void stop() {
        if (recoveryTasks != null) {
            recoveryTasks.clear();
        }

        if (recoveryRunningTime != null) {
            recoveryRunningTime.clear();
        }

        if (recoveryTaskThread != null) {
            recoveryTaskThread.interrupt();
        }
    }

    /**
     * recover task recovery manager
     *
     * @param conf
     * @throws NaviException
     */
    public static void recovery(Configuration conf) throws NaviException {
        List<TaskInfo> infos = MetaDataManager.getJobDao().getNotRecoveryTask();
        for (TaskInfo info : infos) {
            recoveryTasks.put(info.getScheduleId() + split + info.getDataTime(), info);
            recoveryRunningTime
                    .put(info.getScheduleId() + split + info.getDataTime(), info.getRecoveryInfo().getCreatedAt());
            recoveryTaskRanks.put(info.getScheduleId() + split + info.getDataTime(), info.getRecoveryInfo().getRank());
        }
    }

    /**
     * add task to recover buffer
     *
     * @param taskinfo
     * @return ture if succeeded
     */
    public static boolean addRecoveryTaskIfNecessary(TaskInfo taskinfo, double rank) throws NaviException {
        TaskRecoveryInfo recoveryInfo = taskinfo.getRecoveryInfo();
        if (recoveryInfo != null && recoveryInfo.isRecoveryEnable() && recoveryInfo.getRecoveryTimes() < recoveryInfo
                .getMaxRecoveryTimes()) {
            int recoveryTime = recoveryInfo.getRecoveryTimes();
            recoveryInfo.setRecoveryTimes(recoveryTime + 1);
            recoveryInfo.setRank(rank);
            MetaDataManager.getJobDao().addRecoveryExecute(-1,
                    taskinfo); //-1 is a reserved exec_id, representing a not-generated recovery job instance
            LOGGER.info(
                    "add recovery, schedule:" + taskinfo.getScheduleId() + ", data time: " + taskinfo.getDataTime());
            recoveryTaskRanks.put(taskinfo.getScheduleId() + split + taskinfo.getDataTime(), rank);
            recoveryRunningTime
                    .put(taskinfo.getScheduleId() + split + taskinfo.getDataTime(), System.currentTimeMillis());
            recoveryTasks.put(taskinfo.getScheduleId() + split + taskinfo.getDataTime(), taskinfo);
            return true;
        }
        return false;
    }

    static class RecoveryTaskThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Iterator<Map.Entry<String, TaskInfo>> iterator = recoveryTasks.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, TaskInfo> entry = iterator.next();
                    String key = entry.getKey();
                    TaskInfo taskInfo = entry.getValue();
                    long runningTime = recoveryRunningTime.get(key);
                    long currentTime = System.currentTimeMillis();
                    double rank = recoveryTaskRanks.get(key) != null ? recoveryTaskRanks.get(key) : 0;
                    try {
                        Long interval = CronUtil.parsePeriodStringToMills(taskInfo.getRecoveryInfo().getIntervalTime());
                        if (runningTime + interval < currentTime) {
                            LOGGER.info("recovery task: " + key);
                            long executeId = TaskStateManager.newExecuteId();
                            MetaDataManager.getJobDao().updateRecoveryExecute(executeId, taskInfo);
                            TaskStateManager.dispatchEvent(DefaultTaskEventBuilder
                                    .buildEvent(TaskStatus.preparing.toString(), executeId, null, taskInfo, rank));
                            iterator.remove();
                            recoveryRunningTime.remove(key);
                            recoveryTaskRanks.remove(key);
                        }
                    } catch (NaviException e) {
                        LOGGER.error("Recovery Error.", e);
                    }
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOGGER.info("Task recovering thread interrupted.");
                    break;
                }
            }
        }
    }
}
