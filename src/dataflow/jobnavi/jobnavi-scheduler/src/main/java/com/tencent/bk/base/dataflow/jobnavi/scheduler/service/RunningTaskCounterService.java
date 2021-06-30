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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.service;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class RunningTaskCounterService implements Service {

    private static final Logger LOGGER = Logger.getLogger(RunningTaskCounterService.class);

    private static final Map<String, Integer> runningTaskNumbers = new ConcurrentHashMap<>();
    private static int globalMaxRunningTaskCount;
    private static Thread clearCounterThread;

    /**
     * get max running task count of given schedule
     *
     * @param scheduleId schedule ID
     * @return max running task count, null if schedule not exist
     */
    public static Integer getMaxRunningTask(String scheduleId) {
        ScheduleInfo scheduleInfo = ScheduleManager.getSchedule().getScheduleInfo(scheduleId);
        if (scheduleInfo == null) {
            return null;
        }
        int maxRunningTask = scheduleInfo.getMaxRunningTask();
        if (maxRunningTask == -1) {
            maxRunningTask = Integer.MAX_VALUE;
        }
        return Math.min(maxRunningTask, globalMaxRunningTaskCount);
    }

    /**
     * check true if schedule under max running task count limit
     *
     * @param scheduleId
     * @return true if schedule under max running task count limit
     */
    public static boolean validateRunningTaskCountLimit(String scheduleId) {
        Integer maxRunningTask = getMaxRunningTask(scheduleId);
        if (maxRunningTask == null) {
            return true;
        }
        synchronized (runningTaskNumbers) {
            Integer runningTaskNumber = runningTaskNumbers.get(scheduleId);
            if (runningTaskNumber == null) {
                runningTaskNumber = 0;
            }
            if (runningTaskNumber >= maxRunningTask) {
                LOGGER.info("[" + scheduleId + "] running task num:" + runningTaskNumber + " and max running task is "
                        + maxRunningTask);
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * increase running count of given schedule
     *
     * @param scheduleId
     */
    public static void plusRunningTask(String scheduleId) {
        Integer maxRunningTask = getMaxRunningTask(scheduleId);
        if (maxRunningTask == null) {
            return;
        }
        synchronized (runningTaskNumbers) {
            Integer runningTaskNumber = runningTaskNumbers.get(scheduleId);
            if (runningTaskNumber == null) {
                runningTaskNumber = 0;
            }
            if (runningTaskNumber < maxRunningTask) {
                LOGGER.info("plus " + scheduleId + " running task num:" + (runningTaskNumber + 1)
                        + " and max running task is:" + maxRunningTask);
                runningTaskNumbers.put(scheduleId, runningTaskNumber + 1);
            }
        }
    }

    /**
     * decrease running count of given schedule
     *
     * @param scheduleId
     */
    public static void minusRunningTask(String scheduleId) {
        Integer maxRunningTask = getMaxRunningTask(scheduleId);
        if (maxRunningTask == null) {
            return;
        }
        maxRunningTask = Math.min(maxRunningTask, globalMaxRunningTaskCount);
        synchronized (runningTaskNumbers) {
            Integer runningTaskNumber = runningTaskNumbers.get(scheduleId);
            if (runningTaskNumber != null && runningTaskNumber >= 1) {
                runningTaskNumbers.put(scheduleId, runningTaskNumber - 1);
                LOGGER.info("minus " + scheduleId + " running task num:" + (runningTaskNumber - 1)
                        + " and max running task is:" + maxRunningTask);
            }
        }
    }

    /**
     * get running task quota.
     *
     * @param scheduleId schedule ID
     * @return running task quota.
     */
    public static Integer getRunningTaskQuota(String scheduleId) {
        Integer maxRunningTask = getMaxRunningTask(scheduleId);
        if (maxRunningTask == null) {
            maxRunningTask = Integer.MAX_VALUE;
        }
        synchronized (runningTaskNumbers) {
            int runningTaskCount = (runningTaskNumbers.get(scheduleId) != null) ? runningTaskNumbers.get(scheduleId)
                    : 0;
            return (maxRunningTask > runningTaskCount) ? maxRunningTask - runningTaskCount : 0;
        }
    }

    @Override
    public String getServiceName() {
        return RunningTaskCounterService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        globalMaxRunningTaskCount = conf.getInt(Constants.JOBNAVI_SCHEDULER_TASK_RUNNING_COUNT_MAX,
                Constants.JOBNAVI_SCHEDULER_TASK_RUNNING_COUNT_MAX_DEFAULT);
        if (globalMaxRunningTaskCount < 0) {
            globalMaxRunningTaskCount = Integer.MAX_VALUE;
        }
        synchronized (runningTaskNumbers) {
            runningTaskNumbers.putAll(MetaDataManager.getJobDao().queryRunningExecuteNumbers());
            for (Map.Entry<String, Integer> entrySet : runningTaskNumbers.entrySet()) {
                LOGGER.info("running task num:" + entrySet.getKey() + " : " + entrySet.getValue());
            }
        }
        clearCounterThread = new Thread(new ClearCounterThread(), "clear-schedule-running-counter-thread");
        clearCounterThread.start();
    }

    @Override
    public void stop(Configuration conf) throws NaviException {
        if (clearCounterThread != null) {
            clearCounterThread.interrupt();
        }
    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    static class ClearCounterThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    while (!ScheduleManager.isReady()) {
                        Thread.sleep(1000);
                    }
                    //remove not exist schedule running task counter
                    synchronized (runningTaskNumbers) {
                        Iterator<Map.Entry<String, Integer>> iterator = runningTaskNumbers.entrySet().iterator();
                        while (iterator.hasNext()) {
                            String scheduleId = iterator.next().getKey();
                            if (RunningTaskCounterService.getMaxRunningTask(scheduleId) == null) {
                                LOGGER.info("remove running task counter for schedule:" + scheduleId);
                                iterator.remove();
                            }
                        }
                    }
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOGGER.info("clear schedule running task counter thread interrupt.");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("clear schedule running task counter error.", e);
                }
            }
        }
    }
}
