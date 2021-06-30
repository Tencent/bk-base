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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.RunningTaskCounterService;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;

public class RunningTaskEventBuffer {

    private static final Logger LOGGER = Logger.getLogger(RunningTaskEventBuffer.class);
    private static final Object scheduleEventBufferLock = new Object();
    //key:node label -> value:queue<process task event>
    private static Map<String, PriorityBlockingQueue<TaskEventInfo>> processTaskEventBuffer;
    //key:node label -> value:queue<thread task event>
    private static Map<String, PriorityBlockingQueue<TaskEventInfo>> threadTaskEventBuffer;
    //key:schedule ID -> value:queue<task event>
    private static Map<String, PriorityBlockingQueue<TaskEventInfo>> scheduleEventBuffer;
    //key:schedule ID -> value:set<recovering event ID>
    private static Map<String, Set<Long>> recoveringScheduleEvents;
    private static Thread eventRecoveryThread;
    private static Thread clearBufferThread;
    private static long lastCheckAndRecoverTime = 0; //in ms

    public static void init(Configuration conf) {
        processTaskEventBuffer = new ConcurrentHashMap<>();
        threadTaskEventBuffer = new ConcurrentHashMap<>();
        scheduleEventBuffer = new ConcurrentHashMap<>();
        recoveringScheduleEvents = new ConcurrentHashMap<>();
        eventRecoveryThread = new Thread(new EventRecoveryThread(), "running-event-recovery-thread");
        eventRecoveryThread.start();
        clearBufferThread = new Thread(new ClearBufferThread(), "clear-running-event-buffer-thread");
        clearBufferThread.start();
    }

    public static void destroy() {
        if (eventRecoveryThread != null) {
            eventRecoveryThread.interrupt();
        }
        if (clearBufferThread != null) {
            clearBufferThread.interrupt();
        }
    }

    /**
     * add running task event to corresponding buffer and wait till there exist available Runners
     *
     * @param taskEventInfo task event info
     */
    public static synchronized void waitForAvailableRunner(TaskEventInfo taskEventInfo) {
        TaskMode taskMode = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getType().getTaskMode();
        String nodeLabel = taskEventInfo.getTaskEvent().getContext().getTaskInfo().getNodeLabel();
        if (nodeLabel == null) {
            //use empty string as key for null label
            nodeLabel = "";
        }
        switch (taskMode) {
            case process:
                if (!processTaskEventBuffer.containsKey(nodeLabel)) {
                    processTaskEventBuffer.put(nodeLabel, new PriorityBlockingQueue<TaskEventInfo>());
                }
                //wait for available a Runner which has process task quota
                processTaskEventBuffer.get(nodeLabel).add(taskEventInfo);
                break;
            case thread:
                if (!threadTaskEventBuffer.containsKey(nodeLabel)) {
                    threadTaskEventBuffer.put(nodeLabel, new PriorityBlockingQueue<TaskEventInfo>());
                }
                //wait for available a Runner which has thread task quota
                threadTaskEventBuffer.get(nodeLabel).add(taskEventInfo);
                break;
            default:
                LOGGER.warn("unknown task mode:" + taskMode + ", discard event:" + taskEventInfo.getId());
        }
    }

    /**
     * add running task event to corresponding buffer and wait till there is enough quota for this schedule
     *
     * @param scheduleId schedule ID of given task
     * @param taskEventInfo task event info
     */
    public static void waitForScheduleQuota(String scheduleId, TaskEventInfo taskEventInfo) {
        synchronized (scheduleEventBufferLock) {
            if (!scheduleEventBuffer.containsKey(scheduleId)) {
                scheduleEventBuffer.put(scheduleId, new PriorityBlockingQueue<TaskEventInfo>());
                recoveringScheduleEvents.put(scheduleId, new HashSet<Long>());
            }
            //wait for available a schedule running task count of which has not reach the max limit
            scheduleEventBuffer.get(scheduleId).add(taskEventInfo);
        }
    }

    /**
     * check running quota and recover buffered running event
     */
    public static synchronized void checkAndRecover() {
        try {
            //check schedule max running task limit
            synchronized (scheduleEventBufferLock) {
                for (Map.Entry<String, PriorityBlockingQueue<TaskEventInfo>> entry : scheduleEventBuffer.entrySet()) {
                    String scheduleId = entry.getKey();
                    //some events had been moved to running task event buffer
                    int recoveringEventCount = recoveringScheduleEvents.get(scheduleId).size();
                    int quota = RunningTaskCounterService.getRunningTaskQuota(scheduleId) - recoveringEventCount;
                    if (quota > 0 && !entry.getValue().isEmpty()) {
                        LOGGER.info("recovering at most " + quota + " events from running event buffer:[schedule ID:"
                                + scheduleId + "]"
                                + "(" + entry.getValue().size() + " events queueing up)");
                        for (int i = 0; i < quota && !entry.getValue().isEmpty(); ++i) {
                            TaskEventInfo eventInfo = entry.getValue().poll();
                            if (eventInfo != null) {
                                //move schedule event to running task event buffer
                                waitForAvailableRunner(eventInfo);
                                recoveringScheduleEvents.get(scheduleId).add(eventInfo.getId());
                            }
                        }
                    }
                }
            }
            //check available Runner for process task
            for (Map.Entry<String, PriorityBlockingQueue<TaskEventInfo>> entry : processTaskEventBuffer.entrySet()) {
                String nodeLabel = entry.getKey().isEmpty() ? null : entry.getKey();
                List<RunnerInfo> availableRunners = HeartBeatManager.getAvailableRunner(nodeLabel, true);
                int quota = 0;
                for (RunnerInfo runner : availableRunners) {
                    //get process task quota from Runner info
                    quota += (runner.getMaxTaskNum() > runner.getTaskNum()) ? runner.getMaxTaskNum() - runner
                            .getTaskNum() : 0;
                }
                if (quota > 0 && !entry.getValue().isEmpty()) {
                    LOGGER.info("recovering at most " + quota + " events from process task running event buffer:[label:"
                            + nodeLabel + "]"
                            + "(" + entry.getValue().size() + " events queueing up)");
                    recoverTaskEventInfo(entry.getValue(), quota);
                }
            }
            //check available Runner for thread task
            for (Map.Entry<String, PriorityBlockingQueue<TaskEventInfo>> entry : threadTaskEventBuffer.entrySet()) {
                String nodeLabel = entry.getKey().isEmpty() ? null : entry.getKey();
                List<RunnerInfo> availableRunners = HeartBeatManager.getAvailableRunner(nodeLabel, false);
                int quota = 0;
                for (RunnerInfo runner : availableRunners) {
                    //get thread task quota from Runner info
                    quota += (runner.getMaxThreadTaskNum() > runner.getTaskThreadNum()) ? runner.getMaxThreadTaskNum()
                            - runner.getTaskThreadNum() : 0;
                }
                if (quota > 0 && !entry.getValue().isEmpty()) {
                    LOGGER.info("recovering at most " + quota + " events from thread task running event buffer:[label:"
                            + nodeLabel + "]"
                            + "(" + entry.getValue().size() + " events queueing up)");
                    recoverTaskEventInfo(entry.getValue(), quota);
                }
            }
            lastCheckAndRecoverTime = System.currentTimeMillis();
        } catch (Throwable e) {
            LOGGER.error("event recovery error.", e);
        }
    }

    /**
     * recover events from buffer queue to global event queue
     *
     * @param eventQueue buffer queue events from
     * @param quota maximum count of events to recover
     * @throws NaviException
     */
    private static void recoverTaskEventInfo(PriorityBlockingQueue<TaskEventInfo> eventQueue, int quota)
            throws NaviException {
        for (int i = 0; i < quota && !eventQueue.isEmpty(); ++i) {
            TaskEventInfo eventInfo = eventQueue.poll();
            if (eventInfo != null) {
                LOGGER.info(String.format(
                        "recovering running event:%d, execute ID:%d, rank:%f",
                        eventInfo.getId(),
                        eventInfo.getTaskEvent().getContext().getExecuteInfo().getId(),
                        eventInfo.getTaskEvent().getContext().getExecuteInfo().getRank()));
                TaskStateManager.recoverTaskEventInfo(eventInfo);
                String scheduleId = eventInfo.getTaskEvent().getContext().getTaskInfo().getScheduleId();
                if (recoveringScheduleEvents.containsKey(scheduleId)) {
                    recoveringScheduleEvents.get(scheduleId).remove(eventInfo.getId());
                }
            }
        }
    }

    static class EventRecoveryThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    //limit check frequency for running task quota is not always updated in time
                    if (System.currentTimeMillis() - lastCheckAndRecoverTime > 10000) {
                        checkAndRecover();
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("event recovery thread interrupt.");
                    break;
                }
            }
        }

    }

    static class ClearBufferThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    while (!ScheduleManager.isReady()) {
                        Thread.sleep(1000);
                    }
                    //remove not exist schedule event buffer
                    synchronized (scheduleEventBufferLock) {
                        Iterator<Map.Entry<String, PriorityBlockingQueue<TaskEventInfo>>> iterator = scheduleEventBuffer
                                .entrySet().iterator();
                        while (iterator.hasNext()) {
                            String scheduleId = iterator.next().getKey();
                            if (RunningTaskCounterService.getMaxRunningTask(scheduleId) == null) {
                                if (scheduleEventBuffer.get(scheduleId).isEmpty() && recoveringScheduleEvents
                                        .get(scheduleId).isEmpty()) {
                                    LOGGER.info("remove event buffer for schedule:" + scheduleId);
                                    iterator.remove();
                                    recoveringScheduleEvents.remove(scheduleId);
                                }
                            }
                        }
                    }
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOGGER.info("clear running event buffer thread interrupt.");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("clear running event buffer error.", e);
                }
            }
        }
    }
}
