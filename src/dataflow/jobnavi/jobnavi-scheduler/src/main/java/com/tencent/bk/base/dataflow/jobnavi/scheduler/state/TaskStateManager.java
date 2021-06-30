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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.NodeBlackListManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.exception.NotFoundException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event.TaskExecuteEventBuffer;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

public class TaskStateManager {

    private static final Logger LOGGER = Logger.getLogger(TaskStateManager.class);
    private static final Object isReadyLock = new Object();
    private static final PriorityBlockingQueue<TaskEventInfo> events = new PriorityBlockingQueue<>();
    private static final Semaphore eventQueueNotEmpty = new Semaphore(0);
    private static final List<Thread> dispatchThreads = new ArrayList<>();
    private static AtomicLong eventId;
    private static AtomicLong executeId;
    private static boolean isReady;

    /**
     * dispatch event
     *
     * @param event
     * @return eventId
     * @throws NaviException
     */
    public static Long dispatchEvent(TaskEvent event) throws NaviException {
        TaskEventInfo taskEventInfo = newTaskEventInfo(event);
        LOGGER.info(String.format("dispatching task event:[%d][%s][schedule:%s][execute:%d] to task state machine",
                taskEventInfo.getId(),
                event.getEventName(),
                event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getExecuteInfo().getId()));
        MetaDataManager.getJobDao().addEvent(taskEventInfo);
        addEvent(taskEventInfo);
        return taskEventInfo.getId();
    }

    /**
     * dispatch event to runner
     *
     * @param event
     * @return event ID
     * @throws NaviException
     */
    public static Long dispatchEventToRunner(TaskEvent event) throws NaviException {
        TaskEventInfo taskEventInfo = newTaskEventInfo(event);
        LOGGER.info(String.format("dispatching task event:[%d][%s][schedule:%s][execute:%d] to Runner",
                taskEventInfo.getId(),
                event.getEventName(),
                event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getExecuteInfo().getId()));
        MetaDataManager.getJobDao().addEvent(taskEventInfo);
        sendEventToRunner(taskEventInfo);
        return taskEventInfo.getId();
    }


    /**
     * dispatch task event
     *
     * @param event
     * @return eventId
     * @throws NaviException
     */
    public static Long dispatchTaskEvent(TaskEvent event) throws NaviException {
        TaskEventInfo taskEventInfo = newTaskEventInfo(event);
        LOGGER.info(String.format("dispatching task event:[%d][%s][schedule:%s][execute:%d]",
                taskEventInfo.getId(),
                event.getEventName(),
                event.getContext().getTaskInfo().getScheduleId(),
                event.getContext().getExecuteInfo().getId()));
        MetaDataManager.getJobDao().addEvent(taskEventInfo);
        if (taskEventInfo.getTaskEvent() instanceof DefaultTaskEvent) {
            addEvent(taskEventInfo);
        } else {
            sendEventToRunner(taskEventInfo);
        }
        return taskEventInfo.getId();
    }

    public static void recoverTaskEventInfo(TaskEventInfo info) throws NaviException {
        if (info.getTaskEvent() instanceof DefaultTaskEvent) {
            addEvent(info);
        } else {
            sendEventToRunner(info);
        }
    }

    public static Integer queryNotProcessEventAmount() {
        return events.size();
    }

    /**
     * init
     *
     * @param conf JobNavi Scheduler Configuration
     */
    public static void init(Configuration conf) throws NaviException {
        synchronized (isReadyLock) {
            TaskStateManager.isReady = false;
        }
        eventId = new AtomicLong(MetaDataManager.getJobDao().getCurrentEventIdIndex());
        executeId = new AtomicLong(MetaDataManager.getJobDao().getCurrentExecuteIdIndex());
        int thread = conf.getInt(Constants.JOBNAVI_EVENT_DISPATCH_THREAD_NUM,
                Constants.JOBNAVI_EVENT_DISPATCH_THREAD_NUM_DEFAULT);
        for (int i = 0; i < thread; i++) {
            Thread dispatchThread = new Thread(new TaskEventDispatchThread(), "event-dispatch-" + i);
            dispatchThreads.add(dispatchThread);
            dispatchThread.start();
        }
        ExecuteStatusCache.init(conf);
    }

    public static void stop() {
        events.clear();

        for (Thread t : dispatchThreads) {
            t.interrupt();
        }
        dispatchThreads.clear();
        ExecuteStatusCache.destroy();
    }

    /**
     * recover task state manager
     *
     * @param conf
     * @throws NaviException
     */
    public static void recovery(Configuration conf) throws NaviException {
        LOGGER.info("Loading not processed events from DB");
        List<TaskEventInfo> taskEventInfos = MetaDataManager.getJobDao().getNotProcessTaskEvents();
        LOGGER.info(taskEventInfos.size() + " events reloaded from DB");
        for (TaskEventInfo taskEventInfo : taskEventInfos) {
            LOGGER.info(String.format("recovering task event:[%d][%s][schedule:%s][execute:%d]",
                    taskEventInfo.getId(),
                    taskEventInfo.getTaskEvent().getEventName(),
                    taskEventInfo.getTaskEvent().getContext().getTaskInfo().getScheduleId(),
                    taskEventInfo.getTaskEvent().getContext().getExecuteInfo().getId()));
            recoverTaskEventInfo(taskEventInfo);
        }
        ExecuteStatusCache.rebuild(MetaDataManager.getJobDao());
        synchronized (isReadyLock) {
            TaskStateManager.isReady = true;
            isReadyLock.notifyAll();
        }
    }

    /**
     * generate event context
     *
     * @param executeId
     * @return event context
     * @throws NaviException
     */
    public static EventContext generateEventContext(Long executeId) throws NotFoundException, NaviException {
        EventContext context = MetaDataManager.getJobDao().getEventContext(executeId);
        if (context == null || context.getTaskInfo() == null || context.getTaskInfo().getScheduleId() == null) {
            throw new NotFoundException("Cannot find execute [" + executeId + "] info.");
        }
        ScheduleInfo scheduleInfo = ScheduleManager.getSchedule()
                .getScheduleInfo(context.getTaskInfo().getScheduleId());
        if (scheduleInfo == null) {
            throw new NotFoundException("Cannot find schedule [" + context.getTaskInfo().getScheduleId() + "].");
        }
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        TaskType type = jobDao
                .queryTaskType(scheduleInfo.getTypeId(), scheduleInfo.getTypeTag(), scheduleInfo.getNodeLabel());
        if (type == null) {
            throw new NotFoundException("Cannot find task type [" + scheduleInfo.getTypeId() + "].");
        }

        context.getTaskInfo().setExtraInfo(scheduleInfo.getExtraInfo());
        context.getTaskInfo().setType(type);
        context.getTaskInfo().setDecommissionTimeout(scheduleInfo.getDecommissionTimeout());
        context.getTaskInfo().setNodeLabel(scheduleInfo.getNodeLabel());
        context.getTaskInfo().getRecoveryInfo().setRecoveryEnable(scheduleInfo.getRecoveryInfo().isEnable());
        context.getTaskInfo().getRecoveryInfo().setIntervalTime(scheduleInfo.getRecoveryInfo().getIntervalTime());
        context.getTaskInfo().getRecoveryInfo().setMaxRecoveryTimes(scheduleInfo.getRecoveryInfo().getRetryTimes());
        return context;
    }

    /**
     * send event to runner
     *
     * @param eventInfo
     * @return task event result
     * @throws NaviException
     */
    public static TaskEventResult sendEventToRunner(TaskEventInfo eventInfo) throws NaviException {
        TaskEventResult executeResult = new TaskEventResult();
        TaskEvent event = eventInfo.getTaskEvent();
        RunnerInfo runner = HeartBeatManager.getNodeInfos().get(event.getContext().getExecuteInfo().getHost());
        String host = (runner != null) ? runner.getHost() : null;
        Integer port = (runner != null) ? runner.getPort() : null;
        LOGGER.info("sending event:[" + event.getEventName() + "][" + event.getContext().getExecuteInfo().getId() + "]["
                + event.getContext().getTaskInfo().getScheduleId()
                + "] to Runner:[" + host + ":" + port + "]");
        if (port == null) {
            LOGGER.warn("Can not find host:" + host + ", set result false.");
            executeResult.setSuccess(false);
            executeResult.setProcessInfo("Can not find host:" + host);
            if ("running".equals(event.getEventName())) {
                LOGGER.info("running event error, change task " + eventInfo.getTaskEvent().getContext().getTaskInfo()
                        .getScheduleId() + " to preparing.");
                executeResult.addNextEvent(DefaultTaskEventBuilder.changeEvent(event, TaskStatus.preparing));
            }
            return executeResult;
        }
        LOGGER.debug("send event info: " + event.toJson());
        try {
            String resultJson = HttpUtils.post("http://" + host + ":" + port + "/event", eventInfo.toJson());
            executeResult.parseJson(JsonUtils.readMap(resultJson));
            NodeBlackListManager.healthy(host);
        } catch (Throwable e) {
            NodeBlackListManager.plusFail(host);
            throw new NaviException(e);
        }
        return executeResult;
    }

    /**
     * process event result
     *
     * @param eventId
     * @param result
     * @throws NaviException
     */
    public static void processEventResult(Long eventId, TaskEventResult result) throws NaviException {
        AbstractJobDao jobDao = MetaDataManager.getJobDao();
        jobDao.updateEventResult(eventId, result);
        for (TaskEvent nextEvent : result.getNextEvents()) {
            dispatchEvent(nextEvent);
        }
    }

    /**
     * generate new execute ID
     *
     * @return new execute ID
     */
    public static synchronized long newExecuteId() {
        return executeId.addAndGet(1);
    }

    private static TaskEventInfo newTaskEventInfo(TaskEvent event) {
        long currentEventId = eventId.addAndGet(1);
        TaskEventInfo taskEventInfo = new TaskEventInfo();
        taskEventInfo.setId(currentEventId);
        taskEventInfo.setTaskEvent(event);
        return taskEventInfo;
    }

    private static synchronized void addEvent(TaskEventInfo taskEventInfo) {
        events.add(taskEventInfo);
        LOGGER.info("event:" + taskEventInfo.getId() + " added to global queue, notify one process thread");
        eventQueueNotEmpty.release();
    }

    static class TaskEventDispatchThread implements Runnable {

        private static final Logger LOGGER = Logger.getLogger(TaskEventDispatchThread.class);
        //key:execute -> value:event name
        private static final Map<Long, String> eventProcessingLock = new ConcurrentHashMap<>();

        private static boolean tryLockExecute(long executeId, TaskEventInfo taskEventInfo) {
            if (eventProcessingLock.get(executeId) != null) {
                LOGGER.info(String.format(
                        "event:[%d][%s] was unable to lock execute:%d, which had been locked by event:%s",
                        taskEventInfo.getId(),
                        taskEventInfo.getTaskEvent().getEventName(),
                        executeId,
                        eventProcessingLock.get(executeId)));
                return false;
            } else {
                eventProcessingLock.put(executeId, taskEventInfo.getTaskEvent().getEventName());
                LOGGER.info(String.format(
                        "event:[%d][%s] locked execute:%d successfully",
                        taskEventInfo.getId(),
                        taskEventInfo.getTaskEvent().getEventName(),
                        executeId));
                return true;
            }
        }

        @Override
        public void run() {
            //waiting for task state manager to be ready before processing any ask event
            synchronized (isReadyLock) {
                while (!isReady) {
                    try {
                        LOGGER.info("waiting for task state manager to be ready");
                        isReadyLock.wait();
                    } catch (InterruptedException e) {
                        LOGGER.info("event process thread interrupted");
                        return;
                    }
                }
            }
            //continuously taking task events from global queue and process them
            while (!Thread.currentThread().isInterrupted()) {
                long startTime = System.currentTimeMillis();
                try {
                    //wait till global queue is not empty
                    eventQueueNotEmpty.acquire();
                } catch (InterruptedException e) {
                    LOGGER.info("waiting event queue not empty notification interrupted.");
                    break;
                }
                String scheduleId = null;
                Long executeId = null;
                TaskEventInfo eventInfo = null;
                TaskEvent event = null;
                //make taking event and locking execute atomic to prevent execute event out of order
                synchronized (eventProcessingLock) {
                    eventInfo = events.poll();
                    if (eventInfo == null) {
                        continue;
                    }
                    event = eventInfo.getTaskEvent();
                    LOGGER.info(
                            "acquired task event:[" + eventInfo.getId() + "][" + event.getEventName() + "], " + events
                                    .size() + " events queueing up");
                    scheduleId = eventInfo.getTaskEvent().getContext().getTaskInfo().getScheduleId();
                    executeId = eventInfo.getTaskEvent().getContext().getExecuteInfo().getId();
                    //events of the same execute must be process in sequence
                    if (!tryLockExecute(executeId, eventInfo)) {
                        LOGGER.info(String.format(
                                "move event:[%d][%s] of execute:%d to execute event buffer",
                                eventInfo.getId(),
                                eventInfo.getTaskEvent().getEventName(),
                                executeId));
                        TaskExecuteEventBuffer.waitTillExecuteUnlocked(executeId, eventInfo);
                        continue;
                    }
                }

                // process event
                try {
                    long scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
                    long dataTime = event.getContext().getTaskInfo().getDataTime();
                    String eventDigest = String
                            .format("[%d][%s][schedule:%s][execute:%d][schedule time:%s][data time:%s]",
                                    eventInfo.getId(),
                                    event.getEventName(),
                                    scheduleId,
                                    executeId,
                                    CronUtil.getPrettyTime(scheduleTime),
                                    CronUtil.getPrettyTime(dataTime));
                    LOGGER.info("processing task event:" + eventDigest);
                    LOGGER.debug(eventDigest + " check event cost:" + (System.currentTimeMillis() - startTime));
                    //query current status of this execute process the event with proper processor
                    AbstractJobDao jobDao = MetaDataManager.getJobDao();
                    TaskStatus status = jobDao.getExecuteStatus(executeId);
                    LOGGER.info("status of execute:[" + executeId + "][" + scheduleId + "] is " + status.toString());
                    TaskEventResult result = TaskStateMachineFactory.doTransition(status, eventInfo);
                    LOGGER.debug(eventDigest + " transition cost:" + (System.currentTimeMillis() - startTime));
                    if (!result.isProcessed()) {
                        LOGGER.info("Event not processed, detail:" + result.getProcessInfo());
                        continue;
                    }
                    LOGGER.info("finish processing event:" + eventDigest);
                    if (result.isSuccess() && event.getChangeStatus() != null) {
                        //change status of the execute
                        jobDao.updateExecuteStatus(executeId, event.getChangeStatus(),
                                event.getContext().getEventInfo());
                        ExecuteStatusCache.updateStatus(scheduleId, dataTime, event.getChangeStatus());
                        if ((status == TaskStatus.preparing || status == TaskStatus.recovering)
                                && event.getChangeStatus() != TaskStatus.preparing
                                && event.getChangeStatus() != TaskStatus.recovering) {
                            //remove execute status caches referred by current execute
                            ExecuteStatusCache.removeReference(scheduleId, dataTime);
                        }
                        LOGGER.info("status of execute:[" + executeId + "][" + scheduleId + "] changed to " + event
                                .getChangeStatus().toString());
                    }
                    LOGGER.debug(eventDigest + " process cost:" + (System.currentTimeMillis() - startTime));
                    processEventResult(eventInfo.getId(), result);
                } catch (Throwable e) {
                    LOGGER.error(String.format("failed to dispatch event:[%d][execute:%d][name:%s]",
                            eventInfo.getId(),
                            executeId,
                            event.getEventName()), e);
                } finally {
                    synchronized (eventProcessingLock) {
                        //unlock execute and recover buffered execute event
                        eventProcessingLock.remove(executeId);
                        TaskExecuteEventBuffer.notifyExecuteUnlocked(executeId);
                    }
                }
            }
        }
    }
}


