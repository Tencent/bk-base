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

package com.tencent.bk.base.dataflow.jobnavi.runner.task.thread;

import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAggregationService;
import com.tencent.bk.base.dataflow.jobnavi.runner.heartbeat.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.api.JobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.conf.TaskInfoConstants;
import com.tencent.bk.base.dataflow.jobnavi.exception.JobNaviNoDataException;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.logging.ThreadLoggingFactory;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.SchedulerManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.callback.AbstractTaskEventThreadCallback;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.CustomTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.classloader.ClassLoaderBuilder;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class TaskThreadManager {

    private static final Logger logger = Logger.getLogger(TaskThreadManager.class);
    private static final Map<String, URLClassLoader> classLoaderMap = new ConcurrentHashMap<>();
    private static final Map<Long, Thread> executeThreads = new ConcurrentHashMap<>();
    private static final Map<Long, TaskThread> executeTaskThreads = new ConcurrentHashMap<>();
    private static final Map<Long, TaskInfo> executeTaskInfos = new ConcurrentHashMap<>();
    private static Configuration conf;

    public static void init(Configuration config) {
        conf = config;
    }

    /**
     * run thread task
     *
     * @param taskEventInfo
     * @throws NaviException
     */
    public static void run(TaskEventInfo taskEventInfo) throws NaviException {
        synchronized (TaskThreadManager.class) {
            int maxTaskNum = conf.getInt(Constants.JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM,
                    Constants.JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM_DEFAULT);
            if (executeThreads.size() >= maxTaskNum) {
                throw new NaviException(TaskInfoConstants.RUNNING_TASK_FULL_INFO);
            } else if (HeartBeatManager.isFullLoad()) {
                logger.warn("Runner is full load");
                throw new NaviException(TaskInfoConstants.RUNNER_UNAVAILABLE_INFO);
            }
            TaskEvent event = taskEventInfo.getTaskEvent();
            TaskInfo info = event.getContext().getTaskInfo();
            Long executeId = event.getContext().getExecuteInfo().getId();
            if (executeThreads.get(executeId) != null) {
                throw new NaviException("Execute " + executeId + " is running.");
            }
            TaskThread taskThread = new TaskThread(taskEventInfo);
            Thread thread = new Thread(taskThread,
                    executeId + "_" + info.getScheduleId() + "_" + info.getScheduleTime());
            thread.start();
            registerTask(executeId, thread, taskThread);
        }
    }

    /**
     * send event to task
     *
     * @param taskEventInfo
     * @throws NaviException
     */
    public static void sendEvent(final TaskEventInfo taskEventInfo) throws NaviException {
        final TaskEvent event = taskEventInfo.getTaskEvent();
        final long executeId = event.getContext().getExecuteInfo().getId();
        final TaskThread taskThread = executeTaskThreads.get(executeId);
        if (taskThread == null) {
            TaskEventResult result = new TaskEventResult();
            String processInfo = "can not find execute " + executeId + " , send event failed.";
            result.setSuccess(false);
            result.setProcessInfo(processInfo);
            sendEventResult(result, taskEventInfo);
            throw new NaviException(processInfo);
        }
        logger.info("start send event thread, event: " + event.toJson());
        try {
            TaskInfo info = event.getContext().getTaskInfo();
            final AbstractTaskEventThreadCallback callback = TaskEventThreadCallBackFactory
                    .generateProcessCallBack(conf, event);
            Thread eventThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    TaskEventResult result;
                    try {
                        ThreadLoggingFactory.initLogger(executeId);
                        result = taskThread.processEvent(taskEventInfo);
                        if (callback != null) {
                            callback.complete(result);
                        }
                    } catch (Exception e) {
                        if (callback != null) {
                            callback.error(e);
                        }
                        result = new TaskEventResult();
                        result.setSuccess(false);
                        result.setProcessInfo(e.getMessage());
                        logger.error("process event error.", e);
                    }
                    sendEventResult(result, taskEventInfo);
                }
            }, info.getScheduleId() + "_" + info.getScheduleTime() + "_" + event.getEventName() + "_" + System
                    .currentTimeMillis());
            eventThread.start();
        } catch (Throwable e) {
            logger.error("send event error.", e);
            throw new NaviException(e);
        }
    }

    public static void sendEvent(TaskEvent event) throws NaviException {
        TaskEventInfo taskEventInfo = new TaskEventInfo();
        taskEventInfo.setTaskEvent(event);
        sendEvent(taskEventInfo);
    }

    private static void sendEventResult(TaskEventResult result, TaskEventInfo originalTaskEventInfo) {
        final Long eventId = originalTaskEventInfo.getId();
        final TaskEvent event = originalTaskEventInfo.getTaskEvent();
        if (event instanceof CustomTaskEvent) {
            try {
                SchedulerManager.sendEventResult(eventId, result);
            } catch (Exception e) {
                logger.error("send event result error.", e);
            }
        }
    }

    private static void registerTask(Long executeId, Thread thread, TaskThread taskThread) {
        executeThreads.put(executeId, thread);
        executeTaskThreads.put(executeId, taskThread);
        executeTaskInfos.put(executeId, taskThread.getTaskInfo());
    }

    /**
     * unregister task
     *
     * @param executeId
     */
    public static void unregisterTask(Long executeId) {
        logger.info("unregister task " + executeId);
        Thread thread = executeThreads.get(executeId);
        if (thread != null) {
            TaskThread task = executeTaskThreads.get(executeId);
            task.task = null;
            task.mainClass = null;

            try {
                thread.interrupt();
            } catch (Throwable e) {
                logger.error("interrupt task thread error.", e);
            }
            cleanRegister(executeId);
        }
    }

    private static void cleanRegister(Long executeId) {
        executeThreads.remove(executeId);
        executeTaskThreads.remove(executeId);
        executeTaskInfos.remove(executeId);
    }

    public static Map<Long, TaskInfo> getExecuteTasks() {
        return new HashMap<>(executeTaskInfos);
    }

    public static int getTaskNum() {
        return executeTaskInfos.size();
    }

    private static synchronized URLClassLoader buildClassLoader(TaskType type) throws Exception {
        URLClassLoader classLoader = classLoaderMap.get(type.getName());
        if (classLoader == null) {
            String path = System.getProperty("JOBNAVI_HOME");
            List<String> jarList = new ArrayList<>();
            List<String> confList = new ArrayList<>();

            String typePath = path + "/adaptor/" + type.getName() + "/tags/" + type.getTag();
            jarList.add(typePath);

            if (StringUtils.isNotEmpty(type.getEnv())) {
                String envPath = path + "/env/" + type.getEnv() + "/lib";
                jarList.add(envPath);

                String confPath = path + "/env/" + type.getEnv() + "/conf/";
                confList.add(confPath);
            }

            if (StringUtils.isNotEmpty(type.getSysEnv())) {
                String sysEnvPath = path + "/sys_env/" + type.getSysEnv() + "/lib";
                jarList.add(sysEnvPath);

                String sysConfPath = path + "/sys_env/" + type.getSysEnv() + "/conf/";
                confList.add(sysConfPath);
            }
            classLoader = ClassLoaderBuilder.build(jarList, confList);
            classLoaderMap.put(type.getName(), classLoader);
        }
        return classLoader;
    }

    static class TaskThread implements Runnable {

        TaskEventInfo runningTaskEventInfo;
        JobNaviTask task;
        Class<?> mainClass;
        URLClassLoader classLoader;

        TaskThread(TaskEventInfo runningTaskEventInfo) {
            this.runningTaskEventInfo = runningTaskEventInfo;
        }

        TaskEventResult processEvent(final TaskEventInfo taskEventInfo) throws NaviException {
            TaskEvent event = taskEventInfo.getTaskEvent();
            if (task == null) {
                throw new NaviException("task may not start, please wait and retry it later...");
            }
            Thread.currentThread().setContextClassLoader(classLoader);
            EventListener listener = task.getEventListener(event.getEventName());
            if (listener == null) {
                TaskEventResult result = new TaskEventResult();
                result.setSuccess(false);
                result.setProcessInfo("Can not find event [" + event.getEventName() + "] listener.");
                return result;
            }
            return listener.doEvent(taskEventInfo.getTaskEvent());
        }

        private TaskInfo getTaskInfo() {
            return runningTaskEventInfo.getTaskEvent().getContext().getTaskInfo();
        }

        private void cleanObj() {
            logger.info("clean object");
            task = null;
            mainClass = null;
        }

        @Override
        public void run() {
            TaskEvent runningEvent = runningTaskEventInfo.getTaskEvent();
            long executeId = runningEvent.getContext().getExecuteInfo().getId();
            TaskType type = runningEvent.getContext().getTaskInfo().getType();
            try {
                classLoader = buildClassLoader(type);
                logger.info("register classloader is: " + classLoader.toString());
                Thread.currentThread().setContextClassLoader(classLoader);
                mainClass = Thread.currentThread().getContextClassLoader().loadClass(type.getMain());
                task = (JobNaviTask) mainClass.newInstance();
                task.run(runningEvent);
                DefaultTaskEvent finishedEvent = DefaultTaskEventBuilder
                        .changeEvent(runningEvent, "finished", TaskStatus.finished);
                SchedulerManager.sendEvent(finishedEvent);
                LogAggregationService.callAggregateLog(executeId);
                cleanObj();
                cleanRegister(executeId);
            } catch (Throwable e) {
                if (e instanceof ThreadDeath) {
                    cleanObj();
                    logger.error("execute " + executeId + " thread dead.");
                } else {
                    logger.error("running thread task error.", e);
                    cleanObj();
                    cleanRegister(executeId);
                    TaskStatus status = TaskStatus.failed;
                    if (e instanceof JobNaviNoDataException) {
                        status = ((JobNaviNoDataException) e).taskStatus();
                        logger.info("change task status to " + status.name());
                    }
                    DefaultTaskEvent failedEvent = DefaultTaskEventBuilder
                            .changeEvent(runningEvent, status.name(), status);
                    failedEvent.getContext().setEventInfo(e.getMessage());
                    try {
                        SchedulerManager.sendEvent(failedEvent);
                    } catch (Exception e1) {
                        logger.error("send failed event error.", e1);
                    }
                }
                LogAggregationService.callAggregateLog(executeId);
            }
        }
    }
}
