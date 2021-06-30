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

package com.tencent.bk.base.dataflow.jobnavi.runner.task.process;

import com.tencent.bk.base.dataflow.jobnavi.runner.heartbeat.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.conf.TaskInfoConstants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventConvertor;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.state.Language;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

public class TaskProcessManager {

    private static final Logger LOGGER = Logger.getLogger(TaskProcessManager.class);

    //running task cache,key is exec_id, value is task EventContext
    private static final Map<Long, TaskInfo> executeTasks = new ConcurrentHashMap<>();
    //running task cache,key is exec_id, value is task EventContext
    private static final Map<Long, TaskInfo> launchingTasks = new ConcurrentHashMap<>();
    //running task cache,key is exec_id, value is task start port
    private static final Map<Long, Integer> executeTaskPorts = new ConcurrentHashMap<>();
    //running task cache,key is exec_id, value is task heart beat time mills
    private static final Map<Long, Long> executeTaskHeartBeats = new ConcurrentHashMap<>();

    private static Configuration conf;

    public static void init(Configuration config) {
        conf = config;
        startTaskMonitor();
    }

    /**
     * run task
     *
     * @param taskEventInfo
     * @throws NaviException
     */
    public static void run(TaskEventInfo taskEventInfo) throws NaviException {
        TaskEvent event = taskEventInfo.getTaskEvent();
        Long execId = event.getContext().getExecuteInfo().getId();
        String type = event.getContext().getTaskInfo().getType().getName();
        String tag = event.getContext().getTaskInfo().getType().getTag();
        type += "/tags/" + tag;
        Language language = event.getContext().getTaskInfo().getType().getLanguage();
        if (executeTasks.containsKey(execId)) {
            LOGGER.warn("execute Id [" + execId + "] is Running.");
            return;
        }

        int runnerRPCPort = conf.getInt(Constants.JOBNAVI_RUNNER_RPC_PORT);
        synchronized (TaskProcessManager.class) {
            int maxTaskNum = conf
                    .getInt(Constants.JOBNAVI_RUNNER_TASK_MAX_NUM, Constants.JOBNAVI_RUNNER_TASK_MAX_NUM_DEFAULT);
            if (runnerRPCPort == 0) {
                LOGGER.warn("RPC service not started");
                throw new NaviException(TaskInfoConstants.RUNNER_UNAVAILABLE_INFO);
            } else if (executeTasks.size() + launchingTasks.size() >= maxTaskNum) {
                throw new NaviException(TaskInfoConstants.RUNNING_TASK_FULL_INFO);
            } else if (HeartBeatManager.isFullLoad()) {
                LOGGER.warn("Runner is full load");
                throw new NaviException(TaskInfoConstants.RUNNER_UNAVAILABLE_INFO);
            }
            //split launching process into launching and launched
            launchingTasks.put(event.getContext().getExecuteInfo().getId(), event.getContext().getTaskInfo());
        }

        String logs = conf.getString(com.tencent.bk.base.dataflow.jobnavi.conf.Constants.JOBNAVI_RUNNER_TASK_LOG_PATH,
                com.tencent.bk.base.dataflow.jobnavi.conf.Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
        int maxRetryTimes = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_START_MAX_RETRY,
                Constants.JOBNAVI_RUNNER_TASK_START_MAX_RETRY_DEFAULT);

        String path = System.getProperty("JOBNAVI_HOME");
        //todo adapt multi language.
        String shellFileName = "";
        switch (language) {
            case python:
                shellFileName = "task_start_python.sh";
                break;
            default:
                shellFileName = "task_start_java.sh";
        }

        String sysEnv = event.getContext().getTaskInfo().getType().getSysEnv();
        String env = event.getContext().getTaskInfo().getType().getEnv();
        String startCommand = path + "/bin/" + shellFileName + " " + execId + " " + type + "  " + sysEnv + " " + env
                + " " + logs + " " + runnerRPCPort;

        int retryTimes = 0;
        while (true) {
            try {
                long startTimeMills = System.currentTimeMillis();
                LOGGER.info("Launch task, command is " + startCommand);
                ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", startCommand);
                Process process = builder.start();
                int status = process.waitFor();
                if (status != 0) {
                    throw new NaviException("Process result is not zero.");
                } else {
                    sendRunningEvent(taskEventInfo, startTimeMills);
                    //register task after it is launched to avoid unnecessary zombie task detect
                    registerTask(execId);
                    break;
                }
            } catch (Exception e) {
                if (retryTimes == maxRetryTimes - 1) {
                    unregisterTask(execId);
                    launchingTasks.remove(execId);
                    throw new NaviException("Task Running failed.", e);
                }
                retryTimes++;
                LOGGER.warn("Running task error. retry " + retryTimes + " times...", e);
            }
        }
    }

    /**
     * send event to task launcher
     *
     * @param taskEventInfo
     * @param timeout event call back timeout.
     * @throws NaviException
     */
    public static void sendEvent(TaskEventInfo taskEventInfo, Long timeout) throws NaviException {
        TaskEvent event = taskEventInfo.getTaskEvent();
        Long executeId = event.getContext().getExecuteInfo().getId();
        Integer taskRPCPort = executeTaskPorts.get(executeId);
        if (taskRPCPort == null) {
            throw new NaviException("Send event error, cannot find execute " + executeId);
        }
        try {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            TNonblockingTransport transport = new TNonblockingSocket("127.0.0.1", taskRPCPort, 5);
            TProtocolFactory protocol = new TCompactProtocol.Factory();
            TaskEventService.AsyncClient asyncClient = new TaskEventService.AsyncClient(protocol, clientManager,
                    transport);
            if (timeout != null) {
                asyncClient.setTimeout(timeout);
            }
            asyncClient.processEvent(TaskEventConvertor.toThriftEvent(event), TaskEventProcessCallBackFactory
                    .generateProcessCallBack(conf, taskEventInfo, transport, clientManager));
        } catch (Exception e) {
            LOGGER.error("send event error.", e);
            throw new NaviException(e);
        }
    }

    public static void sendEvent(TaskEvent event, Long timeout) throws NaviException {
        TaskEventInfo taskEventInfo = new TaskEventInfo();
        taskEventInfo.setTaskEvent(event);
        sendEvent(taskEventInfo, timeout);
    }

    /**
     * send event to task launcher
     *
     * @param taskEventInfo
     * @throws NaviException
     */
    public static void sendEvent(TaskEventInfo taskEventInfo) throws NaviException {
        sendEvent(taskEventInfo, null);
    }

    /**
     * register task listen port
     *
     * @param executeId
     * @param port
     */
    public static synchronized boolean registerTaskPort(Long executeId, Integer port) {
        //register task launcher port before sending running task event
        if (launchingTasks.containsKey(executeId)) {
            if (!executeTaskPorts.containsKey(executeId)) {
                LOGGER.info(String.format("register task port:[%d] for execute:%d", port, executeId));
                executeTaskPorts.put(executeId, port);
                executeTaskHeartBeats.put(executeId, System.currentTimeMillis());
                return true;
            } else {
                LOGGER.warn("port for task:" + executeId + " has been registered");
                return false;
            }
        } else {
            LOGGER.warn("launching task:" + executeId + " not exist, ignore task port register request");
            return false;
        }
    }

    /**
     * register running task
     *
     * @param executeId
     */
    public static synchronized void registerTask(Long executeId) {
        //register running task on Runner
        if (launchingTasks.containsKey(executeId)) {
            LOGGER.info("register task " + executeId);
            executeTasks.put(executeId, launchingTasks.get(executeId));
            launchingTasks.remove(executeId);
        } else {
            LOGGER.warn("launching task:" + executeId + " not exist, ignore task register request");
        }
    }

    /**
     * unregister running task
     *
     * @param executeId
     */
    public static void unregisterTask(Long executeId) {
        //unregister running task from Runner when task is complete
        LOGGER.info("unregister task " + executeId);
        launchingTasks.remove(executeId);
        executeTaskPorts.remove(executeId);
        executeTasks.remove(executeId);
        executeTaskHeartBeats.remove(executeId);
        clearProject(executeId);
    }

    private static void clearProject(Long executeId) {
        String path = System.getProperty("JOBNAVI_HOME");
        String projectPath = path + "/projects/" + executeId;
        try {
            if (!deleteDir(new File(projectPath))) {
                LOGGER.error("clear project dir " + projectPath + " failed.");
            } else {
                LOGGER.info("clear project dir " + projectPath + ".");
            }
        } catch (Exception e) {
            LOGGER.error("clear project dir " + projectPath + " failed.", e);
        }
    }

    private static boolean deleteDir(File dir) {
        if (dir != null && dir.exists()) {
            String[] childDirList = dir.list();
            if (dir.isDirectory() && childDirList != null) {
                for (String child : childDirList) {
                    if (!deleteDir(new File(dir, child))) {
                        return false;
                    }
                }
            }
            return dir.delete();
        }
        return true;
    }

    /**
     * receive heartbeat from task
     *
     * @param executeId
     * @return true if task exists
     */
    public static boolean heartbeat(Long executeId) {
        if (!launchingTasks.containsKey(executeId) && !executeTasks.containsKey(executeId)) {
            LOGGER.warn("task:" + executeId + " not exist");
            return false;
        }
        executeTaskHeartBeats.put(executeId, System.currentTimeMillis());
        return true;
    }

    private static void startTaskMonitor() {
        Thread thread = new Thread(new TaskMonitor(), "TaskMonitor");
        thread.start();
    }

    private static void sendRunningEvent(TaskEventInfo eventInfo, Long startTimeMills) throws NaviException {
        int taskTimeout = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_START_TIMEOUT_SECOND,
                Constants.JOBNAVI_RUNNER_TASK_START_TIMEOUT_SECOND_DEFAULT) * 1000;
        TaskEvent event = eventInfo.getTaskEvent();
        Long execId = event.getContext().getExecuteInfo().getId();
        try {
            while (System.currentTimeMillis() < (taskTimeout + startTimeMills)) {
                if (executeTaskPorts.get(execId) != null) {
                    LOGGER.info("Task Execute " + execId + " started, Running...");
                    sendEvent(eventInfo);
                    return;
                } else {
                    LOGGER.info("Task Execute " + execId + " may not start, WAITING...");
                    Thread.sleep(1000);
                }
            }
            throw new NaviException("Task " + execId + "  Launch timeout after " + taskTimeout);
        } catch (InterruptedException e) {
            throw new NaviException(e);
        }
    }

    public static Map<Long, TaskInfo> getExecuteTasks() {
        Map<Long, TaskInfo> taskInfoMap = new HashMap<>(executeTasks);
        return taskInfoMap;
    }

    public static int getTaskNum() {
        return executeTasks.size() + launchingTasks.size();
    }

    static class TaskMonitor implements Runnable {

        @Override
        public void run() {
            int expireTimeout = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_EXPIRE_TIMEOUT_SECOND,
                    Constants.JOBNAVI_RUNNER_TASK_EXPIRE_TIMEOUT_SECOND_DEFAULT) * 1000;
            while (!Thread.currentThread().isInterrupted()) {
                for (Map.Entry<Long, Long> entry : executeTaskHeartBeats.entrySet()) {
                    processAlive(entry.getKey(), entry.getValue(), expireTimeout);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("Task Monitor Interrupted.");
                    break;
                }
            }
        }

        private void processAlive(Long executeId, Long timeMills, int expireTimeout) {
            long now = System.currentTimeMillis();
            if (now - timeMills > expireTimeout) {
                LOGGER.warn("Task execute " + executeId + " timeout, remove...");
                unregisterTask(executeId);
            }
        }
    }

}
