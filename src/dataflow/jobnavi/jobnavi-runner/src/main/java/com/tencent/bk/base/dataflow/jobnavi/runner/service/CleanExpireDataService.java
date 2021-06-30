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

package com.tencent.bk.base.dataflow.jobnavi.runner.service;

import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.TaskThreadManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.log4j.Logger;

public class CleanExpireDataService implements Service {

    private static final Logger LOGGER = Logger.getLogger(CleanExpireDataService.class);

    private static boolean deleteDir(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public String getServiceName() {
        return CleanExpireDataService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        LOGGER.info("Start clean thread...");
        cleanProjects();
        Thread cleanExpireDataThread = new Thread(new CleanExpireDataThread(conf), "clean-thread");
        cleanExpireDataThread.start();
    }

    /**
     * clear projects directory
     */
    public void cleanProjects() {
        String path = System.getProperty("JOBNAVI_HOME");
        String projectPath = path + "/projects";
        try {
            File f = new File(projectPath);
            if (f.exists() && f.isDirectory()) {
                File[] files = f.listFiles();
                if (files != null) {
                    for (File file : files) {
                        LOGGER.info("clean " + file.getCanonicalPath());
                        deleteDir(file);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("clear project dir " + projectPath + " failed.", e);
        }
    }

    @Override
    public void stop(Configuration conf) throws NaviException {

    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    static class CleanExpireDataThread implements Runnable {

        Configuration conf;

        CleanExpireDataThread(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                LOGGER.info("Start clean task log...");
                long expireDay = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_EXPIRE_DAY,
                        Constants.JOBNAVI_RUNNER_TASK_LOG_EXPIRE_DAY_DEFAULT);
                long expireTimeMills = expireDay * 24 * 60 * 60 * 1000;
                clearLogFiles(expireTimeMills);
                clearProjectFiles(expireTimeMills);
                try {
                    Thread.sleep(24 * 60 * 60 * 1000);
                } catch (InterruptedException e) {
                    LOGGER.info("clean-thread Interrupted.");
                    break;
                }
            }
        }

        private void clearLogFiles(long expireTimeMills) {
            long now = System.currentTimeMillis();
            String logPath = conf.getString(
                    com.tencent.bk.base.dataflow.jobnavi.conf.Constants.JOBNAVI_RUNNER_TASK_LOG_PATH,
                    com.tencent.bk.base.dataflow.jobnavi.conf.Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
            File file = new File(logPath);
            File[] logFiles = file.listFiles();
            if (logFiles != null) {
                for (File logFileDir : logFiles) {
                    if (!logFileDir.isDirectory()) {
                        continue;
                    }
                    long modifyTime = logFileDir.lastModified();
                    String fileName = logFileDir.getName();
                    long executeId = Long.parseLong(fileName.substring("exec_".length()));
                    if (now - modifyTime > expireTimeMills && !isTaskRunning(executeId) && !deleteDir(logFileDir)) {
                        LOGGER.warn("Delete log file [" + logFileDir.getName() + "] error.");
                    }
                }
            }
        }

        private void clearProjectFiles(long expireTimeMills) {
            long now = System.currentTimeMillis();
            String projectPath = System.getProperty("JOBNAVI_HOME") + "/projects";
            File projectFilePath = new File(projectPath);
            if (projectFilePath.exists()) {
                File[] projectFiles = projectFilePath.listFiles();
                if (projectFiles != null) {
                    for (File projectDir : projectFiles) {
                        if (!projectDir.isDirectory()) {
                            continue;
                        }
                        long modifyTime = projectDir.lastModified();
                        String fileName = projectDir.getName();
                        long executeId = Long.parseLong(fileName);
                        if (now - modifyTime > expireTimeMills && !isTaskRunning(executeId) && !deleteDir(projectDir)) {
                            LOGGER.warn("Delete log file [" + projectDir.getName() + "] error.");
                        }
                    }
                }
            }
        }

        private boolean isTaskRunning(long executeId) {
            return (TaskProcessManager.getExecuteTasks().containsKey(executeId) || TaskThreadManager.getExecuteTasks()
                    .containsKey(executeId));
        }
    }
}
