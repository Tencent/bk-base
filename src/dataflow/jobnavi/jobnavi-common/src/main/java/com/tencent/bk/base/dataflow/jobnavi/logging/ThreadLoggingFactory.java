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

package com.tencent.bk.base.dataflow.jobnavi.logging;

import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import net.logstash.log4j.JSONEventLayoutV1;
import org.apache.log4j.Appender;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

public class ThreadLoggingFactory {

    private static final Logger logger = Logger.getLogger(ThreadLoggingFactory.class);
    private static final AtomicInteger taskLoggerCounter = new AtomicInteger(0);
    private static final Object taskLoggerCounterLock = new Object();
    private static ThreadLocal<RollingFileAppender> rollingFileAppenderThreadLocal = new ThreadLocal<>();
    private static ThreadLocal<RollingFileAppender> jsonRollingFileAppenderThreadLocal = new ThreadLocal<>();
    private static Configuration conf;

    public static String getLoggerRootPath() throws Exception {
        synchronized (ThreadLoggingFactory.class) {
            if (conf == null) {
                try {
                    conf = new Configuration(true);
                } catch (IOException e) {
                    logger.error("cannot init logger because init config error.", e);
                    throw e;
                }
            }
        }
        return conf.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_PATH, Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
    }

    /**
     * init logger
     *
     * @param executeId
     * @throws Exception
     */
    public static void initLogger(long executeId) throws Exception {
        logger.info("init logging...");

        String logPath = getLoggerRootPath() + File.separator + "exec_" + executeId;
        String logFileName = "jobnavi-task.log";
        String jsonLogFileName = "jobnavi-task-json.log";

        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logger.info("create log dir " + logDir);
            if (!logDir.mkdirs()) {
                logger.warn("log dir:" + logDir + "may not created");
            }
        }
        File logFile = new File(logPath, logFileName);
        try {
            logger.info("create log file " + logFile);
            if (!logFile.createNewFile()) {
                logger.info("log file:" + logFile + " already exist");
            }
        } catch (IOException e) {
            logger.error("cannot init logger because create log file error.", e);
            throw e;
        }

        File jsonLogFile = new File(logPath, jsonLogFileName);
        if (!jsonLogFile.exists()) {
            try {
                logger.info("create log file " + jsonLogFile);
                if (!jsonLogFile.createNewFile()) {
                    logger.info("log file:" + jsonLogFile + " already exist");
                }
            } catch (IOException e) {
                logger.error("cannot init logger because create json log file error.", e);
                throw e;
            }
        }

        PatternLayout patternLayout = new PatternLayout("%d{yyyy/MM/dd HH:mm:ss.SSS Z} %p [%t] [%c{1}] %m%n");
        RollingFileAppender fileAppender1;
        fileAppender1 = null;
        try {
            fileAppender1 = new RollingFileAppender(patternLayout, logPath + File.separator + logFileName);
            fileAppender1.setAppend(false);
            fileAppender1.setImmediateFlush(true);
            fileAppender1.setThreshold(Level.INFO);
        } catch (IOException e) {
            logger.error("failed to create file appender.", e);
        }

        JSONEventLayoutV1 jsonLayout = new JSONEventLayoutV1();
        RollingFileAppender fileAppender2;
        fileAppender2 = null;
        try {
            fileAppender2 = new RollingFileAppender(jsonLayout, logPath + File.separator + jsonLogFileName);
            fileAppender2.setAppend(false);
            fileAppender2.setImmediateFlush(true);
            fileAppender2.setThreshold(Level.INFO);
        } catch (IOException e) {
            logger.error("failed to create file appender.", e);
        }
        rollingFileAppenderThreadLocal.set(fileAppender1);
        jsonRollingFileAppenderThreadLocal.set(fileAppender2);
    }

    /**
     * get logger
     *
     * @param clazz
     * @return
     */
    public static Logger getLogger(Class<?> clazz) {
        RollingFileAppender fileAppender1 = rollingFileAppenderThreadLocal.get();
        RollingFileAppender fileAppender2 = jsonRollingFileAppenderThreadLocal.get();
        if (fileAppender1 == null || fileAppender2 == null) {
            logger.error("JobNavi logger may not initial. please call ThreadLoggingFactory.init first.");
            return Logger.getLogger(clazz);
        }
        Logger logger = Logger.getLogger(clazz);

        logger.removeAllAppenders();
        // 绑定到logger
        logger.setLevel(Level.INFO);
        logger.addAppender(fileAppender1);
        logger.addAppender(fileAppender2);
        return logger;
    }

    /**
     * get logger
     *
     * @param executeId
     * @param name
     * @return
     */
    public static Logger getLogger(long executeId, String name) {
        //init task log directory
        String logPath;
        try {
            logPath = getLoggerRootPath() + File.separator + "exec_" + executeId;
            File logDir = new File(logPath);
            if (!logDir.exists()) {
                logger.info("create task log directory:" + logDir);
                if (!logDir.mkdirs()) {
                    logger.warn("failed to create task log directory:" + logDir.getName());
                }
            }
        } catch (Exception e) {
            logger.warn("failed to init task log directory, retrieve default logger, detail:" + e);
            return Logger.getLogger(name);
        }
        //set log file path
        String loggerClass = conf.getString(Constants.JOBNAVI_RUNNER_THREAD_TASK_LOGGER_CLASS,
                Constants.JOBNAVI_RUNNER_THREAD_TASK_LOGGER_CLASS_DEFAULT);
        Logger taskLogger;
        Enumeration<?> appenders;
        synchronized (taskLoggerCounterLock) {
            int maxLoggerCount = conf.getInt(Constants.JOBNAVI_RUNNER_THREAD_TASK_LOGGER_NUM_MAX,
                    Constants.JOBNAVI_RUNNER_THREAD_TASK_LOGGER_NUM_MAX_DEFAULT);
            if (taskLoggerCounter.addAndGet(1) > maxLoggerCount) {
                //clear finished task logger
                logger.info(
                        "task logger count:" + taskLoggerCounter.get() + " reach the max limit, trying to clear them");
                clearTaskLoggers(loggerClass);
                taskLoggerCounter.set(0);
            }
            taskLogger = Logger.getLogger(loggerClass + "." + name);
            appenders = Logger.getLogger(loggerClass).getAllAppenders();
        }
        //set appenders
        while (appenders != null && appenders.hasMoreElements()) {
            Appender appender = (Appender) appenders.nextElement();
            if (appender instanceof RollingFileAppender) {
                RollingFileAppender rollingFileAppender = new RollingFileAppender();
                rollingFileAppender.setName(appender.getName());
                rollingFileAppender.setThreshold(((RollingFileAppender) appender).getThreshold());
                rollingFileAppender.setMaximumFileSize(((RollingFileAppender) appender).getMaximumFileSize());
                rollingFileAppender.setMaxBackupIndex(((RollingFileAppender) appender).getMaxBackupIndex());
                if (appender.getLayout() instanceof PatternLayout) {
                    String logFileName = conf.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME,
                            Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME_DEFAULT);
                    rollingFileAppender.setLayout(
                            new PatternLayout(((PatternLayout) appender.getLayout()).getConversionPattern()));
                    rollingFileAppender.setFile(logPath + File.separator + logFileName);
                } else if (appender.getLayout() instanceof JSONEventLayoutV1) {
                    String jsonLogFileName = conf.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_JSON_FILE_NAME,
                            Constants.JOBNAVI_RUNNER_TASK_LOG_JSON_FILE_NAME_DEFAULT);
                    rollingFileAppender.setLayout(new JSONEventLayoutV1());
                    rollingFileAppender.setFile(logPath + File.separator + jsonLogFileName);
                }
                rollingFileAppender.activateOptions();
                taskLogger.addAppender(rollingFileAppender);
            }
            taskLogger.setAdditivity(false);
        }
        return taskLogger;
    }

    /**
     * clear unused task loggers
     *
     * @param taskLoggerClass
     */
    public static void clearTaskLoggers(String taskLoggerClass) {
        Hierarchy hierarchy = (Hierarchy) LogManager.getLoggerRepository();
        Enumeration<?> currentLoggers = hierarchy.getCurrentLoggers();
        hierarchy.clear();
        int clearCounter = 0;
        while (currentLoggers.hasMoreElements()) {
            Logger oldLogger = (Logger) currentLoggers.nextElement();
            if (!oldLogger.getName().startsWith(taskLoggerClass + ".")) {
                //keep system loggers
                Enumeration<?> oldAppenders = oldLogger.getAllAppenders();
                Logger newLogger = Logger.getLogger(oldLogger.getName());
                newLogger.setLevel(oldLogger.getLevel());
                newLogger.setAdditivity(oldLogger.getAdditivity());
                while (oldAppenders != null && oldAppenders.hasMoreElements()) {
                    newLogger.addAppender((Appender) oldAppenders.nextElement());
                }
            } else {
                ++clearCounter;
            }
        }
        logger.info("remove " + clearCounter + " loggers from logger repository");
    }

}
