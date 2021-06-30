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

import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.classloader.ClassLoaderBuilder;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

public class LogAggregationService implements Service {

    private static final Logger LOGGER = Logger.getLogger(LogAggregationService.class);
    private static final Map<Long, Long> logAggregations = new ConcurrentHashMap<>();
    private static Configuration config;
    private static ClassLoader classLoader;
    private static boolean isTaskLogAggregation;

    private static void initClassLoader() {
        try {
            classLoader = buildClassLoader();
        } catch (Exception e) {
            LOGGER.error("build class loader failed.", e);
            return;
        }
    }

    private static ClassLoader buildClassLoader() throws Exception {
        String path = System.getProperty("JOBNAVI_HOME");
        String hadoopPath = path + "/share/log-aggregation";

        String libPath = hadoopPath + "/lib";
        List<String> libPaths = new ArrayList<>();
        libPaths.add(libPath);

        String confPath = hadoopPath + "/conf";
        List<String> confPaths = new ArrayList<>();
        confPaths.add(confPath);
        return ClassLoaderBuilder.build(libPaths, confPaths);
    }

    /**
     * aggregate log
     *
     * @param executeId
     */
    public static void callAggregateLog(Long executeId) {
        boolean isTaskLogAggregation = config.getBoolean(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_DEFAULT);
        if (isTaskLogAggregation) {
            logAggregations.put(executeId, System.currentTimeMillis());
            LOGGER.info("call aggregate log success. executeId is: " + executeId);
        }
    }

    /**
     * get task log file size
     *
     * @param executeId
     * @param aggregateTime
     * @return task log file size
     */
    public static Long getTaskLogFileSize(long executeId, long aggregateTime) {
        String logRootPath = config
                .getString(Constants.JOBNAVI_RUNNER_TASK_LOG_PATH, Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
        String logPath = logRootPath + File.separator + "exec_" + executeId;
        String logFileName = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME,
                Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME_DEFAULT);
        File logPathDir = new File(logPath);
        if (logPathDir.exists()) {
            String logFile = logPath + File.separator + logFileName;
            return getFileSize(logFile);
        } else if (isTaskLogAggregation) {
            String hdfsRootPath = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH,
                    Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT);
            Date aggregateDate = new Date(aggregateTime);
            String aggregationPath = hdfsRootPath + File.separator + PeriodUtil.getYear(aggregateDate)
                    + File.separator + PeriodUtil.getMonth(aggregateDate)
                    + File.separator + PeriodUtil.getDay(aggregateDate)
                    + File.separator + "exec_" + executeId;
            String aggregationLogPath = aggregationPath + File.separator + logFileName;
            return getHDFSTaskLogFileSize(aggregationLogPath);
        } else {
            return null;
        }
    }

    /**
     * get task log content
     *
     * @param executeId task execute ID
     * @param begin begin byte offset
     * @param end end byte offset
     * @param aggregateTime log aggregated timestamp(ms)
     * @return file content
     */
    public static String getTaskLog(long executeId, long begin, long end, long aggregateTime) {
        int maxBytes = config.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_MAX_BYTES,
                Constants.JOBNAVI_RUNNER_TASK_LOG_MAX_BYTES_DEFAULT);
        end = Math.min(end, begin + maxBytes);
        String logRootPath = config
                .getString(Constants.JOBNAVI_RUNNER_TASK_LOG_PATH, Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
        String logPath = logRootPath + File.separator + "exec_" + executeId;
        String logFileName = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME,
                Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME_DEFAULT);
        File logPathDir = new File(logPath);
        if (logPathDir.exists()) {
            String logFile = logPath + File.separator + logFileName;
            return readToString(logFile, begin, end);
        } else if (isTaskLogAggregation) {
            String hdfsRootPath = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH,
                    Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT);
            Date aggregateDate = new Date(aggregateTime);
            String aggregationPath = hdfsRootPath + File.separator + PeriodUtil.getYear(aggregateDate)
                    + File.separator + PeriodUtil.getMonth(aggregateDate)
                    + File.separator + PeriodUtil.getDay(aggregateDate)
                    + File.separator + "exec_" + executeId;
            String aggregationLogPath = aggregationPath + File.separator + logFileName;
            return getHDFSTaskLog(aggregationLogPath, begin, end);
        } else {
            return null;
        }
    }

    /**
     * extract last matched content from task log file
     *
     * @param executeId task execute ID
     * @param regex regular expression pattern
     * @param aggregateTime log aggregated timestamp(ms)
     * @return matched content
     */
    public static String extractLastFromTaskLog(long executeId, String regex, long aggregateTime) {
        String logRootPath = config
                .getString(Constants.JOBNAVI_RUNNER_TASK_LOG_PATH, Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
        String logPath = logRootPath + File.separator + "exec_" + executeId;
        String logFileName = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME,
                Constants.JOBNAVI_RUNNER_TASK_LOG_FILE_NAME_DEFAULT);
        File logPathDir = new File(logPath);
        if (logPathDir.exists()) {
            String logFile = logPath + File.separator + logFileName;
            return extractLastFromFile(logFile, regex);
        } else if (isTaskLogAggregation) {
            String hdfsRootPath = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH,
                    Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT);
            Date aggregateDate = new Date(aggregateTime);
            String aggregationPath = hdfsRootPath + File.separator + PeriodUtil.getYear(aggregateDate)
                    + File.separator + PeriodUtil.getMonth(aggregateDate)
                    + File.separator + PeriodUtil.getDay(aggregateDate)
                    + File.separator + "exec_" + executeId;
            String aggregationLogPath = aggregationPath + File.separator + logFileName;
            return extractLastFromHDFSTaskLog(aggregationLogPath, regex);
        } else {
            return null;
        }
    }

    private static long getHDFSTaskLogFileSize(String aggregationLogPath) {
        String className = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS_DEFAULT);
        Object logAggregationUtilObj;
        Method getLogContent;
        try {
            Class<?> mainClass = classLoader.loadClass(className);
            logAggregationUtilObj = mainClass.newInstance();
            getLogContent = mainClass.getDeclaredMethod("getLogFileSize", String.class);
        } catch (Exception e) {
            LOGGER.error("can not initialize " + className, e);
            return -1;
        }

        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return (long) getLogContent.invoke(logAggregationUtilObj, aggregationLogPath);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("invoke log content error. ", e);
            return -1;
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    private static String getHDFSTaskLog(String aggregationLogPath, long begin, long end) {
        String className = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS_DEFAULT);
        Object logAggregationUtilObj;
        Method getLogContent;
        try {
            Class<?> mainClass = classLoader.loadClass(className);
            logAggregationUtilObj = mainClass.newInstance();
            getLogContent = mainClass.getDeclaredMethod("getLogContent", String.class, long.class, long.class);
        } catch (Exception e) {
            LOGGER.error("can not initialize " + className, e);
            return null;
        }

        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return (String) getLogContent.invoke(logAggregationUtilObj, aggregationLogPath, begin, end);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("invoke log content error. ", e);
            return null;
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    private static String extractLastFromHDFSTaskLog(String aggregationLogPath, String regex) {
        String className = config.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS_DEFAULT);
        Object logAggregationUtilObj;
        Method extractLastFromLogFile;
        try {
            Class<?> mainClass = classLoader.loadClass(className);
            logAggregationUtilObj = mainClass.newInstance();
            extractLastFromLogFile = mainClass.getDeclaredMethod("extractLastFromLogFile", String.class, String.class);
        } catch (Exception e) {
            LOGGER.error("can not initialize " + className, e);
            return null;
        }

        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return (String) extractLastFromLogFile.invoke(logAggregationUtilObj, aggregationLogPath, regex);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("invoke extract from log file error. ", e);
            return null;
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }

    public static Long getFileSize(String fileName) {
        File file = new File(fileName);
        try (RandomAccessFile accessFile = new RandomAccessFile(file, "r")) {
            return accessFile.length();
        } catch (Exception e) {
            LOGGER.error("read file error.", e);
            return null;
        }
    }

    /**
     * read file content to string
     *
     * @param fileName
     * @return file content
     */
    public static String readToString(String fileName) {
        String encoding = "UTF-8";
        File file = new File(fileName);
        long fileLength = file.length();
        byte[] fileContent = new byte[(int) fileLength];
        try (FileInputStream in = new FileInputStream(file)) {
            int bytesRead = in.read(fileContent);
            LOGGER.error(bytesRead + " bytes read from file:" + fileName);
        } catch (Exception e) {
            LOGGER.error("read file error.", e);
        }
        try {
            return new String(fileContent, encoding);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("The OS does not support " + encoding, e);
            return null;
        }
    }

    public static String readToString(String fileName, long begin, long end) {
        File file = new File(fileName);
        try (RandomAccessFile accessFile = new RandomAccessFile(file, "r")) {
            int byteCount = 0;
            long fileLength = accessFile.length();
            accessFile.seek(begin);
            byteCount = (int) (Math.min(end, fileLength) - begin);
            byte[] buffer = new byte[byteCount];
            byteCount = accessFile.read(buffer, 0, byteCount);
            return new String(buffer, 0, byteCount, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("read file error.", e);
            return null;
        }
    }

    /**
     * extract matched content from task log file
     *
     * @param fileName
     * @param regex
     * @return matched content
     */
    public static List<String> extractFromFile(String fileName, String regex) {
        File file = new File(fileName);
        List<String> result = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file);
             InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(isr)) {
            String line;
            while ((line = br.readLine()) != null) {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    result.add(matcher.group(0));
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("log file:" + fileName + " not found", e);
        } catch (IOException e) {
            LOGGER.error("extract '" + regex + "' from log file:" + fileName + " failed", e);
        }
        return result;
    }

    /**
     * extract last matched content from file
     *
     * @param fileName file name
     * @param regex regular expression pattern
     * @return matched content
     */
    public static String extractLastFromFile(String fileName, String regex) {
        String result = "";
        Pattern pattern = Pattern.compile(regex);
        try (RandomAccessFile accessFile = new RandomAccessFile(fileName, "r")) {
            long fileLength = accessFile.length();
            int batch = (int) Math.min(fileLength, 512);
            long pos = fileLength - batch;
            //read from tail of file
            accessFile.seek(pos);
            byte[] buffer = new byte[batch];
            StringBuilder line = new StringBuilder();
            int readBytes = accessFile.read(buffer, 0, batch);
            while (readBytes > 0) {
                String lines = new String(buffer, 0, readBytes, StandardCharsets.UTF_8);
                //split into lines
                String[] split = lines.split("\n");
                for (int i = split.length - 1; i >= 0; --i) {
                    if (i < split.length - 1) {
                        //read a whole line
                        Matcher matcher = pattern.matcher(line);
                        if (matcher.find()) {
                            return matcher.group(0);
                        }
                        line.delete(0, line.length());
                    }
                    line.insert(0, split[i]);
                }
                batch = (int) Math.min(pos, batch);
                pos -= batch;
                accessFile.seek(pos);
                readBytes = accessFile.read(buffer, 0, batch);
            }
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                return matcher.group(0);
            }
        } catch (Exception e) {
            LOGGER.error("extract '" + regex + "' from log file:" + fileName + " failed", e);
        }
        return result;
    }

    @Override
    public String getServiceName() {
        return LogAggregationService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        config = conf;
        isTaskLogAggregation = config.getBoolean(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_DEFAULT);
        if (isTaskLogAggregation) {
            initClassLoader();
            Thread logAggregation = new Thread(new LogAggregationThread(conf), "log-aggregation");
            logAggregation.start();
        }
    }

    @Override
    public void stop(Configuration conf) throws NaviException {

    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    static class LogAggregationThread implements Runnable {

        private final int taskLogAggregationInterval;
        private final String logRootPath;
        private final String hdfsRootPath;

        LogAggregationThread(Configuration conf) {
            taskLogAggregationInterval = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_INTERVAL_MILLIS,
                    Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_INTERVAL_MILLIS_DEFAULT);
            logRootPath = conf
                    .getString(Constants.JOBNAVI_RUNNER_TASK_LOG_PATH, Constants.JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT);
            hdfsRootPath = conf.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH,
                    Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT);
        }

        @Override
        public void run() {
            Thread.currentThread().setContextClassLoader(classLoader);

            String className = "com.tencent.blueking.dataflow.jobnavi.log.LogAggregationUtil";
            Object logAggregationUtilObj;
            Method aggregateLogMethod;
            try {
                Class<?> mainClass = Thread.currentThread().getContextClassLoader().loadClass(className);
                logAggregationUtilObj = mainClass.newInstance();
                aggregateLogMethod = mainClass.getDeclaredMethod("aggregateLog", String.class, String.class);
            } catch (Throwable e) {
                LOGGER.error("can not initialize " + className, e);
                return;
            }

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long now = System.currentTimeMillis();
                    Iterator<Map.Entry<Long, Long>> iterator = logAggregations.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, Long> entry = iterator.next();
                        long executeId = entry.getKey();
                        long aggregationTime = entry.getValue();
                        if (aggregationTime + taskLogAggregationInterval < now) {
                            String logPath = logRootPath + File.separator + "exec_" + executeId;
                            Date aggreagtionDate = new Date(aggregationTime);
                            String aggregationPath = hdfsRootPath + File.separator + PeriodUtil.getYear(aggreagtionDate)
                                    + File.separator + PeriodUtil.getMonth(aggreagtionDate)
                                    + File.separator + PeriodUtil.getDay(aggreagtionDate)
                                    + File.separator + "exec_" + executeId;
                            aggregateLogMethod.invoke(logAggregationUtilObj, logPath, aggregationPath);
                            iterator.remove();
                        }
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.info("log aggregating thread interrupted.");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("aggregate log failed.", e);
                }
            }
        }
    }

}
