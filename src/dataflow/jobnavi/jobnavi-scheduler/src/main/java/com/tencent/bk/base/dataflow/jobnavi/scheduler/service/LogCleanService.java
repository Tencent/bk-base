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

import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.classloader.ClassLoaderBuilder;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUtil;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

public class LogCleanService implements Service {

    private static final Logger LOGGER = Logger.getLogger(LogCleanService.class);
    private static Thread logCleanThread;

    @Override
    public String getServiceName() {
        return LogCleanService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        boolean isLogAggregation = conf.getBoolean(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION,
                Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_DEFAULT);
        if (isLogAggregation) {
            LOGGER.info("Start clean thread...");
            logCleanThread = new Thread(new LogCleanThread(conf), "log-clean");
            logCleanThread.start();
        }
    }

    @Override
    public void stop(Configuration conf) throws NaviException {
        if (logCleanThread != null) {
            logCleanThread.interrupt();
        }
    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }


    static class LogCleanThread implements Runnable {

        private final Configuration conf;

        LogCleanThread(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public void run() {
            ClassLoader classLoader;
            try {
                classLoader = buildClassLoader();
            } catch (Exception e) {
                LOGGER.error("build class loader failed.", e);
                return;
            }
            LOGGER.info("classloader: " + classLoader.toString());
            Thread.currentThread().setContextClassLoader(classLoader);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    String hdfsRootPath = conf.getString(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH,
                            Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT);
                    int expireDay = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_EXPIRE_DAY,
                            Constants.JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_EXPIRE_DAY_DEFAULT);
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.DATE, -1 * expireDay);
                    Date expireDate = new Date(calendar.getTimeInMillis());
                    String expirePath = hdfsRootPath + File.separator + PeriodUtil.getYear(expireDate)
                            + File.separator + PeriodUtil.getMonth(expireDate)
                            + File.separator + PeriodUtil.getDay(expireDate);
                    LOGGER.info("clear hdfs log path:" + expirePath);
                    delete(expirePath);
                } catch (Throwable e) {
                    LOGGER.error("clear hdfs log failed.", e);
                }

                try {
                    Thread.sleep(24 * 60 * 60 * 1000);
                } catch (InterruptedException e) {
                    LOGGER.info("clear hdfs log thread interrupt.");
                    break;
                }
            }
        }

        private void delete(String expirePath) throws ClassNotFoundException, IllegalAccessException,
                                                      InstantiationException, NoSuchMethodException,
                                                      InvocationTargetException {
            String className = "com.tencent.blueking.dataflow.jobnavi.log.LogAggregationUtil";
            Class<?> mainClass = Thread.currentThread().getContextClassLoader().loadClass(className);
            Object o = mainClass.newInstance();
            Method m = mainClass.getDeclaredMethod("delete", String.class);
            m.invoke(o, expirePath);
        }

        private ClassLoader buildClassLoader() throws Exception {
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
    }
}
