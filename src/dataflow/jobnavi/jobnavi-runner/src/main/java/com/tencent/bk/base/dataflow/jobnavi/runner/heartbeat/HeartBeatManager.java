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

package com.tencent.bk.base.dataflow.jobnavi.runner.heartbeat;

import com.tencent.bk.base.dataflow.jobnavi.runner.Main;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.TaskProcessManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.TaskThreadManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.runner.decommission.DecommissionTaskManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.log4j.Logger;

public class HeartBeatManager {

    private static final Logger LOGGER = Logger.getLogger(HeartBeatManager.class);

    private static Configuration conf;

    private static Thread heartBeatThread;

    private static HardwareLoad hardwareLoad;

    private static Thread collectHardwareLoadThread;

    /**
     * init heartbeat manager
     *
     * @param config
     */
    public static void init(Configuration config) {
        conf = config;
        Set<String> labelSet = new HashSet<>();
        String labelListStr = conf
                .getString(Constants.JOBNAVI_RUNNER_LABEL_LIST, Constants.JOBNAVI_RUNNER_LABEL_LIST_DEFAULT);
        for (String label : labelListStr.split(",")) {
            if (!label.trim().isEmpty()) {
                labelSet.add(label.trim());
            }
        }
        Main.getRunnerInfo().setLabelSet(labelSet);
        int timeout = conf.getInt(Constants.JOBNAVI_RUNNER_HEARTBEAT_INTERNAL_SECOND,
                Constants.JOBNAVI_RUNNER_HEARTBEAT_INTERNAL_SECOND_DEFAULT);
        heartBeatThread = new Thread(new HeartBeatThread(timeout), "heartbeat");
        heartBeatThread.start();
        hardwareLoad = new HardwareLoad();
        int defaultCollectInterval = Constants.JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_INTERNAL_SECOND_DEFAULT;
        int collectInterval = conf
                .getInt(Constants.JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_INTERNAL_SECOND, defaultCollectInterval);
        if (collectInterval <= 0) {
            collectInterval = defaultCollectInterval;
        }
        //collect hardware load in an independent thread
        collectHardwareLoadThread = new Thread(new CollectHardwareLoadThread(collectInterval), "collect-hardware-load");
        collectHardwareLoadThread.start();
    }

    /**
     * collect runner info
     *
     * @return runner info
     * @throws NaviException
     */
    public static RunnerInfo collect() throws NaviException {
        RunnerInfo info = Main.getRunnerInfo();
        info.setMaxTaskNum(
                conf.getInt(Constants.JOBNAVI_RUNNER_TASK_MAX_NUM, Constants.JOBNAVI_RUNNER_TASK_MAX_NUM_DEFAULT));
        info.setTaskNum(TaskProcessManager.getTaskNum());

        info.setMaxThreadTaskNum(conf.getInt(Constants.JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM,
                Constants.JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM_DEFAULT));
        info.setTaskThreadNum(TaskThreadManager.getTaskNum());

        if (info.getExecIdSet() == null) {
            info.setExecIdSet(new HashSet<Long>());
        } else {
            info.getExecIdSet().clear();
        }
        info.getExecIdSet().addAll(TaskProcessManager.getExecuteTasks().keySet());
        info.getExecIdSet().addAll(TaskThreadManager.getExecuteTasks().keySet());

        info.setDecommissioningTasks(DecommissionTaskManager.getAllDecommissionTask());
        fillHardwareLoad(info);
        changeRunnerStatus(info);
        try {
            boolean isRunnerIdEnabled = conf
                    .getBoolean(Constants.JOBNAVI_RUNNER_ID_ENABLED, Constants.JOBNAVI_RUNNER_ID_ENABLED_DEFAULT);
            if (isRunnerIdEnabled) {
                if (conf.getString(Constants.JOBNAVI_RUNNER_ID) != null) {
                    info.setRunnerId(conf.getString(Constants.JOBNAVI_RUNNER_ID));
                } else {
                    //use hostname:port as Runner ID default
                    info.setRunnerId((InetAddress.getLocalHost()).getCanonicalHostName() + ":" + info.getPort());
                    info.setHost((InetAddress.getLocalHost()).getCanonicalHostName());
                }
            } else {
                info.setHost((InetAddress.getLocalHost()).getCanonicalHostName());
            }
        } catch (UnknownHostException e) {
            throw new NaviException(e);
        }
        return info;
    }

    /**
     * check if current runner full loaded
     *
     * @return true if one of the load metrics is above threshold or load metrics has not been collect for some time
     */
    public static boolean isFullLoad() {
        int hardwareLoadCollectTimeout = conf.getInt(Constants.JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_TIMEOUT_SECOND,
                Constants.JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_TIMEOUT_SECOND_DEFAULT);
        double maxCpuUsage = conf
                .getDouble(Constants.JOBNAVI_RUNNER_MAX_CPU_USAGE, Constants.JOBNAVI_RUNNER_MAX_CPU_USAGE_DEFAULT);
        double maxCpuAvgLoad = conf.getDouble(Constants.JOBNAVI_RUNNER_MAX_CPU_AVG_LOAD,
                Constants.JOBNAVI_RUNNER_MAX_CPU_AVG_LOAD_DEFAULT);
        double maxMemoryUsage = conf.getDouble(Constants.JOBNAVI_RUNNER_MAX_MEMORY_USAGE,
                Constants.JOBNAVI_RUNNER_MAX_MEMORY_USAGE_DEFAULT);

        return hardwareLoad.getLastUpdateTime() == null || hardwareLoad.getCpuUsage() > maxCpuUsage
                || hardwareLoad.getCpuAvgLoad() > maxCpuAvgLoad || hardwareLoad.getMemoryUsage() > maxMemoryUsage
                || System.currentTimeMillis() - hardwareLoad.getLastUpdateTime()
                > (long) hardwareLoadCollectTimeout * 1000;
    }

    private static void changeRunnerStatus(RunnerInfo info) {
        boolean isUnavailable =
                (info.getTaskNum() >= info.getMaxTaskNum() && info.getTaskThreadNum() >= info.getMaxThreadTaskNum())
                        || isFullLoad();
        if (info.getStatus() == RunnerStatus.running && isUnavailable) {
            LOGGER.info(
                    "status of this Runner changed to unavailable, current node info:" + Main.getRunnerInfo().toJson());
            info.setStatus(RunnerStatus.unavailable);
        } else if (info.getStatus() == RunnerStatus.unavailable && !isUnavailable) {
            LOGGER.info("status of this Runner changed to running");
            info.setStatus(RunnerStatus.running);
        }
    }

    private static void fillHardwareLoad(RunnerInfo info) {
        info.setCpuUsage(hardwareLoad.getCpuUsage());
        info.setCpuAvgLoad(hardwareLoad.getCpuAvgLoad());
        info.setMemory(hardwareLoad.getMemory());
        info.setMemoryUsage(hardwareLoad.getMemoryUsage());
    }

    private static void collectCpuUsage(HardwareLoad hardwareLoad) {
        try {
            /*
            $ cat /proc/stat | grep 'cpu\s'

            cpu  4409701839 5860491 3043372756 11777957443 471600199 13606335 49392558 0

            ----------------------------------------------------------------------------

            user：从系统启动开始累计到当前时刻，用户态的CPU时间 ，不包含 nice值为负进程。

            nice：从系统启动开始累计到当前时刻，nice值为负的进程所占用的CPU时间

            system：从系统启动开始累计到当前时刻，内核态时间

            idle：从系统启动开始累计到当前时刻，除硬盘IO等待时间以外其它等待时间

            iowait：从系统启动开始累计到当前时刻，硬盘IO等待时间

            irq：从系统启动开始累计到当前时刻，硬中断时间

            softirq：从系统启动开始累计到当前时刻，软中断时间

            steal：在虚拟环境下 CPU 花在处理其他作业系统的时间，Linux 2.6.11 开始才开始支持。

            guest：在 Linux 内核控制下 CPU 为 guest 作业系统运行虚拟 CPU 的时间，Linux 2.6.24 开始才开始支持。（因为内核版本不支持，上面的示例没有这一列）

            ----------------------------------------------------------------------------

            cpu_total = user + nice + system + idle + iowait + irq + softirq

            cpu_used = user + nice + system + irq + softirq

            */
            ArrayList<String> output = executeCommand("cat /proc/stat|grep 'cpu\\s'");
            // process the output from bash call.
            if (output.size() > 0) {
                final String[] splitedResult = output.get(0).split("\\s+");
                if (splitedResult.length < 8) {
                    throw new Exception("unexpected output of `cat /proc/stat|grep 'cpu\\s'`: " + output.get(0));
                }

                double cpuUsage = 0.0;
                try {
                    double cpuTimeTotal = 0.0;
                    for (int i = 1; i < 8; ++i) {
                        cpuTimeTotal += Double.parseDouble(splitedResult[i]);
                    }
                    double cpuTimeUsed = cpuTimeTotal - Double.parseDouble(splitedResult[4]) - Double
                            .parseDouble(splitedResult[5]);
                    //cpuUsage = (cpu_used2 - cpu_used1) / (cpu_total2 - cpu_total1) * 100%
                    cpuUsage = (cpuTimeUsed - hardwareLoad.getLastCpuTimeUsed()) / (cpuTimeTotal - hardwareLoad
                            .getLastCpuTimeTotal()) * 100;
                    hardwareLoad.setLastCpuTimeTotal(cpuTimeTotal);
                    hardwareLoad.setLastCpuTimeUsed(cpuTimeUsed);
                } catch (final NumberFormatException e) {
                    LOGGER.error("yielding 0.0 for CPU usage as output is invalid -" + output.get(0));
                }
                hardwareLoad.setCpuUsage(cpuUsage);
            }
        } catch (final Exception ex) {
            LOGGER.error("failed fetch system load info "
                    + "as exception is captured when fetching result from bash call. Ex -" + ex
                    .getMessage());
        }
    }

    private static void collectCpuAvgLoad(HardwareLoad hardwareLoad) {
        try {
            ArrayList<String> output = executeCommand("cat /proc/loadavg");
            // process the output from bash call.
            if (output.size() > 0) {
                final String[] splitedResult = output.get(0).split("\\s+");
                double loadAvg = 0.0;
                try {
                    loadAvg = Double.parseDouble(splitedResult[0]);
                } catch (final NumberFormatException e) {
                    throw new Exception("yielding 0.0 for CPU usage as output is invalid -" + output.get(0));
                }
                ArrayList<String> output2 = executeCommand("cat /proc/cpuinfo | grep processor | wc -l");
                if (output2.size() > 0) {
                    final String[] splitedResult2 = output2.get(0).split("\\s+");
                    int cpuAmount = Integer.parseInt(splitedResult2[0]);
                    if (cpuAmount <= 0) {
                        cpuAmount = 1;
                    }
                    hardwareLoad.setCpuAvgLoad(round(loadAvg / cpuAmount));
                }
            }
        } catch (final Throwable ex) {
            LOGGER.error("failed fetch system load info "
                    + "as exception is captured when fetching result from bash call. Ex -" + ex
                    .getMessage());
        }
    }

    private static Double round(double f) {
        BigDecimal bg = new BigDecimal(f);
        return bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    protected static void collectHardwareLoad() {
        collectCpuUsage(hardwareLoad);
        collectCpuAvgLoad(hardwareLoad);
        collectMemory(hardwareLoad);
        hardwareLoad.setLastUpdateTime(System.currentTimeMillis());
    }

    protected static void collectMemory(HardwareLoad hardwareLoad) {
        try {
            ArrayList<String> output = executeCommand(
                    "cat /proc/meminfo | grep -E \"^MemTotal:|^MemFree:|^Buffers:|^Cached:|^SwapCached:\"");
            long totalMemory = 0;
            long totalFreeMemory = 0;
            Long parsedResult = 0L;

            // process the output from bash call.
            // we expect the result from the bash call to be something like following -
            // MemTotal:       65894264 kB
            // MemFree:        57753844 kB
            // Buffers:          305552 kB
            // Cached:          3802432 kB
            // SwapCached:            0 kB
            // Note : total free memory = freeMemory + cached + buffers + swapCached
            // TODO : think about merging the logic in systemMemoryInfo as the logic is similar
            if (output.size() == 5) {
                for (final String result : output) {
                    // find the total memory and value the variable.
                    parsedResult = extractMemoryInfo("MemTotal", result);
                    if (null != parsedResult) {
                        totalMemory = parsedResult;
                        continue;
                    }

                    // find the free memory.
                    parsedResult = extractMemoryInfo("MemFree", result);
                    if (null != parsedResult) {
                        totalFreeMemory += parsedResult;
                        continue;
                    }

                    // find the Buffers.
                    parsedResult = extractMemoryInfo("Buffers", result);
                    if (null != parsedResult) {
                        totalFreeMemory += parsedResult;
                        continue;
                    }

                    // find the Cached.
                    parsedResult = extractMemoryInfo("SwapCached", result);
                    if (null != parsedResult) {
                        totalFreeMemory += parsedResult;
                        continue;
                    }

                    // find the Cached.
                    parsedResult = extractMemoryInfo("Cached", result);
                    if (null != parsedResult) {
                        totalFreeMemory += parsedResult;
                        continue;
                    }
                }
            } else {
                LOGGER.error(
                        "failed to get total/free memory info as the bash call returned invalid result."
                                + String.format(" Output from the bash call - %s ", output.toString()));
            }

            // the number got from the proc file is in KBs we want
            // to see the number in MBs so we are dividing it by 1024.
            hardwareLoad.setMemory(totalFreeMemory / 1024);
            hardwareLoad.setMemoryUsage(totalMemory == 0 ? 0
                    : (((double) totalMemory - (double) totalFreeMemory) / (double) totalMemory) * 100);
        } catch (final Exception ex) {
            LOGGER.error("failed fetch system memory info "
                    + "as exception is captured when fetching result from bash call. Ex -" + ex
                    .getMessage());
        }
    }

    private static ArrayList<String> executeCommand(String cmd) throws Exception {
        final ArrayList<String> output = new ArrayList<>();
        final java.lang.ProcessBuilder processBuilder =
                new java.lang.ProcessBuilder("/bin/bash", "-c", cmd);
        final Process process = processBuilder.start();
        process.waitFor();
        final InputStream inputStream = process.getInputStream();
        try {
            final java.io.BufferedReader reader = new java.io.BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line = null;
            while ((line = reader.readLine()) != null) {
                output.add(line);
            }
        } finally {
            inputStream.close();
        }
        return output;
    }


    private static Long extractMemoryInfo(final String field, final String result) {
        Long returnResult = null;
        if (null != result && null != field && result.matches(String.format("^%s:.*", field))
                && result.split("\\s+").length > 2) {
            try {
                returnResult = Long.parseLong(result.split("\\s+")[1]);
                LOGGER.debug(field + ":" + returnResult);
            } catch (final NumberFormatException e) {
                returnResult = 0L;
                LOGGER.error(String.format("yielding 0 for %s as output is invalid - %s", field, result));
            }
        }
        return returnResult;
    }


    public static void sendHeartbeat(boolean printLog) throws Exception {
        RunnerInfo info = collect();
        if (printLog) {
            LOGGER.info("runner info: " + info.toJson());
        }
        if (info.getPort() != null) {
            HAProxy.sendPostRequest("/sys/heartbeat", info.toJson(), null);
        }
    }

    public static void stop() {
        heartBeatThread.interrupt();
        collectHardwareLoadThread.interrupt();
    }

    static class HeartBeatThread implements Runnable {

        int timeout;

        HeartBeatThread(int timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long startTime = System.currentTimeMillis();
                    sendHeartbeat(false);
                    long endTime = System.currentTimeMillis();
                    if (endTime - startTime > (long) timeout * 1000 * 10) {
                        LOGGER.warn(
                                String.format("unexpected long time (%dms) since last heartbeat", endTime - startTime));
                    }
                    Thread.sleep(timeout * 1000);
                } catch (Throwable e) {
                    LOGGER.error("HeartBeat error", e);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                        LOGGER.error("HeartBeat thread sleep interrupt", e1);
                    }
                }
            }
        }
    }

    static class CollectHardwareLoadThread implements Runnable {

        int interval;

        CollectHardwareLoadThread(int interval) {
            this.interval = interval;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long startTime = System.currentTimeMillis();
                    collectHardwareLoad();
                    long endTime = System.currentTimeMillis();
                    if (endTime - startTime > (long) interval * 1000 * 10) {
                        LOGGER.warn(String.format("unexpected long time (%dms) since last hardware collect",
                                endTime - startTime));
                    }
                    Thread.sleep(interval * 1000);
                } catch (Throwable e) {
                    LOGGER.error("collect hardware load error", e);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                        LOGGER.error("CollectHardwareLoadThread thread sleep interrupt", e1);
                    }
                }
            }
        }
    }

    static class HardwareLoad {

        private Double lastCpuTimeTotal = 0.0;
        private Double lastCpuTimeUsed = 0.0;
        private Double cpuUsage;
        private Double cpuAvgLoad;
        private Long memory;
        private Double memoryUsage;
        private Long lastUpdateTime;

        public Double getLastCpuTimeTotal() {
            return lastCpuTimeTotal;
        }

        public void setLastCpuTimeTotal(Double lastCpuTimeTotal) {
            this.lastCpuTimeTotal = lastCpuTimeTotal;
        }

        public Double getLastCpuTimeUsed() {
            return lastCpuTimeUsed;
        }

        public void setLastCpuTimeUsed(Double lastCpuTimeUsed) {
            this.lastCpuTimeUsed = lastCpuTimeUsed;
        }

        public Double getCpuUsage() {
            return cpuUsage;
        }

        public void setCpuUsage(Double cpuUsage) {
            this.cpuUsage = cpuUsage;
        }

        public Double getCpuAvgLoad() {
            return cpuAvgLoad;
        }

        public void setCpuAvgLoad(Double cpuAvgLoad) {
            this.cpuAvgLoad = cpuAvgLoad;
        }

        public Double getMemoryUsage() {
            return memoryUsage;
        }

        public void setMemoryUsage(Double memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        public Long getMemory() {
            return memory;
        }

        /**
         * set Memory, unit is MB
         *
         * @param memory
         */
        public void setMemory(Long memory) {
            this.memory = memory;
        }

        public Long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(Long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }
    }
}
