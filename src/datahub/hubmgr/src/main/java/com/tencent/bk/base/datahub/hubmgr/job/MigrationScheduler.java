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

package com.tencent.bk.base.datahub.hubmgr.job;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAHUB_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_LIST;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_LIST_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_START;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_START_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_STATUS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_STATUS_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_STOP;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_STOP_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_UPDATE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_MIGRATION_TASK_UPDATE_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUS_ADMIN;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeLock;
import com.tencent.bk.base.datahub.hubmgr.utils.HdfsUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MigrationScheduler implements Job {

    private static final Logger log = LoggerFactory.getLogger(MigrationScheduler.class);
    private static final long LOCK_SEC = 5;

    private String receivers;
    private String apiDns;
    private String taskListPath;
    private String updateTaskPath;
    private String startTaskPath;
    private String stopTaskPath;
    private String taskStatusPath;
    private String getTaskUrl;
    private String updateTaskUrl;

    private Map<String, Map<String, List<Map<String, Object>>>> tasks = new HashMap<>();
    private Map<String, Map<String, Object>> overallTaskMap = new HashMap<>();


    /**
     * 初始化各种配置参数
     */
    private void init() {
        DatabusProps props = DatabusProps.getInstance();
        receivers = props.getOrDefault(DATABUS_ADMIN, "");
        apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        taskListPath = props.getOrDefault(API_MIGRATION_LIST, API_MIGRATION_LIST_DEFAULT);
        updateTaskPath = props.getOrDefault(API_MIGRATION_TASK_UPDATE, API_MIGRATION_TASK_UPDATE_DEFAULT);
        startTaskPath = props.getOrDefault(API_MIGRATION_TASK_START, API_MIGRATION_TASK_START_DEFAULT);
        stopTaskPath = props.getOrDefault(API_MIGRATION_TASK_STOP, API_MIGRATION_TASK_STOP_DEFAULT);
        taskStatusPath = props.getOrDefault(API_MIGRATION_TASK_STATUS, API_MIGRATION_TASK_STATUS_DEFAULT);
        getTaskUrl = String.format("http://%s%s", apiDns, taskListPath);
        updateTaskUrl = String.format("http://%s%s", apiDns, updateTaskPath);
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        long triggerTime = jobExecutionContext.getFireTime().getTime();
        LogUtils.info(log, "databus cluster status check triggered at {}", triggerTime);

        // 集群检查是排他性任务，只能在一个实例上被触发，这里首先通过zk获取锁
        String lockPath = String.format("/databusmgr/lock/%s", MigrationScheduler.class.getSimpleName());
        try (DistributeLock lock = new DistributeLock(lockPath)) {
            init();

            if (lock.lock(LOCK_SEC)) {
                ApiResult result;
                try {
                    result = HttpUtils.getApiResult(getTaskUrl);
                    if (!result.isResult()) {
                        return;
                    }
                } catch (Exception e) {
                    LogUtils.warn(log, "failed to get migration list", e);
                    return;
                }

                // 生成任务列表： 任务集 -> { 运行状态 -> 任务列表}
                // 任务集，一个迁移任务的所有任何集合
                // 接口中返回的任务，都是未完成状态
                // 任务状态说明： init-等待调起，running-运行中，source_success-已拉取数据完成，sink_success-已写入完成
                taskSorting(result);

                // 并发执行tasks中的任务
                executeTasks();

                // 确保占用锁的时间超过LOCK_SEC的时间，避免释放锁太快导致其他进程里的job获取到锁，重复执行
                long duration = System.currentTimeMillis() - triggerTime;
                if (duration < (3 + LOCK_SEC) * 1000) {
                    try {
                        Thread.sleep((10 + LOCK_SEC) * 1000 - duration);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            } else {
                // 获取执行锁失败
                LogUtils.info(log, "unable to get a lock to execute job logic!");
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to run databus cluster status check job!", e);
            // 集群状态检查失败时，需要通知管理员
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                    "【数据迁移】执行任务迁移调度失败： " + ExceptionUtils.getStackTrace(e));
            throw new JobExecutionException(e);
        }
    }

    /**
     * 并发执行tasks中的任务
     */
    private void executeTasks() {
        for (Map.Entry<String, Map<String, List<Map<String, Object>>>> entry : tasks.entrySet()) {
            Map<String, List<Map<String, Object>>> statusTasksMap = entry.getValue();
            Map<String, Object> taskDetail = overallTaskMap.get(entry.getKey());
            if (null == taskDetail) {
                LogUtils.warn(log, "task {} not fund overall, skip it!", entry.getKey());
                continue;
            }
            int parallelism = (int) taskDetail.get("parallelism");

            LogUtils.info(log, "begin to process {}:", entry.getKey());
            for (Map.Entry<String, List<Map<String, Object>>> item : statusTasksMap.entrySet()) {
                LogUtils.info(log, "status: {}, task count: {}", item.getKey(), item.getValue().size());
            }

            // 新任务第一次运行，按并发度启动任务
            if (statusTasksMap.size() == 1 && statusTasksMap.containsKey("init")) {
                LogUtils.info(log, "only init task, begin to process!");
                startTasks(statusTasksMap.get("init"), parallelism, "http://" + apiDns + startTaskPath);
            }

            // 有正常运行中任务，检查状态
            if (statusTasksMap.containsKey("running") || statusTasksMap.containsKey("source_success")) {
                // 生成运行中任务列表
                List<Map<String, Object>> runningTaskList =
                        statusTasksMap.computeIfAbsent("running", k -> new ArrayList<>());
                runningTaskList.addAll(statusTasksMap.computeIfAbsent("source_success", k -> new ArrayList<>()));
                checkRunningTask(runningTaskList);
            }

            // 有任务写入完成
            if (statusTasksMap.containsKey("sink_success")) {
                // 子任务完成了~
                // 如果没有其他状态任务，就大功告成
                processFinishTask((int) overallTaskMap.get(entry.getKey()).get("id"), parallelism, statusTasksMap);
            }

            // 如果没有未完成状态的任务，可能是前一次调度更新总任务失败，再执行一次
            if (statusTasksMap.isEmpty()) {
                Map<String, Object> params = new HashMap<>();
                params.put("task_id", overallTaskMap.get(entry.getKey()).get("id"));
                params.put("status", "finish");
                HttpUtils.postAndCheck(updateTaskUrl, params);
            }
        }

    }

    /**
     * 任务分拣
     *
     * @param result 查询到任务详情
     */
    private void taskSorting(ApiResult result) {
        // 以下功能：任务分类和简单处理
        List<String> failInitList = new ArrayList<>();
        for (Map<String, Object> task : (List<Map<String, Object>>) result.getData()) {
            // 总览任务，不写入任务列表，只记录并发数
            if (task.get("task_type").equals("overall")) {
                if (overallTaskProcess(task)) {
                    overallTaskMap.put((String) task.get("task_label"), task);
                } else {
                    LogUtils.info(log, "fail to process overall task: {}", task);
                    failInitList.add((String) task.get("task_label"));
                }
                continue;
            }

            // source/sink任务处理，按照任务状态分类
            Map<String, List<Map<String, Object>>> taskSet =
                    tasks.computeIfAbsent((String) task.get("task_label"), k -> new HashMap<>());
            List<Map<String, Object>> jobList =
                    taskSet.computeIfAbsent((String) task.get("status"), k -> new ArrayList<>());
            jobList.add(task);
        }

        // 更新总览任务状态失败的任务不处理，待下个周期更新成功后再处理
        for (String strTask : failInitList) {
            tasks.remove(strTask);
        }
    }

    /**
     * 在待启动任务列表中启动指定数量的任务
     *
     * @param taskList 待启动任务列表
     * @param startNum 启动任务数量
     * @param startTaskPath 启动任务url
     */
    private void startTasks(List<Map<String, Object>> taskList, int startNum, String startTaskPath) {
        ApiResult result;
        LogUtils.info(log, "begin to start jobs: {}", taskList);
        for (int i = 0, index = 0; i < startNum; i++) {
            if (taskList.size() == index) {
                break;
            }

            String path = startTaskPath.replace("#taskId#", taskList.get(index).get("id").toString());
            try {
                if ((boolean) taskList.get(index).get("overwrite")) {
                    // 删除目标存储数据
                    if (!deleteOldData(taskList.get(index))) {
                        i -= 1;
                        index += 1;
                        LogUtils.warn(log, "failed to delete file, doesnot start the task: {}", taskList.get(index));
                        continue;
                    }
                    LogUtils.info(log, "success to delete old data!");
                }

                // 启动任务
                result = HttpUtils.getApiResult(path);
                if (!result.isResult()) {
                    // 启动失败
                    LogUtils.warn(log, "failed to start the task: {}, result: {}", taskList.get(index), result);
                    i -= 1;
                }
                LogUtils.info(log, "success to start task: {}", path);
            } catch (Exception ignor) {
                LogUtils.warn(log, "fail to start task[{}]: {}", taskList.get(index).get("id"), ignor);
                i -= 1;
            }
            index += 1;
        }
    }

    /**
     * 检查运行中任务
     *
     * @param runningTaskList 运行中任务列表
     */
    private void checkRunningTask(List<Map<String, Object>> runningTaskList) {
        ApiResult result;
        for (Map<String, Object> task : runningTaskList) {
            String path = taskStatusPath.replace("#taskId#", task.get("id") + "");
            String url = String.format("http://%s%s", apiDns, path);
            try {
                result = HttpUtils.getApiResult(url);
                if (!result.isResult()) {
                    // 检查失败，跳过，下个周期继续检查
                    continue;
                }

                String taskInfo = "rt：" + task.get("result_table_id") + ",任务id：" + task.get("id");
                Map<String, Map<String, Object>> taskStatus = getTaskStatus((Map<String, Object>) result.getData());
                if (taskStatus.get("source").get("status").equals("error")
                        || taskStatus.get("sink").get("status").equals("error")) {
                    // 异常任务处理：停任务，删数据，起任务
                    // 为什么要这么麻烦？？因为pulsar任务没法设置从起点处消费，为保障数据完整，只能重来了
                    path = stopTaskPath.replace("#taskId#", task.get("id").toString());
                    url = String.format("http://%s%s", apiDns, path);
                    if (!HttpUtils.getApiResult(url).isResult()) {
                        // 停止任务失败，下个周期继续处理
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                                "【数据迁移】任务失败：\n" + taskInfo + "\n重启失败：停止任务失败！");
                        continue;
                    }

                    if ((boolean) task.get("overwrite") && !deleteOldData(task)) {
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                                "【数据迁移】任务失败：\n" + taskInfo + "\n重启失败：删除目标数据失败！");
                        continue;
                    }

                    path = startTaskPath.replace("#taskId#", task.get("id").toString());
                    url = String.format("http://%s%s", apiDns, path);
                    if (HttpUtils.getApiResult(url).isResult()) {
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                                "【数据迁移】任务失败：\n" + taskInfo + "\n重启成功！");
                    } else {
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                                "【数据迁移】任务失败：\n" + taskInfo + "\n重新启动失败！");
                    }
                }
            } catch (Exception e) {
                LogUtils.warn(log, "fail to query status of task[{}]: {}", task.get("id"), e);
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, "【数据迁移】检查任务失败：\nrt："
                        + task.get("result_table_id") + ", 任务id：" + task.get("id") + "\n" + e.getMessage());
            }
        }
    }

    /**
     * 处理已完成任务
     *
     * @param taskParentId 总览任务id
     * @param configParallelism 任务并发数
     * @param statusTasksMap 任务列表
     */
    private void processFinishTask(int taskParentId, int configParallelism,
            Map<String, List<Map<String, Object>>> statusTasksMap) {
        ApiResult result;
        int finishCnt = 0;
        List<Map<String, Object>> sinkSuccessTaskList = statusTasksMap.get("sink_success");
        for (Map<String, Object> task : sinkSuccessTaskList) {
            String path = taskStatusPath.replace("#taskId#", task.get("id") + "");
            String url = String.format("http://%s%s", apiDns, path);
            int input = 0;
            int output = 0;
            try {
                result = HttpUtils.getApiResult(url);
                if (result.isResult()) {
                    Map<String, Map<String, Object>> taskStatus = getTaskStatus((Map<String, Object>) result.getData());
                    input = (int) taskStatus.get("source").get("count");
                    output = (int) taskStatus.get("sink").get("count");
                }
            } catch (Exception e) {
                LogUtils.info(log, "failed to get task count! {}", e);
            }

            path = stopTaskPath.replace("#taskId#", task.get("id").toString());
            url = String.format("http://%s%s", apiDns, path);
            try {
                if (HttpUtils.getApiResult(url).isResult()) {
                    finishCnt += 1;
                    // 停止任务失败，下个周期继续处理
                    Map<String, Object> params = new HashMap<>();
                    params.put("task_id", task.get("id"));
                    params.put("status", "finish");
                    params.put("input", input);
                    params.put("output", output);
                    // 更新任务状态。万一失败了，那么就等下次更新了，但是读写数据量就丢失了
                    HttpUtils.postAndCheck(updateTaskUrl, params);
                }
            } catch (Exception e) {
                LogUtils.warn(log, "fail to stop task: {}", e);
            }
        }

        if (finishCnt == sinkSuccessTaskList.size() && statusTasksMap.size() == 1) {
            // 如果任务列表只有sink_success一种状态任务，并且都完成了，那么任务完成
            Map<String, Object> params = new HashMap<>();
            params.put("task_id", taskParentId);
            params.put("status", "finish");
            HttpUtils.postAndCheck(updateTaskUrl, params);
        } else if (statusTasksMap.containsKey("init")) {
            // 还有没启动的子任务，启动它们
            int parallelism = configParallelism - sinkSuccessTaskList.size() + finishCnt;
            startTasks(statusTasksMap.get("init"), parallelism,
                    "http://" + apiDns + startTaskPath + "?parent=" + taskParentId);
        }
    }

    /**
     * 获取任务执行状态，非正常running状态，都返回error
     *
     * @param statusMap pulsar返回的任务状态信息
     * @return 运行状态：running/error
     */
    private Map<String, Map<String, Object>> getTaskStatus(Map<String, Object> statusMap) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        result.put("source", getInstanceStatus((Map<String, Object>) statusMap.get("source"), "source"));
        result.put("sink", getInstanceStatus((Map<String, Object>) statusMap.get("sink"), "sink"));

        return result;
    }

    /**
     * 获取任务执行状态，非正常running状态，都返回error
     *
     * @param statusMap pulsar返回的任务状态信息
     * @return 运行状态：running/error
     */
    private Map<String, Object> getInstanceStatus(Map<String, Object> statusMap, String type) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "error");
        result.put("count", 0);
        if (!statusMap.containsKey("instances")) {
            return result;
        }

        try {
            Map<String, Object> instance = ((List<Map<String, Object>>) statusMap.get("instances")).get(0);
            if (!instance.containsKey("status")) {
                return result;
            }

            Map<String, Object> status = (Map<String, Object>) instance.get("status");
            if (status.containsKey("running")) {
                result.put("status", (boolean) status.get("running") ? "running" : "error");

                // 记住下面数据量需要减1，为什么？因为多了一个任务标志数据，不属于迁移数据内容
                if ("source".equals(type)) {
                    result.put("count", (int) status.get("numReceivedFromSource") - 1);
                } else {
                    result.put("count", (int) status.get("numReadFromPulsar") - 1);
                }
            }
        } catch (Exception ignor) {
            return result;
        }

        return result;
    }

    /**
     * 处理总览任务
     *
     * @param task 任务参数
     * @return 是否处理成功
     */
    private boolean overallTaskProcess(Map<String, Object> task) {
        if (task.get("status").equals("init")) {
            // 如果任务尚未执行，更新状态为running
            Map<String, Object> params = new HashMap<>();
            params.put("task_id", task.get("id"));
            params.put("status", "running");
            return HttpUtils.postAndCheck(updateTaskUrl, params);
        } else if (task.get("status").equals("pause")) {
            // 如果任务处于暂停状态，跳过
            return false;
        }
        return true;
    }

    /**
     * 删除目标存储数据
     *
     * @param task 任务参数
     * @return 是否删除成功
     */
    private boolean deleteOldData(Map<String, Object> task) {
        long startTm = 0;
        try {
            SimpleDateFormat confSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            startTm = confSdf.parse((String) task.get("start")).getTime();
            long endTm = confSdf.parse((String) task.get("end")).getTime();
            Set<String> pathSet = new HashSet<>();
            while (startTm < endTm) {
                SimpleDateFormat pathSdf = new SimpleDateFormat("/yyyy/MM/dd/HH");
                pathSet.add(pathSdf.format(new Date(startTm)));
                startTm += 3600000;
            }

            String type = (String) task.get("dest");
            LogUtils.info(log, "begin to delete old data, {}", pathSet);
            switch (type) {
                case "hdfs":
                    Map<String, Object> t = overallTaskMap.get((String) task.get("task_label"));
                    return HdfsUtils.remove((String) t.get("dest_config"), pathSet);
                default:
                    return true;
            }
        } catch (ParseException e) {
            LogUtils.warn(log, "failed to delete old data", e);
            return false;
        }
    }
}
