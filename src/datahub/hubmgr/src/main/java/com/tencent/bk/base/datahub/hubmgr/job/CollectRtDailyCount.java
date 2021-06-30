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
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_RT_STORAGES;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_RT_STORAGES_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DAILY_COUNT_BIZ_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DAILY_COUNT_COMPONENT_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DAILY_COUNT_PROJECT_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DAILY_COUNT_RT_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DAILY_STAT_DELTA_DAYS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.MONITOR_METRIC_DB;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.MONITOR_METRIC_DB_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ADMIN;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ApiException;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeLock;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CollectRtDailyCount implements Job {

    private static final Logger log = LoggerFactory.getLogger(CollectRtDailyCount.class);
    private static final String SQL_TEMPLATE = "SELECT SUM(data_inc) AS cnt FROM data_loss_output_total "
            + "WHERE logical_tag='%s' and time >= %ss and time < %ss GROUP BY component, bk_biz_id, project_id";
    private static final long LOCK_SEC = 5;
    private static final int PAGE_SIZE = 2000;

    private Map<String, Long> bizToCount = null;
    private Map<String, Long> projectToCount = null;
    private Map<String, Long> componentToCount = null;
    private String receivers;
    private String apiDns;
    private String path;
    private String metricDb;
    private int delta;


    /**
     * 查询rt每天的数据量，将此结果数据写入指定的统计表中。
     *
     * @param context 作业执行上下文
     * @throws JobExecutionException 作业执行异常
     */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        initVariables();
        long triggerTime = context.getFireTime().getTime();
        LogUtils.info(log, "collect result table daily count job triggered at {}", triggerTime);

        // 排他性任务，只能在一个实例上被触发，这里首先通过zk获取锁
        String lockPath = String.format("/databusmgr/lock/%s", CollectRtDailyCount.class.getSimpleName());
        try (DistributeLock lock = new DistributeLock(lockPath)) {
            if (lock.lock(LOCK_SEC)) {
                Set<String> rtIds = getAllRtSet();
                // 逐个对processingId获取昨天的统计数据
                long endTs = CommUtils.getTodayStartTs();
                endTs = endTs - delta * 24 * 3600;
                long startTs = endTs - 24 * 3600; // 前一天的起始时间戳
                String statDay = CommUtils.getDate(startTs * 1000);
                for (String rtId : rtIds) {
                    String sql = String.format(SQL_TEMPLATE, rtId, startTs, endTs);
                    ApiResult metrics = CommUtils.queryMetrics(metricDb, sql);
                    if (metrics.isResult()) {
                        Map<String, Object> data = (Map<String, Object>) metrics.getData();
                        parseMetric(rtId, statDay, data);
                    } else {
                        LogUtils.warn(log, "query monitor metric failed. {}",
                                JsonUtils.toJsonWithoutException(metrics));
                    }
                }

                reportStatData(statDay, startTs);
                LogUtils.info(log, "finish get daily count job");

                // 确保占用锁的时间超过LOCK_SEC的时间，避免释放锁太快导致其他进程里的job获取到锁，重复执行
                long duration = System.currentTimeMillis() - triggerTime;
                if (duration < (10 + LOCK_SEC) * 1000) {
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
            LogUtils.warn(log, "failed to run collect result table daily count job!", e);
            // 集群状态检查失败时，需要通知管理员
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                    "采集rt每天数据量失败 " + ExceptionUtils.getStackTrace(e));
            throw new JobExecutionException(e);
        }

    }

    /**
     * 初始化必要的变量
     */
    private void initVariables() {
        bizToCount = new HashMap<>();
        projectToCount = new HashMap<>();
        componentToCount = new HashMap<>();

        DatabusProps props = DatabusProps.getInstance();
        receivers = props.getOrDefault(STOREKIT_ADMIN, "");
        // 获取总线存储的信息列表，然后从中获取所有rt的列表
        apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        path = props.getOrDefault(API_RT_STORAGES, API_RT_STORAGES_DEFAULT);
        // 按照配置项计算偏移天数对应的时间戳
        delta = props.getOrDefault(DAILY_STAT_DELTA_DAYS, 0);
        metricDb = props.getOrDefault(MONITOR_METRIC_DB, MONITOR_METRIC_DB_DEFAULT);
    }

    /**
     * 获取所有的rtId，放入集合中返回
     *
     * @return rtId的集合
     * @throws ApiException api请求异常
     */
    private Set<String> getAllRtSet() throws ApiException {
        // 逐页获取数据，直到所有数据均获取到
        Set<String> rtIds = new HashSet<>(PAGE_SIZE);
        for (int page = 1; ; page++) {
            String restUrl = String.format("http://%s%s?limit=%s&page=%s", apiDns, path, PAGE_SIZE, page);
            ApiResult result = HttpUtils.getApiResult(restUrl);
            if (result.isResult()) {
                List<Map<String, Object>> data = (List<Map<String, Object>>) result.getData();
                data.forEach(rtStorage -> rtIds.add(rtStorage.get("result_table_id").toString()));

                if (data.size() == 0) {
                    break;  // 所有数据已获取
                }
            } else {
                LogUtils.warn(log, "failed to get rt storage list {}", JsonUtils.toJsonWithoutException(result));
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                        "获取RT存储列表失败 " + JsonUtils.toJsonWithoutException(result));
                break;
            }
        }
        LogUtils.info(log, "going to get daily count for {} rts.", rtIds.size());

        return rtIds;
    }

    /**
     * 上报采集的统计信息
     *
     * @param statDay 统计日期
     * @param startTs 统计时间戳
     */
    private void reportStatData(String statDay, long startTs) {
        LogUtils.info(log, "collected stats: {}   {}   {}", bizToCount, projectToCount, componentToCount);
        // 按照project和业务ID维度上报
        for (Map.Entry<String, Long> entry : bizToCount.entrySet()) {
            TsdbWriter.getInstance().reportData(DAILY_COUNT_BIZ_DEFAULT,
                    String.format("bk_biz_id=%s,thedate=%s", entry.getKey(), statDay),
                    String.format("daily_count=%si", entry.getValue()), startTs);
        }
        for (Map.Entry<String, Long> entry : projectToCount.entrySet()) {
            TsdbWriter.getInstance().reportData(DAILY_COUNT_PROJECT_DEFAULT,
                    String.format("project_id=%s,thedate=%s", entry.getKey(), statDay),
                    String.format("daily_count=%si", entry.getValue()), startTs);
        }
        for (Map.Entry<String, Long> entry : componentToCount.entrySet()) {
            TsdbWriter.getInstance().reportData(DAILY_COUNT_COMPONENT_DEFAULT,
                    String.format("component=%s,thedate=%s", entry.getKey(), statDay),
                    String.format("daily_count=%si", entry.getValue()), startTs);
        }
    }

    /**
     * 解析查询到的打点数据，将结果发送到tsdb中
     *
     * @param rtId 结果表id
     * @param date 统计数据日期
     * @param data 查询到的打点数据
     */
    private void parseMetric(String rtId, String date, Map<String, Object> data) {
        String dataFormat = data.get("data_format").toString();
        List<Map<String, Object>> series = (List<Map<String, Object>>) data.get("series");
        if ("simple".equals(dataFormat)) {
            for (Map<String, Object> entry : series) {
                parseMetricEntry(rtId, date, entry);
            }
        } else if ("series".equals(dataFormat)) {
            for (Map<String, Object> map : series) {
                // 复合结构，获取values里的数组，逐个解析处理数据
                List<Map<String, Object>> values = (List<Map<String, Object>>) map.get("values");
                for (Map<String, Object> entry : values) {
                    parseMetricEntry(rtId, date, entry);
                }
            }
        } else {
            LogUtils.warn(log, "bad data for metrics: {}", JsonUtils.toJsonWithoutException(data));
        }
    }

    /**
     * 解析单条打点数据，将结果发送到tsdb中
     *
     * @param rtId 结果表id
     * @param date 统计数据日期
     * @param entry 单条打点数据
     */
    private void parseMetricEntry(String rtId, String date, Map<String, Object> entry) {
        String bizId = entry.get("bk_biz_id").toString();
        if (StringUtils.isBlank(bizId) || "None".equals(bizId)) {
            bizId = "-1";
        }
        String projectId = entry.get("project_id").toString();
        if (StringUtils.isBlank(projectId) || "None".equals(projectId)) {
            projectId = "-1";
        }
        if ("-1".equals(bizId) || "-1".equals(projectId)) {
            LogUtils.warn(log, "bad bizId or projectId: {} {} {}", rtId, date, entry);
        }
        long cnt = ((Number) entry.get("cnt")).longValue();
        String component = entry.get("component").toString();
        String tagStr = String.format("rt_id=%s,bk_biz_id=%s,project_id=%s,component=%s,thedate=%s",
                rtId, bizId, projectId, component, date);
        String fieldStr = String.format("daily_count=%si", cnt);
        LogUtils.debug(log, "reporting {} {} {}", tagStr, fieldStr, ((Number) entry.get("time")).longValue());
        TsdbWriter.getInstance().reportData(DAILY_COUNT_RT_DEFAULT, tagStr, fieldStr,
                ((Number) entry.get("time")).longValue());

        // 更新按照业务id和项目id维度的统计数据
        bizToCount.put(bizId, cnt + bizToCount.getOrDefault(bizId, 0L));
        projectToCount.put(projectId, cnt + projectToCount.getOrDefault(projectId, 0L));
        componentToCount.put(component, cnt + componentToCount.getOrDefault(component, 0L));
    }
}

