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
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_CAPACITY_WARN_LIMIT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_CLUSTER_TYPE_LIST;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.job.aspect.JobService;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterCapacityCheck implements Job {

    private static final Logger log = LoggerFactory.getLogger(ClusterCapacityCheck.class);

    /**
     * 检查所有的存储容量信息，触发阀值告警。
     *
     * @param context 作业执行上下文
     * @throws JobExecutionException 作业执行异常
     */
    @JobService(lockPath = "ClusterCapacityCheck", lease = 5, description = "存储集群容量巡检")
    public void execute(JobExecutionContext context) throws JobExecutionException {
        DatabusProps props = DatabusProps.getInstance();
        String apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        String[] storageTypeList = props.getArrayProperty(STOREKIT_CLUSTER_TYPE_LIST, ",");

        String receivers = DatabusProps.getInstance().getOrDefault(STOREKIT_ADMIN, "");
        String threshold = DatabusProps.getInstance().getOrDefault(STOREKIT_CAPACITY_WARN_LIMIT, "0");

        // 存在配置存储集群
        if (storageTypeList.length > 0) {
            for (String storageType : storageTypeList) {
                String restUrl = String.format("http://%s/v3/storekit/capacities/%s/", apiDns, storageType);
                try {
                    ApiResult result = HttpUtils.getApiResult(restUrl);
                    if (result.isResult()) {
                        //请求成功,检查集群容量
                        Map<String, Object> capacitySeriesMap = (Map<String, Object>) result.getData();
                        checkCapacity(capacitySeriesMap, receivers, threshold, storageType);
                    } else {
                        String msg = JsonUtils.toJsonWithoutException(result);
                        LogUtils.warn(log, "failed to query capacities by storekit url, bad response! {}", msg);
                        throw new JobExecutionException(
                                String.format("failed to query storekit capacities, message: %s", msg));
                    }
                } catch (Exception e) {
                    LogUtils.warn(log, "failed to query capacities by storekit url, exception! {}", e.getMessage());
                    throw new JobExecutionException(e);
                }
            }
        }
    }

    /**
     * 检查集群容量
     *
     * @param capacitySeriesMap 集群容量集合
     * @param receivers 告警接收人
     * @param threshold 阀值
     * @param storageType 存储类型
     * @throws JobExecutionException JobExecutionException
     */
    private void checkCapacity(Map<String, Object> capacitySeriesMap, String receivers, String threshold,
            String storageType) throws JobExecutionException {
        HashMap<String, Boolean> hasCheckMap = new HashMap<>();
        if (capacitySeriesMap != null && capacitySeriesMap.size() > 0) {
            ArrayList<Map<String, Object>> capacityArray = (ArrayList<Map<String, Object>>) capacitySeriesMap
                    .get("series");
            for (Map<String, Object> capacityMap : capacityArray) {
                String clusterName = (String) capacityMap.get("cluster_name");
                if (hasCheckMap.get(clusterName) == null) {
                    int priority = (Integer) capacityMap.get("Priority");
                    if (priority < Integer.valueOf(threshold)) {
                        String msg = String.format("存储集群%s(%s)当前剩余容量占比%s%%，低于阈值%s%%，容量相关信息：%s %n %s", clusterName,
                                storageType, priority, threshold, removeValueNull(capacityMap),
                                CommUtils.getDatetime());
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, msg);
                    }
                    hasCheckMap.put(clusterName, Boolean.TRUE);
                }
            }
        } else {
            LogUtils.warn(log, "failed to query capacities by storekit url, response is empty");
            throw new JobExecutionException("failed to query capacities by storekit url, response is empty");
        }
    }

    /**
     * 去除值为null的字符串
     *
     * @param map map
     * @return 去除null的map
     */
    private String removeValueNull(Map<String, Object> map) {
        for (Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Object> item = it.next();
            Object val = item.getValue();
            if (null == val) {
                it.remove();
            }
        }
        return JsonUtils.toPrettyJsonWithoutException(map);
    }
}
