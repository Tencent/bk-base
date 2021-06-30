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

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.job.aspect.JobService;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.JmxCollectorService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 采集ignite cache指标。
 */
public class IgniteCacheMetricScheduler implements Job {

    private static final Logger log = LoggerFactory.getLogger(IgniteCacheMetricScheduler.class);
    private static final String filePrefix = "ignite_cache";

    private static final int DEFAULT_THREAD_NUM = 2;
    private final ExecutorService pool = Executors.newFixedThreadPool(DEFAULT_THREAD_NUM, runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName("ignite-cache-jmx-thread-pool");
        thread.setDaemon(true);
        return thread;
    });


    /**
     * 采集ignite cache指标。
     *
     * @param context 作业执行上下文
     * @throws JobExecutionException 作业执行异常
     */
    @JobService(lockPath = "IgniteCacheMetricScheduler", lease = 5, description = "ignite cache指标采集")
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jmxFilePath = DatabusProps.getInstance().getProperty(Consts.JMX_CONF_PATH);
        if (StringUtils.isEmpty(jmxFilePath)) {
            LogUtils.warn(log, "not found ignite cache jmx file");
            return;
        }
        //获取当前目录下 ignite cache jmx文件, 该文件一般以ignite_cache 开头
        List<String> jmxFile = CommUtils.filterFile(jmxFilePath, ".json");
        jmxFile.stream().filter(filePath -> filePath.contains(filePrefix)).forEach(
                filePath -> {
                    Map<Object, Object> clusterMap = JsonUtils.readJsonFileWithoutException(filePath);
                    String clusterName = (String) clusterMap.get("clusterName");
                    String clusterType = (String) clusterMap.get("clusterType");
                    List<String> jmxAddress = (List<String>) clusterMap.get("address");
                    List<Object> beanConf = (List<Object>) clusterMap.get("conf");
                    pool.execute(new JmxCollectorService(clusterName, clusterType, jmxAddress, beanConf));
                }
        );
    }
}
