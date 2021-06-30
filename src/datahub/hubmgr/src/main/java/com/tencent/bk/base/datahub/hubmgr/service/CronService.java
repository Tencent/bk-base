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

package com.tencent.bk.base.datahub.hubmgr.service;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CronService implements Service {

    private static final Logger log = LoggerFactory.getLogger(CronService.class);
    private static final String DEFAULT_PACKAGE = "com.tencent.bk.base.datahub.hubmgr.job.";

    private final SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    private Scheduler scheduler = null;

    /**
     * 构造函数，给反射机制使用
     */
    public CronService() {
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public String getServiceName() {
        return MgrConsts.CRON_SERVICE;
    }

    /**
     * 启动服务
     */
    public void start() throws Exception {
        LogUtils.info(log, "going to start cron service!");
        if (scheduler == null) {
            scheduler = schedulerFactory.getScheduler();
        }

        DatabusProps props = DatabusProps.getInstance();
        // 获取配置的定时任务列表
        String[] jobArr = props.getArrayProperty(MgrConsts.DATABUSMGR_CRON_JOBS, "|");
        String[] triggerArr = props.getArrayProperty(MgrConsts.DATABUSMGR_CRON_TRIGGERS, "|");

        // 检查配置项是否匹配
        if (jobArr.length != triggerArr.length) {
            String msg = String.format("bad config for cron service. %s and %s doesn't match in count", "", "");
            LogUtils.error(MgrConsts.ERRCODE_BAD_CONFIG, log, msg);
            throw new Exception(msg);

        } else {
            // 逐个作业注册
            for (int i = 0; i < jobArr.length; i++) {
                String job = jobArr[i].trim();
                String trigger = triggerArr[i].trim();
                String className = DEFAULT_PACKAGE + job;

                try {
                    Class jobClass = Class.forName(className);
                    if (Job.class.isAssignableFrom(jobClass)) {
                        JobDetail jd = JobBuilder.newJob(jobClass).build();
                        LogUtils.info(log, "going to schedule job {} with {}", job, trigger);
                        CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                                .withIdentity(job + "_trigger")
                                .withSchedule(CronScheduleBuilder.cronSchedule(trigger))
                                .startNow()
                                .build();
                        scheduler.scheduleJob(jd, cronTrigger);
                    } else {
                        LogUtils.warn(log, "job class {} is invalid, unable to create job task", className);
                    }

                } catch (ClassNotFoundException ignore) {
                    LogUtils.warn(log, "unable to schedule job as class {} not found", className);
                }

            }
        }

        // 启动任务scheduler
        scheduler.start();
    }

    /**
     * 停止服务
     */
    public void stop() throws Exception {
        LogUtils.info(log, "going to stop cron service!");
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
}
