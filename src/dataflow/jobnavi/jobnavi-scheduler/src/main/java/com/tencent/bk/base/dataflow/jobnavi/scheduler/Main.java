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

package com.tencent.bk.base.dataflow.jobnavi.scheduler;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.ha.ZooKeeperLeaderElectionAgent;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.CleanExpireDataService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.HeartBeatService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.HttpServerService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.LogCleanService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.MetaDataService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.NodeBlackListService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.NodeLabelService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.RunnerTaskRecoveryService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.RunningTaskCounterService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.ScheduleService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.TaskEventBufferService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.TaskEventRecoveryService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.TaskManagerService;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.TaskRecoveryService;
import com.tencent.bk.base.dataflow.jobnavi.service.ServiceManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    static {
        ServiceManager.registerService(new MetaDataService());
        ServiceManager.registerService(new NodeLabelService());
        ServiceManager.registerService(new HeartBeatService());
        ServiceManager.registerService(new TaskEventRecoveryService());
        ServiceManager.registerService(new TaskEventBufferService());
        ServiceManager.registerService(new RunningTaskCounterService());
        ServiceManager.registerService(new TaskManagerService());
        ServiceManager.registerService(new ScheduleService());
        ServiceManager.registerService(new TaskRecoveryService());
        ServiceManager.registerService(new NodeBlackListService());
        ServiceManager.registerService(new CleanExpireDataService());
        ServiceManager.registerService(new LogCleanService());
        ServiceManager.registerService(new HttpServerService());
        ServiceManager.registerService(new RunnerTaskRecoveryService());
    }

    /**
     * main method
     *
     * @param args args
     */
    public static void main(String[] args) {
        try {
            logger.info("/***********************************************/");
            logger.info("/*********jobnavi Scheduler Starting************/");
            logger.info("/***********************************************/");
            final Configuration conf = new Configuration(true);
            conf.printProperties();
            final ZooKeeperLeaderElectionAgent agent = new ZooKeeperLeaderElectionAgent();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("Receive stop signal. Stoping...");
                        logger.info("stop zk agent...");
                        agent.stop();
                        ServiceManager.stopAllService(conf);
                    } catch (Throwable e) {
                        logger.error("Stop Scheduler error:", e);
                        System.exit(1);
                    }
                }
            });
            agent.start(conf);
            Thread.currentThread().join();
        } catch (Throwable e) {
            logger.error("Start Scheduler error:", e);
            System.exit(1);
        }
    }
}
