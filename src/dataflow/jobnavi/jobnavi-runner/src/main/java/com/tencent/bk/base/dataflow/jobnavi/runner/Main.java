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

package com.tencent.bk.base.dataflow.jobnavi.runner;

import com.tencent.bk.base.dataflow.jobnavi.runner.heartbeat.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.HeartBeatService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.RPCService;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.runner.event.DecommissionTaskEventListener;
import com.tencent.bk.base.dataflow.jobnavi.runner.event.EventListenerFactory;
import com.tencent.bk.base.dataflow.jobnavi.runner.event.KillTaskEventListener;
import com.tencent.bk.base.dataflow.jobnavi.runner.event.RollbackDecommissionTaskEventListener;
import com.tencent.bk.base.dataflow.jobnavi.runner.event.RunningTaskEventListener;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.CleanExpireDataService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.HttpServerService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAggregationService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.TaskManagerService;
import com.tencent.bk.base.dataflow.jobnavi.service.ServiceManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class);
    private static final RunnerInfo runnerInfo = new RunnerInfo();
    private static Configuration conf;

    static {
        ServiceManager.registerService(new CleanExpireDataService());
        ServiceManager.registerService(new HeartBeatService());
        ServiceManager.registerService(new TaskManagerService());
        ServiceManager.registerService(new LogAggregationService());
        ServiceManager.registerService(new HttpServerService());
        ServiceManager.registerService(new RPCService());

        EventListenerFactory.addEventListener("running", new RunningTaskEventListener());
        EventListenerFactory.addEventListener("kill", new KillTaskEventListener());
        EventListenerFactory.addEventListener("sys_decommission", new DecommissionTaskEventListener());
        EventListenerFactory.addEventListener("sys_decommission_rollback", new RollbackDecommissionTaskEventListener());
        runnerInfo.setStatus(RunnerStatus.running);
    }

    public static RunnerInfo getRunnerInfo() {
        return runnerInfo;
    }

    public static Configuration getConfig() {
        return conf;
    }

    /**
     * main
     *
     * @param args args
     */
    public static void main(String[] args) {
        try {
            LOGGER.info("/***********************************************/");
            LOGGER.info("/**********jobnavi Runner Starting**************/");
            LOGGER.info("/***********************************************/");
            conf = new Configuration(true);
            conf.printProperties();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    LOGGER.info("Receive stop signal. Stopping...");
                    try {
                        LOGGER.info("stop heartbeat...");
                        HeartBeatManager.stop();
                        runnerInfo.setStatus(RunnerStatus.lost);
                        LOGGER.info("send remove runner request...");
                        String host = getRunnerInfo().getRunnerId();
                        HeartBeatManager.sendHeartbeat(true);
                        HAProxy.sendGetRequest("/sys/runner?operate=stop_runner&host=" + host, 4);
                    } catch (Throwable e) {
                        LOGGER.error("send remove runner request error:", e);
                    }
                    try {
                        ServiceManager.stopAllService(conf);
                    } catch (Throwable e) {
                        LOGGER.error("Stop Runner error:", e);
                        System.exit(1);
                    }
                }
            });
            HAProxy.init(conf);
            ServiceManager.startAllService(conf);

        } catch (Throwable e) {
            LOGGER.error("Start Runner error:", e);
            System.exit(1);
        }
    }
}
