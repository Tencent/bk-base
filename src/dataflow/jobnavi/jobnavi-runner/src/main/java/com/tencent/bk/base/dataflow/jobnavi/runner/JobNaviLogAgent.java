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

import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAgentHeartBeatService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAggregationService;
import com.tencent.bk.base.dataflow.jobnavi.runner.service.TaskLogService;
import com.tencent.bk.base.dataflow.jobnavi.runner.heartbeat.LogAgentHeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.node.LogAgentInfo;
import com.tencent.bk.base.dataflow.jobnavi.service.ServiceManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.net.InetAddress;
import org.apache.log4j.Logger;

public class JobNaviLogAgent {

    private static final Logger LOGGER = Logger.getLogger(JobNaviLogAgent.class);
    private static final LogAgentInfo logAgentInfo = new LogAgentInfo();
    private static Configuration conf;

    static {
        ServiceManager.registerService(new TaskLogService());
        ServiceManager.registerService(new LogAgentHeartBeatService());
        ServiceManager.registerService(new LogAggregationService());
    }

    public static LogAgentInfo getLogAgentInfo() {
        return logAgentInfo;
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
            LOGGER.info("/**********jobnavi log agent Starting**************/");
            LOGGER.info("/***********************************************/");
            conf = new Configuration("jobnavi-log-agent.properties");
            conf.printProperties();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    LOGGER.info("Receive stop signal. Stopping...");
                    try {
                        LOGGER.info("stop heartbeat...");
                        LogAgentHeartBeatManager.stop();
                        LOGGER.info("send remove log agent request...");
                        String host = (InetAddress.getLocalHost()).getHostName();
                        LogAgentHeartBeatManager.sendHeartbeat();
                        HAProxy.sendGetRequest("/sys/log_agent?operate=stop_log_agent&host=" + host, 4);
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
            LOGGER.error("Start log agent error:", e);
            System.exit(1);
        }
    }
}
