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

import com.tencent.bk.base.dataflow.jobnavi.runner.JobNaviLogAgent;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.node.LogAgentInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;

public class LogAgentHeartBeatManager {

    private static final Logger LOGGER = Logger.getLogger(LogAgentHeartBeatManager.class);

    private static Configuration conf;

    private static Thread heartBeatThread;

    /**
     * init log agent heartbeat manager
     *
     * @param config
     */
    public static void init(Configuration config) {
        conf = config;
        int timeout = conf.getInt(Constants.JOBNAVI_LOG_AGENT_HEARTBEAT_INTERNAL_SECOND,
                Constants.JOBNAVI_LOG_AGENT_HEARTBEAT_INTERNAL_SECOND_DEFAULT);
        heartBeatThread = new Thread(new HeartBeatThread(timeout), "heartbeat");
        heartBeatThread.start();
    }

    public static void sendHeartbeat() throws Exception {
        LogAgentInfo info = JobNaviLogAgent.getLogAgentInfo();
        info.setPort(
                conf.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_PORT, Constants.JOBNAVI_RUNNER_TASK_LOG_PORT_DEFAULT));
        try {
            info.setHost((InetAddress.getLocalHost()).getHostName());
        } catch (UnknownHostException e) {
            throw new NaviException(e);
        }
        HAProxy.sendPostRequest("/sys/log_agent_heartbeat", info.toJson(), null);
    }

    public static void stop() {
        heartBeatThread.interrupt();
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
                    sendHeartbeat();
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
}
