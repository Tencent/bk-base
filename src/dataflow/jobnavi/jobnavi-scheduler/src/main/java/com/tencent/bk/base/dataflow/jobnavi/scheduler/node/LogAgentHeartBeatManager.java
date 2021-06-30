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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.node;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.LogAgentInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class LogAgentHeartBeatManager {

    private static final Logger LOGGER = Logger.getLogger(LogAgentHeartBeatManager.class);

    private static Map<String, LogAgentInfo> agentInfos;
    private static Map<String, Long> updateTimes;
    private static Thread heartBeatThread;

    /**
     * init log agent heartbeat manager
     *
     * @param conf
     */
    public static void start(Configuration conf) throws NaviException {
        int expireTime = conf.getInt(Constants.JOBNAVI_SCHEDULER_HEARTBEAT_LOG_AGENT_EXPIRE_SECOND,
                Constants.JOBNAVI_SCHEDULER_HEARTBEAT_LOG_AGENT_EXPIRE_SECOND_DEFAULT);
        agentInfos = new ConcurrentHashMap<>();
        updateTimes = new ConcurrentHashMap<>();
        heartBeatThread = new Thread(new HeartBeatThread(expireTime), "log-agent-heartbeat");
        heartBeatThread.start();
    }

    /**
     * stop log agent heartbeat manager
     */
    public static void stop() {
        if (agentInfos != null) {
            agentInfos.clear();
        }
        if (updateTimes != null) {
            updateTimes.clear();
        }
        if (heartBeatThread != null) {
            heartBeatThread.interrupt();
        }
    }

    /**
     * receive heartbeat from loa agent
     *
     * @param info
     */
    public static void receiveHeartBeat(LogAgentInfo info) {
        if (info.getHost() == null) {
            LOGGER.error("host of log agent is null, ignored");
            return;
        }
        if (!agentInfos.containsKey(info.getHost())) {
            LOGGER.info("add new log agent: " + info.getHost());
        }
        agentInfos.put(info.getHost(), info);
        updateTimes.put(info.getHost(), System.currentTimeMillis());
    }

    public static void removeNode(String host) {
        agentInfos.remove(host);
        updateTimes.remove(host);
    }

    public static Integer getPort(String host) {
        return !(host == null) && agentInfos.containsKey(host) ? agentInfos.get(host).getPort() : null;
    }

    public static Map<String, LogAgentInfo> getAgentInfos() {
        return agentInfos;
    }

    static class HeartBeatThread implements Runnable {

        int expireTime;

        HeartBeatThread(int expireTime) {
            this.expireTime = expireTime;
        }

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long current = System.currentTimeMillis();
                    for (Map.Entry<String, Long> entry : updateTimes.entrySet()) {
                        if (current - entry.getValue() > (long) expireTime * 1000) {
                            LOGGER.warn("Node " + entry.getKey() + " expired, remove.");
                            removeNode(entry.getKey());
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error("HeartBeat error.", e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("Log agent heartBeat thread interrupted.");
                    break;
                }
            }

        }
    }


}
