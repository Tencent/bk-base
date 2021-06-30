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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.ha;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.service.ServiceManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

public class LeaderClient implements LeaderLatchListener {

    private static final Logger LOGGER = Logger.getLogger(LeaderClient.class);

    private static final String ZK_ELECTION_ROOT = "/election";
    private final Configuration conf;
    private LeaderLatch leaderLatch;
    private CuratorFramework client;


    public LeaderClient(Configuration conf) {
        this.conf = conf;
        LOGGER.info("Start Zookeeper LeaderLatch... ");
    }

    public void startClient() throws Exception {
        startClient(conf);
    }

    private void startClient(Configuration conf) throws Exception {
        String zkUrl = conf.getString(Constants.JOBNAVI_HA_ZK_URL);
        int sessionTimeout = conf.getInt(Constants.JOBNAVI_HA_ZK_SESSION_TIMEOUT_MILLIS,
                                         Constants.JOBNAVI_HA_ZK_SESSION_TIMEOUT_MILLIS_DEFAULT);
        int connectionTimeout = conf.getInt(Constants.JOBNAVI_HA_ZK_CONNECTION_TIMEOUT_MILLIS,
                                            Constants.JOBNAVI_HA_ZK_CONNECTION_TIMEOUT_MILLIS_DEFAULT);
        int retryWaitMillis = conf
                .getInt(Constants.JOBNAVI_HA_ZK_RETRY_WAIT_MILLIS, Constants.JOBNAVI_HA_ZK_RETRY_WAIT_MILLIS_DEFAULT);
        int maxReconnectAttempts = conf.getInt(Constants.JOBNAVI_HA_ZK_MAX_RECONNECT_ATTEMPTS,
                                               Constants.JOBNAVI_HA_ZK_MAX_RECONNECT_ATTEMPTS_DEFAULT);
        this.client = CuratorFrameworkFactory
                .newClient(zkUrl,
                           sessionTimeout,
                           connectionTimeout,
                           new ExponentialBackoffRetry(retryWaitMillis, maxReconnectAttempts));
        client.start();
        String rootUrl = conf.getString(Constants.JOBNAVI_HA_ZK_PATH, Constants.JOBNAVI_HA_ZK_PATH_DEFAULT);
        String electionUrl = rootUrl + ZK_ELECTION_ROOT;
        leaderLatch = new LeaderLatch(client, electionUrl);
        leaderLatch.addListener(this);
        leaderLatch.start();
    }

    public void stopClient() {
        if (leaderLatch != null && leaderLatch.getState() == LeaderLatch.State.STARTED) {
            try {
                leaderLatch.close();
            } catch (IOException e) {
                LOGGER.error("close zkClient error", e);
            }
        }

        if (client != null) {
            client.close();
        }
    }

    @Override
    public void isLeader() {
        synchronized (this) {
            // could have lost leadership by now.
            if (!leaderLatch.hasLeadership()) {
                return;
            }
            LOGGER.info("We have gained leadership");
            updateLeadershipStatus(true);
        }
    }

    @Override
    public void notLeader() {
        synchronized (this) {
            // could have gained leadership by now.
            if (leaderLatch.hasLeadership()) {
                return;
            }
            LOGGER.info("We have lost leadership");
            updateLeadershipStatus(false);
        }
    }

    private void updateLeadershipStatus(boolean isLeader) {
        if (isLeader) {
            try {
                ServiceManager.startAllService(conf);
                ServiceManager.recoveryAllService(conf);
            } catch (Exception e) {
                LOGGER.error("Start Service error:", e);
                stopClient();
                System.exit(1);
            }
        } else {
            try {
                ServiceManager.stopAllService(conf);
            } catch (Exception e) {
                LOGGER.error("Stop Service error:", e);
                stopClient();
                System.exit(1);
            }
        }
        ZooKeeperLeaderElectionAgent.updateStatus(isLeader);
    }

    public void restartClient() throws Exception {
        try {
            this.leaderLatch.close();
            this.client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        startClient(conf);
    }
}
