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
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class NodeBlackListManager {

    private static final Logger LOGGER = Logger.getLogger(NodeBlackListManager.class);
    private static final Map<String, Integer> nodeFailedAmountMap = new ConcurrentHashMap<>();
    private static final Map<String, Long> blackListMap = new ConcurrentHashMap<>();
    private static final Object blackListMapLock = new Object();
    private static int failedThreshold = Integer.MAX_VALUE;

    /**
     * init node black list manager
     *
     * @param conf
     */
    public static void init(Configuration conf) {
        failedThreshold = conf.getInt(Constants.JOBNAVI_SCHEDULER_BLACKLIST_FAILED_THRESHOLD,
                Constants.JOBNAVI_SCHEDULER_BLACKLIST_FAILED_THRESHOLD_DEFAULT);
        Thread nodeBlackListExpireThread = new Thread(new NodeBlackListExpireThread(conf), "blackListExpire-Thread");
        nodeBlackListExpireThread.start();
    }

    /**
     * increase host fail count
     *
     * @param host
     */
    public static void plusFail(String host) {
        synchronized (blackListMapLock) {
            if (blackListMap.containsKey(host)) {
                LOGGER.info("host " + host + " is in the blacklist.");
                return;
            }

            Integer currentValue = nodeFailedAmountMap.get(host);
            int failedAmount;
            if (currentValue == null) {
                failedAmount = 1;
            } else {
                failedAmount = currentValue + 1;
            }
            nodeFailedAmountMap.put(host, failedAmount);
            LOGGER.info("plus host [" + host + "] fail amount. current failed amount is " + failedAmount);
            if (failedAmount >= failedThreshold) {
                LOGGER.info("set host " + host + " in the blacklist.");
                blackListMap.put(host, System.currentTimeMillis());
            }
        }
    }

    public static void healthy(String host) {
        synchronized (blackListMapLock) {
            blackListMap.remove(host);
        }
    }

    public static boolean isBlackNode(String host) {
        synchronized (blackListMapLock) {
            return blackListMap.containsKey(host);
        }
    }

    static class NodeBlackListExpireThread implements Runnable {

        private final int expireTimeMills;

        NodeBlackListExpireThread(Configuration conf) {
            this.expireTimeMills = conf.getInt(Constants.JOBNAVI_SCHEDULER_BLACKLIST_EXPIRE_MILLS,
                    Constants.JOBNAVI_SCHEDULER_BLACKLIST_EXPIRE_MILLS_DEFAULT);
        }

        @Override
        public void run() {
            while (true) {
                synchronized (blackListMapLock) {
                    long now = System.currentTimeMillis();
                    Iterator<Map.Entry<String, Long>> iterator = blackListMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Long> entry = iterator.next();
                        String host = entry.getKey();
                        if (now - entry.getValue() > expireTimeMills) {
                            LOGGER.info("host " + host + " blacklist expire, set host enable.");
                            iterator.remove();
                            nodeFailedAmountMap.remove(host);
                        }
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.error(Thread.currentThread().getName() + " thread down.", e);
                    break;
                }
            }
        }
    }

}
