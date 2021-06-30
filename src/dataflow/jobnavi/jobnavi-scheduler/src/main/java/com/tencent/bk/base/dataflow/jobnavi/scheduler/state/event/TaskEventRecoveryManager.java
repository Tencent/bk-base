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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.RecoveryEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class TaskEventRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(TaskEventRecoveryManager.class);
    private static final Map<Long, RecoveryEventInfo> recoveryEventInfos = new ConcurrentHashMap<>();
    private static Thread eventRecoveryThread;

    /**
     * start task event recovery manager
     */
    public static void start() {
        eventRecoveryThread = new Thread(new EventRecoveryThread(), "event-recovery-thread");
        eventRecoveryThread.start();
    }

    /**
     * stop task event recovery manager
     */
    public static void stop() {
        if (eventRecoveryThread != null) {
            eventRecoveryThread.interrupt();
        }
    }

    /**
     * recover event after given delay
     *
     * @param eventInfo
     * @param delayMills
     * @throws NaviException
     */
    public static synchronized void recoveryEvent(TaskEventInfo eventInfo, long delayMills) {
        LOGGER.info("add recovery event: " + eventInfo.getTaskEvent().toJson() + " id is:" + eventInfo.getId()
                + " delay is :" + delayMills);
        RecoveryEventInfo recoveryEventInfo = new RecoveryEventInfo();
        long now = System.currentTimeMillis();
        recoveryEventInfo.setRecoveryTimeMills(now);
        recoveryEventInfo.setEventInfo(eventInfo);
        recoveryEventInfo.setDelayMills(delayMills);
        recoveryEventInfos.put(eventInfo.getId(), recoveryEventInfo);
    }

    static class EventRecoveryThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long now = System.currentTimeMillis();
                    Iterator<Map.Entry<Long, RecoveryEventInfo>> iterator = recoveryEventInfos.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, RecoveryEventInfo> entry = iterator.next();
                        RecoveryEventInfo recoveryEventInfo = entry.getValue();
                        if (recoveryEventInfo != null
                                && recoveryEventInfo.getRecoveryTimeMills() + recoveryEventInfo.getDelayMills() < now) {
                            TaskStateManager.recoverTaskEventInfo(recoveryEventInfo.getEventInfo());
                            iterator.remove();
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("event recovery thread interrupt.");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("event recovery error.", e);
                }
            }
        }
    }

}
