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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;

public class TaskExecuteEventBuffer {

    private static final Logger LOGGER = Logger.getLogger(TaskExecuteEventBuffer.class);
    private static final Object bufferLock = new Object();
    //key:execute ID -> value:queue<process task event>
    private static Map<Long, PriorityBlockingQueue<TaskEventInfo>> waitTillExecuteUnlockedBuffer;
    //key:execute ID -> value:last update timestamp in ms
    private static Map<Long, Long> bufferLastUpdateTime;
    private static Thread clearBufferThread;

    public static void init(Configuration conf) {
        waitTillExecuteUnlockedBuffer = new ConcurrentHashMap<>();
        bufferLastUpdateTime = new ConcurrentHashMap<>();
        int bufferExpireSeconds = conf.getInt(
                Constants.JOBNAVI_SCHEDULER_TASK_EXECUTE_EVENT_BUFFER_EXPIRE_SECOND,
                Constants.JOBNAVI_SCHEDULER_TASK_EXECUTE_EVENT_BUFFER_EXPIRE_SECOND_DEFAULT);
        clearBufferThread = new Thread(new ClearBufferThread(bufferExpireSeconds), "clear-execute-event-buffer-thread");
        clearBufferThread.start();
    }

    public static void destroy() {
        if (clearBufferThread != null) {
            clearBufferThread.interrupt();
        }
        synchronized (bufferLock) {
            if (waitTillExecuteUnlockedBuffer != null) {
                waitTillExecuteUnlockedBuffer.clear();
            }
        }
        if (bufferLastUpdateTime != null) {
            bufferLastUpdateTime.clear();
        }
    }

    /**
     * add task execute event to corresponding buffer and wait till corresponding execute is unlocked
     *
     * @param executeId execute ID of given task
     * @param taskEventInfo task event info
     */
    public static void waitTillExecuteUnlocked(long executeId, TaskEventInfo taskEventInfo) {
        synchronized (bufferLock) {
            if (!waitTillExecuteUnlockedBuffer.containsKey(executeId)) {
                //wait till execute unlocked by other event
                waitTillExecuteUnlockedBuffer.put(executeId, new PriorityBlockingQueue<TaskEventInfo>());
            }
            waitTillExecuteUnlockedBuffer.get(executeId).add(taskEventInfo);
            bufferLastUpdateTime.put(executeId, System.currentTimeMillis());
        }
    }

    /**
     * notify corresponding buffer the given execute is unlocked and recover task execute event to global task event
     * queue
     *
     * @param executeId ID of execute which is notified unlocked
     */
    public static void notifyExecuteUnlocked(long executeId) {
        synchronized (bufferLock) {
            if (waitTillExecuteUnlockedBuffer.containsKey(executeId) && !waitTillExecuteUnlockedBuffer.get(executeId)
                    .isEmpty()) {
                try {
                    LOGGER.info("recovering the peek event from execute event buffer:[execute ID:" + executeId + "]"
                            + "(" + waitTillExecuteUnlockedBuffer.get(executeId).size() + " events queueing up)");
                    recoverTaskEventInfo(waitTillExecuteUnlockedBuffer.get(executeId));
                    bufferLastUpdateTime.put(executeId, System.currentTimeMillis());
                } catch (NaviException e) {
                    LOGGER.error("unexpected exception caught when recover event of execute:" + executeId);
                }
            }
        }
    }

    /**
     * recover events from buffer queue to global event queue
     *
     * @param eventQueue buffer queue events from
     * @throws NaviException
     */
    private static void recoverTaskEventInfo(PriorityBlockingQueue<TaskEventInfo> eventQueue) throws NaviException {
        TaskEventInfo eventInfo = eventQueue.poll();
        if (eventInfo != null) {
            LOGGER.info(String.format(
                    "recovering execute event:%d, execute ID:%d, rank:%f, event name:%s",
                    eventInfo.getId(),
                    eventInfo.getTaskEvent().getContext().getExecuteInfo().getId(),
                    eventInfo.getTaskEvent().getContext().getExecuteInfo().getRank(),
                    eventInfo.getTaskEvent().getEventName()));
            TaskStateManager.recoverTaskEventInfo(eventInfo);
        }
    }

    static class ClearBufferThread implements Runnable {

        private final int expireSeconds;

        ClearBufferThread(int expireSeconds) {
            this.expireSeconds = expireSeconds;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Iterator<Map.Entry<Long, Long>> iterator = bufferLastUpdateTime.entrySet().iterator();
                    while (iterator.hasNext()) {
                        //remove expired execute event buffer
                        Map.Entry<Long, Long> entry = iterator.next();
                        if (System.currentTimeMillis() / 1000 - entry.getValue() >= expireSeconds) {
                            long executeId = entry.getKey();
                            synchronized (bufferLock) {
                                if (waitTillExecuteUnlockedBuffer.get(executeId).isEmpty()) {
                                    LOGGER.info("remove expired event buffer for execute:" + executeId);
                                    waitTillExecuteUnlockedBuffer.remove(entry.getKey());
                                    iterator.remove();
                                }
                            }
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.info("clear execute event buffer thread interrupt.");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("clear execute event buffer error.", e);
                }
            }
        }
    }
}
