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

package com.tencent.bk.base.dataflow.jobnavi.state.event;

import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;

public class DefaultTaskEventBuilder {

    private static double defaultEventRank;

    public static double getDefaultEventRank() {
        return defaultEventRank;
    }

    public static void setDefaultEventRank(double defaultEventRank) {
        DefaultTaskEventBuilder.defaultEventRank = defaultEventRank;
    }

    /**
     * <p>build default task event</p>
     * <p>if preparing event , host set null.</p>
     *
     * @param eventName
     * @param executeId
     * @param host if preparing event , host set null.
     * @param taskInfo
     * @param rank
     * @return
     */
    public static DefaultTaskEvent buildEvent(String eventName, long executeId, String host, TaskInfo taskInfo,
            double rank) {
        DefaultTaskEvent event = new DefaultTaskEvent();
        event.setChangeStatus(TaskStatus.valueOf(eventName));
        event.setEventName(eventName);
        ExecuteInfo executeInfo = new ExecuteInfo();
        executeInfo.setId(executeId);
        executeInfo.setHost(host);
        executeInfo.setRank(rank);
        EventContext context = new EventContext();
        context.setExecuteInfo(executeInfo);
        context.setTaskInfo(taskInfo);
        event.setContext(context);
        return event;
    }

    /**
     * <p>build default task event</p>
     * <p>if preparing event , host set null.</p>
     *
     * @param eventName
     * @param executeId
     * @param host if preparing event , host set null.
     * @param taskInfo
     * @return
     */
    public static DefaultTaskEvent buildEvent(String eventName, long executeId, String host, TaskInfo taskInfo) {
        return buildEvent(eventName, executeId, host, taskInfo, defaultEventRank);
    }


    /**
     * build event base on another one
     *
     * @param origin
     * @param changeToEventName
     * @param changeStatus
     * @return
     */
    public static DefaultTaskEvent changeEvent(TaskEvent origin, String changeToEventName, TaskStatus changeStatus) {
        DefaultTaskEvent newEvent = new DefaultTaskEvent();
        newEvent.loadFromEvent(origin);
        newEvent.setChangeStatus(changeStatus);
        newEvent.setEventName(changeToEventName);
        return newEvent;
    }

    /**
     * build event base on another one
     *
     * @param origin
     * @param changeStatus
     * @return
     */
    public static DefaultTaskEvent changeEvent(TaskEvent origin, TaskStatus changeStatus) {
        DefaultTaskEvent newEvent = new DefaultTaskEvent();
        newEvent.loadFromEvent(origin);
        newEvent.setChangeStatus(changeStatus);
        newEvent.setEventName(changeStatus.toString());
        return newEvent;
    }
}
