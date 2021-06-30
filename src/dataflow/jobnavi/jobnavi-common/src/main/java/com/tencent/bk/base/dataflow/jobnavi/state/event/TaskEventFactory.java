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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class TaskEventFactory {

    private static final Logger logger = Logger.getLogger(TaskEventFactory.class);

    private static Map<String, String> events = new ConcurrentHashMap<>();

    static {
        try {
            registerTaskEvent("preparing", DefaultTaskEvent.class.getName());
            registerTaskEvent("running", DefaultTaskEvent.class.getName());
            registerTaskEvent("failed", DefaultTaskEvent.class.getName());
            registerTaskEvent("kill", DefaultTaskEvent.class.getName());
            registerTaskEvent("disabled", DefaultTaskEvent.class.getName());
            registerTaskEvent("finished", DefaultTaskEvent.class.getName());
            registerTaskEvent("decommission", DefaultTaskEvent.class.getName());
            registerTaskEvent("failed_succeeded", DefaultTaskEvent.class.getName());
            registerTaskEvent("lost", DefaultTaskEvent.class.getName());
            registerTaskEvent("zombie", DefaultTaskEvent.class.getName());
            registerTaskEvent("recovering", DefaultTaskEvent.class.getName());
            registerTaskEvent("patching", DefaultTaskEvent.class.getName());
        } catch (NaviException e) {
            logger.error("register event error.", e);
        }
    }

    /**
     * register task event.
     * if event name is duplicate, throw NaviException
     *
     * @param eventName
     * @throws NaviException
     */
    private static void registerTaskEvent(String eventName, String eventClassName) throws NaviException {
        if (events.containsKey(eventName)) {
            throw new NaviException("eventName [" + eventName + "] is duplicate.");
        }
        events.put(eventName, eventClassName);
    }

    /**
     * generate task event from event name
     *
     * @param eventName
     * @return task event
     * @throws NaviException
     */
    public static TaskEvent generateTaskEvent(String eventName) throws NaviException {
        if (eventName == null) {
            throw new IllegalArgumentException("eventName cannot be null.");
        }
        String eventClassName = events.get(eventName);
        if (eventClassName == null) {
            CustomTaskEvent customTaskEvent = new CustomTaskEvent();
            customTaskEvent.setEventName(eventName);
            return customTaskEvent;
        }
        try {
            return (TaskEvent) Class.forName(eventClassName).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new NaviException(e);
        }
    }
}
