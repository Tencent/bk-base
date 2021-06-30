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

package com.tencent.bk.base.dataflow.jobnavi.runner.task.thread;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.callback.DecommissionEventThreadCallback;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.thread.callback.AbstractTaskEventThreadCallback;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TaskEventThreadCallBackFactory {

    public static Map<String, Class<? extends AbstractTaskEventThreadCallback>> callbackMaps = new HashMap<>();

    static {
        register("decommission", DecommissionEventThreadCallback.class);
    }

    private static void register(String eventName, Class<? extends AbstractTaskEventThreadCallback> callback) {
        callbackMaps.put(eventName, callback);
    }

    /**
     * generate event process callback
     *
     * @param conf
     * @param event
     * @return event process callback
     * @throws NaviException
     */
    static AbstractTaskEventThreadCallback generateProcessCallBack(
            Configuration conf,
            TaskEvent event) throws NaviException {
        Class<? extends AbstractTaskEventThreadCallback> clz = callbackMaps.get(event.getEventName());
        if (clz != null) {
            try {
                Constructor<? extends AbstractTaskEventThreadCallback> c
                        = clz.getConstructor(Configuration.class, TaskEvent.class);
                return c.newInstance(conf, event);
            } catch (InstantiationException
                    | IllegalAccessException
                    | NoSuchMethodException
                    | InvocationTargetException e) {
                throw new NaviException(e);
            }
        }
        return null;
    }
}
